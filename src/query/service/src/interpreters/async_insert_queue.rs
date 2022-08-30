// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::hash::Hash;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common_base::base::tokio::sync::Notify;
use common_base::base::tokio::time::interval_at;
use common_base::base::tokio::time::Duration;
use common_base::base::tokio::time::Instant;
use common_base::base::GlobalIORuntime;
use common_base::base::ProgressValues;
use common_base::base::Runtime;
use common_base::base::Singleton;
use common_base::base::TrySpawn;
use common_config::Config;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::InsertPlan;
use common_planners::SelectPlan;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use parking_lot::RwLock;

use super::InsertInterpreter;
use super::SelectInterpreter;
use crate::interpreters::Interpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::BlocksSource;
use crate::pipelines::processors::ContextSink;
use crate::pipelines::SinkPipeBuilder;
use crate::pipelines::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;
use crate::sessions::Settings;
use crate::sessions::TableContext;

#[derive(Clone)]
pub struct InsertKey {
    plan: Arc<InsertPlan>,
    // settings different with default settings
    changed_settings: Arc<Settings>,
}

impl InsertKey {
    pub fn get_serialized_changed_settings(&self) -> String {
        let mut serialized_settings = String::new();
        let values = self.changed_settings.get_setting_values();
        for value in values.into_iter() {
            let serialized = serde_json::to_string(&value).unwrap();
            serialized_settings.push_str(&serialized);
        }
        serialized_settings
    }
}

impl PartialEq for InsertKey {
    fn eq(&self, other: &Self) -> bool {
        self.plan.eq(&other.plan)
            && self
                .get_serialized_changed_settings()
                .eq(&other.get_serialized_changed_settings())
    }
}

impl Eq for InsertKey {}

impl Hash for InsertKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let table = format!(
            "{}.{}.{}",
            self.plan.catalog, self.plan.database, self.plan.table
        );
        state.write(table.as_bytes());
        let values = self.changed_settings.get_setting_values();
        let serialized = serde_json::to_string(&values).unwrap();
        state.write(serialized.as_bytes());
    }
}

impl InsertKey {
    pub fn try_create(plan: Arc<InsertPlan>, changed_settings: Arc<Settings>) -> Self {
        Self {
            plan,
            changed_settings,
        }
    }
}

#[derive(Clone)]
pub struct Entry {
    block: DataBlock,
    notify: Arc<Notify>,
    finished: Arc<RwLock<bool>>,
    timeout: Arc<RwLock<bool>>,
    error: Arc<RwLock<ErrorCode>>,
}

impl Entry {
    pub fn try_create(block: DataBlock) -> Self {
        Self {
            block,
            notify: Arc::new(Notify::new()),
            finished: Arc::new(RwLock::new(false)),
            timeout: Arc::new(RwLock::new(false)),
            error: Arc::new(RwLock::new(ErrorCode::Ok(""))),
        }
    }

    pub fn finish(&self) {
        let mut finished = self.finished.write();
        *finished = true;
        self.notify.notify_one();
    }

    pub fn finish_with_err(&self, err: ErrorCode) {
        let mut error = self.error.write();
        *error = err;
        self.notify.notify_one();
    }

    pub fn finish_with_timeout(&self) {
        let mut timeout = self.timeout.write();
        *timeout = true;
        self.notify.notify_one();
    }

    pub async fn wait(&self) -> Result<()> {
        if *self.finished.read() {
            return Ok(());
        }
        self.notify.clone().notified().await;
        match self.is_finished() {
            true => Ok(()),
            false => match self.is_timeout() {
                true => Err(ErrorCode::AsyncInsertTimeoutError("Async insert timeout")),
                false => Err((*self.error.read()).clone()),
            },
        }
    }

    pub fn is_finished(&self) -> bool {
        return *self.finished.read();
    }

    pub fn is_timeout(&self) -> bool {
        return *self.timeout.read();
    }
}

#[derive(Clone)]
pub struct InsertData {
    entries: Vec<EntryPtr>,
    data_size: u64,
    first_update: Instant,
    last_update: Instant,
}

impl InsertData {
    pub fn try_create(
        entries: Vec<EntryPtr>,
        data_size: u64,
        first_update: Instant,
        last_update: Instant,
    ) -> Self {
        Self {
            entries,
            data_size,
            first_update,
            last_update,
        }
    }
}

type EntryPtr = Arc<Entry>;
type Queue = HashMap<InsertKey, InsertData>;
type QueryIdToEntry = HashMap<String, EntryPtr>;

pub struct AsyncInsertManager {
    async_runtime: Runtime,
    max_data_size: u64,
    busy_timeout: Duration,
    stale_timeout: Duration,
    queue: Arc<RwLock<Queue>>,
    current_processing_insert: Arc<RwLock<QueryIdToEntry>>,
    finished: AtomicBool,
}

static ASYNC_INSERT_MANAGER: OnceCell<Singleton<Arc<AsyncInsertManager>>> = OnceCell::new();

impl AsyncInsertManager {
    pub fn init(config: &Config, v: Singleton<Arc<AsyncInsertManager>>) -> Result<()> {
        let max_data_size = config.query.async_insert_max_data_size;
        let busy_timeout = Duration::from_millis(config.query.async_insert_busy_timeout);
        let stale_timeout = Duration::from_millis(config.query.async_insert_stale_timeout);
        let async_runtime = Runtime::with_worker_threads(2, Some(String::from("Async-Insert")))?;

        v.init(Arc::new(AsyncInsertManager {
            busy_timeout,
            stale_timeout,
            max_data_size,
            async_runtime,
            finished: AtomicBool::new(false),
            queue: Arc::new(RwLock::new(Queue::default())),
            current_processing_insert: Arc::new(RwLock::new(QueryIdToEntry::default())),
        }))?;

        ASYNC_INSERT_MANAGER.set(v).ok();
        Ok(())
    }

    pub fn instance() -> Arc<AsyncInsertManager> {
        match ASYNC_INSERT_MANAGER.get() {
            None => panic!("AsyncInsertManager is not init"),
            Some(async_insert_manager) => async_insert_manager.get(),
        }
    }

    pub fn shutdown(self: &Arc<Self>) {
        self.finished
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub async fn start(self: &Arc<Self>) {
        // TODO: need refactor this code.
        let this = self.clone();
        // busy timeout
        let busy_timeout = this.busy_timeout;
        self.async_runtime.spawn(async move {
            let mut intv = interval_at(Instant::now() + busy_timeout, busy_timeout);
            while !this.finished.load(std::sync::atomic::Ordering::Relaxed) {
                intv.tick().await;
                if this.queue.read().is_empty() {
                    continue;
                }
                let timeout = this.clone().busy_check();
                if timeout != busy_timeout {
                    intv = interval_at(Instant::now() + timeout, timeout);
                }
            }
        });
        // stale timeout
        let stale_timeout = self.stale_timeout;
        if !stale_timeout.is_zero() {
            let this = self.clone();
            self.async_runtime.spawn(async move {
                let mut intv = interval_at(Instant::now() + stale_timeout, stale_timeout);
                while !this.finished.load(std::sync::atomic::Ordering::Relaxed) {
                    intv.tick().await;
                    if this.queue.read().is_empty() {
                        continue;
                    }
                    let timeout = this.stale_check();
                    if timeout != busy_timeout {
                        intv = interval_at(Instant::now() + timeout, timeout);
                    }
                }
            });
        }
    }

    pub async fn push(
        self: Arc<Self>,
        plan_node: Arc<InsertPlan>,
        ctx: Arc<QueryContext>,
    ) -> Result<()> {
        let self_arc = self.clone();
        let plan = plan_node.clone();
        let settings = ctx.get_changed_settings();

        let data_block = match &plan_node.source {
            common_planners::InsertInputSource::SelectPlan(plan) => {
                let select_interpreter = SelectInterpreter::try_create(ctx.clone(), SelectPlan {
                    input: Arc::new((**plan).clone()),
                })?;

                let mut build_res = select_interpreter.create_new_pipeline().await?;

                let mut sink_pipeline_builder = SinkPipeBuilder::create();
                for _ in 0..build_res.main_pipeline.output_len() {
                    let input_port = InputPort::create();
                    sink_pipeline_builder.add_sink(
                        input_port.clone(),
                        ContextSink::create(input_port, ctx.clone()),
                    );
                }
                build_res
                    .main_pipeline
                    .add_pipe(sink_pipeline_builder.finalize());
                let mut pipelines = build_res.sources_pipelines;
                pipelines.push(build_res.main_pipeline);
                let executor_settings = ExecutorSettings::try_create(&settings)?;
                let executor = PipelineCompleteExecutor::from_pipelines(
                    GlobalIORuntime::instance(),
                    ctx.query_need_abort(),
                    pipelines,
                    executor_settings,
                )
                .unwrap();
                executor.execute()?;
                drop(executor);
                let blocks = ctx.consume_precommit_blocks();
                DataBlock::concat_blocks(&blocks)?
            }
            common_planners::InsertInputSource::StreamingWithFormat(_) => todo!(),
            common_planners::InsertInputSource::Values(values) => {
                let data_block = values.block.clone();

                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                ctx.get_scan_progress().incr(&progress_values);

                data_block
            }
        };

        let entry = Arc::new(Entry::try_create(data_block.clone()));
        let key = InsertKey::try_create(plan, settings);

        let mut queue = self_arc.queue.write();

        match queue.get_mut(&key) {
            Some(value) => {
                value.entries.push(entry.clone());
                value.data_size += data_block.memory_size() as u64;
                value.last_update = Instant::now();
                if value.data_size > self_arc.max_data_size {
                    self_arc.clone().schedule(key.clone(), value.clone());
                    queue.remove(&key);
                }
            }
            None => {
                let entries = vec![entry.clone()];
                let data_size = data_block.memory_size();
                let first_update = Instant::now();
                let last_update = Instant::now();
                let value =
                    InsertData::try_create(entries, data_size as u64, first_update, last_update);
                queue.insert(key.clone(), value.clone());
                if data_size > self_arc.max_data_size as usize {
                    self_arc.clone().schedule(key.clone(), value);
                    queue.remove(&key);
                }
            }
        }
        let mut current_processing_insert = self.current_processing_insert.write();
        current_processing_insert.insert(ctx.get_id(), entry);
        Ok(())
    }

    pub fn get_entry(&self, query_id: &str) -> Result<EntryPtr> {
        let current_processing_insert = self.current_processing_insert.read();
        Ok(current_processing_insert.get(query_id).unwrap().clone())
    }

    pub fn delete_entry(&self, query_id: &str) -> Result<()> {
        let mut current_processing_insert = self.current_processing_insert.write();
        current_processing_insert.remove(query_id);
        Ok(())
    }

    pub async fn wait_for_processing_insert(
        self: Arc<Self>,
        query_id: String,
        time_out: Duration,
    ) -> Result<()> {
        let entry = self.get_entry(&query_id)?;
        let e = entry.clone();
        self.async_runtime.spawn(async move {
            let mut intv = interval_at(Instant::now() + time_out, time_out);
            intv.tick().await;
            e.finish_with_timeout();
        });
        match entry.wait().await {
            Ok(_) => {
                self.delete_entry(&query_id)?;
                Ok(())
            }
            Err(err) => {
                self.delete_entry(&query_id)?;
                Err(err)
            }
        }
    }

    fn schedule(self: &Arc<Self>, key: InsertKey, data: InsertData) {
        let this = self.clone();
        self.async_runtime.spawn(async {
            match this.process(key, data.clone()).await {
                Ok(_) => {
                    for entry in data.entries.into_iter() {
                        entry.finish();
                    }
                }
                Err(err) => {
                    for entry in data.entries.into_iter() {
                        entry.finish_with_err(err.clone());
                    }
                }
            }
        });
    }

    async fn process(self: Arc<Self>, key: InsertKey, data: InsertData) -> Result<()> {
        let insert_plan = key.plan;

        let session_manager = SessionManager::instance();
        let session = session_manager
            .create_session(SessionType::HTTPQuery)
            .await?;
        let ctx = session.create_query_context().await?;
        ctx.apply_changed_settings(key.changed_settings.clone())?;

        let interpreter =
            InsertInterpreter::try_create(ctx.clone(), insert_plan.as_ref().clone(), true)?;

        let output_port = OutputPort::create();
        let blocks = Arc::new(Mutex::new(VecDeque::from_iter(
            data.entries.iter().map(|x| x.block.clone()),
        )));
        let source = BlocksSource::create(ctx, output_port.clone(), blocks)?;
        let mut builder = SourcePipeBuilder::create();
        builder.add_source(output_port.clone(), source);

        interpreter.set_source_pipe_builder(Some(builder))?;
        interpreter.execute().await?;
        Ok(())
    }

    fn busy_check(self: Arc<Self>) -> Duration {
        let mut keys = Vec::new();
        let mut queue = self.queue.write();

        let mut timeout = self.busy_timeout;

        for (key, value) in queue.iter() {
            if value.data_size == 0 {
                continue;
            }
            let time_lag = Instant::now() - value.first_update;
            if time_lag.cmp(&self.clone().busy_timeout) == Ordering::Greater {
                self.clone().schedule(key.clone(), value.clone());
                keys.push(key.clone());
            } else {
                timeout = timeout.min(self.busy_timeout - time_lag);
            }
        }

        for key in keys.iter() {
            queue.remove(key);
        }

        timeout
    }

    fn stale_check(self: &Arc<Self>) -> Duration {
        let mut keys = Vec::new();
        let mut queue = self.queue.write();

        let mut timeout = self.busy_timeout;

        for (key, value) in queue.iter() {
            if value.data_size == 0 {
                continue;
            }
            let time_lag = Instant::now() - value.last_update;
            if time_lag.cmp(&self.clone().stale_timeout) == Ordering::Greater {
                self.clone().schedule(key.clone(), value.clone());
                keys.push(key.clone());
            } else {
                timeout = timeout.min(self.stale_timeout - time_lag);
            }
        }

        for key in keys.iter() {
            queue.remove(key);
        }

        timeout
    }
}
