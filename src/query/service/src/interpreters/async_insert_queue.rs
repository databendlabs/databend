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

use std::collections::HashMap;
use std::collections::VecDeque;
use std::hash::Hash;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use common_base::base::tokio::sync::Notify;
use common_base::base::tokio::time::interval_at;
use common_base::base::tokio::time::Duration;
use common_base::base::tokio::time::Instant;
use common_base::base::ProgressValues;
use common_base::base::Runtime;
use common_base::base::Singleton;
use common_base::base::TrySpawn;
use common_config::Config;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::InsertInputSource;
use common_planners::InsertPlan;
use common_planners::SelectPlan;
use futures_util::future::Either;
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
    pub fn create(entry: EntryPtr) -> Self {
        InsertData {
            data_size: entry.block.memory_size() as u64,
            entries: vec![entry],
            first_update: Instant::now(),
            last_update: Instant::now(),
        }
    }

    pub fn push_entry(&mut self, entry: EntryPtr) -> &mut Self {
        self.entries.push(entry.clone());
        self.data_size += entry.block.memory_size() as u64;
        self.last_update = Instant::now();
        self
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
    finished: Arc<AtomicBool>,
    finished_notify: Arc<Notify>,
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
            finished: Arc::new(AtomicBool::new(false)),
            finished_notify: Arc::new(Notify::new()),
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
        self.finished_notify.notify_waiters();
    }

    pub async fn start(&self) {
        self.start_busy_listen();
        self.start_stale_listen();
    }

    fn start_stale_listen(&self) {
        // stale timeout
        let finished = self.finished.clone();
        let stale_timeout = self.stale_timeout;
        let queue = self.queue.clone();
        let finished_notify = self.finished_notify.clone();

        if !stale_timeout.is_zero() {
            self.async_runtime.spawn(async move {
                let mut notified = Box::pin(finished_notify.notified());
                let mut intv = interval_at(Instant::now() + stale_timeout, stale_timeout);
                while !finished.load(std::sync::atomic::Ordering::Relaxed) {
                    let interval_tick = Box::pin(intv.tick());
                    match futures::future::select(notified, interval_tick).await {
                        Either::Left(_) => {
                            if !queue.read().is_empty() {
                                AsyncInsertManager::stale_check(&queue, stale_timeout);
                            }

                            break;
                        }
                        Either::Right((_, left)) => {
                            notified = left;
                        }
                    };

                    if !queue.read().is_empty() {
                        let timeout = AsyncInsertManager::stale_check(&queue, stale_timeout);
                        if timeout != stale_timeout {
                            intv = interval_at(Instant::now() + timeout, timeout);
                        }
                    }
                }
            });
        }
    }

    fn start_busy_listen(&self) {
        // busy timeout
        let busy_timeout = self.busy_timeout;
        let queue = self.queue.clone();
        let finished = self.finished.clone();
        let finished_notify = self.finished_notify.clone();

        if !busy_timeout.is_zero() {
            self.async_runtime.spawn(async move {
                let mut notified = Box::pin(finished_notify.notified());
                let mut intv = interval_at(Instant::now() + busy_timeout, busy_timeout);
                while !finished.load(std::sync::atomic::Ordering::Relaxed) {
                    match futures::future::select(notified, Box::pin(intv.tick())).await {
                        Either::Left(_) => {
                            if !queue.read().is_empty() {
                                AsyncInsertManager::busy_check(&queue, busy_timeout);
                            }

                            break;
                        }
                        Either::Right((_, left)) => {
                            notified = left;
                        }
                    };

                    if !queue.read().is_empty() {
                        let timeout = AsyncInsertManager::busy_check(&queue, busy_timeout);
                        if timeout != busy_timeout {
                            intv = interval_at(Instant::now() + timeout, timeout);
                        }
                    }
                }
            });
        }
    }

    // Only used in test?
    pub async fn push(&self, ctx: Arc<QueryContext>, plan_node: Arc<InsertPlan>) -> Result<()> {
        let plan = plan_node.clone();
        let settings = ctx.get_changed_settings();

        let query_id = ctx.get_id();
        let max_data_size = self.max_data_size;
        let data_block = AsyncInsertManager::get_source(&ctx, &plan_node, &settings).await?;

        let key = InsertKey::try_create(plan, settings);
        let entry = Arc::new(Entry::try_create(data_block.clone()));

        let queue = self.queue.clone();
        let processing_insert = self.current_processing_insert.clone();

        let push_future = self.async_runtime.spawn(async move {
            processing_insert.write().insert(query_id, entry.clone());

            let mut queue = queue.write();

            match queue.entry(key) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    let data = InsertData::create(entry);

                    if data.data_size > max_data_size {
                        AsyncInsertManager::schedule(v.into_key(), data);
                        return;
                    }

                    v.insert(data);
                }
                std::collections::hash_map::Entry::Occupied(mut v) => {
                    if v.get_mut().push_entry(entry).data_size > max_data_size {
                        let (key, data) = v.remove_entry();
                        AsyncInsertManager::schedule(key, data);
                    }
                }
            };
        });

        match push_future.await {
            Ok(_) => Ok(()),
            Err(panic_message) => match panic_message.is_cancelled() {
                true => Err(ErrorCode::TokioError("Tokio runtime cancelled task.")),
                false => {
                    let cause = panic_message.into_panic();
                    match cause.downcast_ref::<&'static str>() {
                        None => match cause.downcast_ref::<String>() {
                            None => Err(ErrorCode::PanicError("Sorry, unknown panic message")),
                            Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                        },
                        Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                    }
                }
            },
        }
    }

    async fn get_source(
        ctx: &Arc<QueryContext>,
        node: &InsertPlan,
        settings: &Settings,
    ) -> Result<DataBlock> {
        match &node.source {
            InsertInputSource::SelectPlan(plan) => {
                let select_interpreter = SelectInterpreter::try_create(ctx.clone(), SelectPlan {
                    input: Arc::new((**plan).clone()),
                })?;

                let mut build_res = select_interpreter.execute2().await?;

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
                let executor_settings = ExecutorSettings::try_create(settings)?;
                let executor = PipelineCompleteExecutor::from_pipelines(
                    ctx.query_need_abort(),
                    pipelines,
                    executor_settings,
                )?;

                executor.execute()?;
                drop(executor);
                let blocks = ctx.consume_precommit_blocks();
                DataBlock::concat_blocks(&blocks)
            }
            InsertInputSource::StreamingWithFormat(_) => Err(ErrorCode::UnImplement(
                "Async insert streaming with format is unimplemented.",
            )),
            InsertInputSource::Values(values) => {
                let data_block = values.block.clone();

                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                ctx.get_scan_progress().incr(&progress_values);

                Ok(data_block)
            }
        }
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

    fn schedule(key: InsertKey, data: InsertData) {
        // We can't block the scheduled task, create new task to be executed by current runtime.
        common_base::base::tokio::spawn(async {
            match Self::process(key, data.clone()).await {
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

    async fn process(key: InsertKey, data: InsertData) -> Result<()> {
        let insert_plan = key.plan;

        let session = SessionManager::instance()
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
        let source = BlocksSource::create(ctx.clone(), output_port.clone(), blocks)?;
        let mut builder = SourcePipeBuilder::create();
        builder.add_source(output_port.clone(), source);

        interpreter.set_source_pipe_builder(Some(builder))?;
        interpreter.execute(ctx).await?;
        Ok(())
    }

    fn busy_check(queue: &RwLock<Queue>, busy_timeout: Duration) -> Duration {
        let mut busy_keys = Vec::new();
        let mut new_timeout = busy_timeout;

        {
            let readable_queue = queue.read();

            for (key, data) in readable_queue.iter() {
                if data.data_size != 0 {
                    let time_lag = Instant::now() - data.first_update;

                    if time_lag > busy_timeout {
                        busy_keys.push(key.clone());
                    } else {
                        // Find the minimum time for the next round
                        new_timeout = std::cmp::min(new_timeout, busy_timeout - time_lag);
                    }
                }
            }
        }

        let mut writeable_queue = queue.write();

        for busy_key in busy_keys.into_iter() {
            if let Some(data) = writeable_queue.remove(&busy_key) {
                AsyncInsertManager::schedule(busy_key, data);
            }
        }

        new_timeout
    }

    fn stale_check(queue: &RwLock<Queue>, stale_timeout: Duration) -> Duration {
        let mut stale_keys = Vec::new();
        let mut new_timeout = stale_timeout;

        {
            let readable_queue = queue.read();

            for (key, data) in readable_queue.iter() {
                if data.data_size != 0 {
                    let time_lag = Instant::now() - data.last_update;

                    if time_lag > stale_timeout {
                        stale_keys.push(key.clone());
                    } else {
                        // Find the minimum time for the next round
                        new_timeout = std::cmp::min(new_timeout, stale_timeout - time_lag);
                    }
                }
            }
        }

        let mut writeable_queue = queue.write();

        for stale_key in stale_keys.into_iter() {
            if let Some(data) = writeable_queue.remove(&stale_key) {
                AsyncInsertManager::schedule(stale_key, data);
            }
        }

        new_timeout
    }
}
