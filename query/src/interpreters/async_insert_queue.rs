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
use std::sync::Arc;

use common_base::base::tokio::sync::Notify;
use common_base::base::tokio::time::interval;
use common_base::base::tokio::time::Duration;
use common_base::base::tokio::time::Instant;
use common_base::base::ProgressValues;
use common_base::base::Runtime;
use common_base::infallible::Mutex;
use common_base::infallible::RwLock;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::InsertPlan;
use common_planners::SelectPlan;

use super::InsertInterpreter;
use super::SelectInterpreter;
use crate::pipelines::new::executor::PipelineCompleteExecutor;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::BlocksSource;
use crate::pipelines::new::SinkPipeBuilder;
use crate::pipelines::new::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;
use crate::sessions::Settings;
use crate::storages::memory::MemoryTableSink;

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
            self.plan.catalog_name, self.plan.database_name, self.plan.table_name
        );
        state.write(table.as_bytes());
        let values = self.changed_settings.get_setting_values();
        for value in values.into_iter() {
            let serialized = serde_json::to_string(&value).unwrap();
            state.write(serialized.as_bytes());
        }
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
    finished: Arc<RwLock<bool>>,
    notify: Arc<Notify>,
}

impl Entry {
    pub fn try_create(block: DataBlock) -> Self {
        Self {
            block,
            finished: Arc::new(RwLock::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn finish(&self) {
        let mut finished = self.finished.write();
        *finished = true;
        self.notify.notify_one();
    }

    pub fn finish_with_timeout(&self) {
        self.notify.notify_one();
    }

    pub async fn wait(&self) -> bool {
        if *self.finished.read() {
            return true;
        }
        self.notify.clone().notified().await;
        self.is_finished()
    }

    pub fn is_finished(&self) -> bool {
        return *self.finished.read();
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

#[derive(Clone)]
pub struct AsyncInsertQueue {
    pub session_mgr: Arc<RwLock<Option<Arc<SessionManager>>>>,
    runtime: Arc<Runtime>,
    max_data_size: u64,
    busy_timeout: Duration,
    stale_timeout: Duration,
    queue: Arc<RwLock<Queue>>,
    current_processing_insert: Arc<RwLock<QueryIdToEntry>>,
}

impl AsyncInsertQueue {
    pub fn try_create(
        // TODO(fkuner): maybe circular reference
        session_mgr: Arc<RwLock<Option<Arc<SessionManager>>>>,
        runtime: Arc<Runtime>,
        max_data_size: u64,
        busy_timeout: Duration,
        stale_timeout: Duration,
    ) -> Self {
        Self {
            session_mgr,
            runtime,
            max_data_size,
            busy_timeout,
            stale_timeout,
            queue: Arc::new(RwLock::new(Queue::default())),
            current_processing_insert: Arc::new(RwLock::new(QueryIdToEntry::default())),
        }
    }

    pub async fn start(self: Arc<Self>) {
        let self_arc = self.clone();
        // busy timeout
        let busy_timeout = self_arc.busy_timeout;
        self_arc.clone().runtime.as_ref().inner().spawn(async move {
            let mut intv = interval(busy_timeout);
            loop {
                intv.tick().await;
                let timeout = self_arc.clone().busy_check();
                intv = interval(timeout);
            }
        });
        // stale timeout
        let stale_timeout = self.stale_timeout;
        if !stale_timeout.is_zero() {
            let mut intv = interval(stale_timeout);
            loop {
                intv.tick().await;
                self.clone().stale_check();
            }
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
                })
                .unwrap();

                let mut pipeline = select_interpreter.create_new_pipeline().unwrap();

                let mut sink_pipeline_builder = SinkPipeBuilder::create();
                for _ in 0..pipeline.output_len() {
                    let input_port = InputPort::create();
                    sink_pipeline_builder.add_sink(
                        input_port.clone(),
                        MemoryTableSink::create(input_port, ctx.clone()),
                    );
                }
                pipeline.add_pipe(sink_pipeline_builder.finalize());
                let executor =
                    PipelineCompleteExecutor::try_create(self.runtime.clone(), pipeline).unwrap();
                executor.execute().unwrap();
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
        let key = InsertKey::try_create(plan, settings.clone());

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

    pub fn get_entry(&self, query_id: String) -> EntryPtr {
        let current_processing_insert = self.current_processing_insert.read();
        return current_processing_insert.get(&query_id).unwrap().clone();
    }

    pub async fn wait_for_processing_insert(
        self: Arc<Self>,
        query_id: String,
        time_out: Duration,
    ) -> Result<()> {
        let entry = self.get_entry(query_id);
        let e = entry.clone();
        self.runtime.as_ref().inner().spawn(async move {
            let mut intv = interval(time_out);
            intv.tick().await;
            intv.tick().await;
            e.finish_with_timeout();
        });
        let finished = entry.wait().await;
        match finished {
            true => Ok(()),
            false => Err(ErrorCode::AsyncInsertTimeoutError("Async insert timeout.")),
        }
    }

    fn schedule(self: Arc<Self>, key: InsertKey, data: InsertData) {
        self.runtime.as_ref().inner().spawn(async {
            self.process(key, data).await;
        });
    }

    async fn process(self: Arc<Self>, key: InsertKey, data: InsertData) {
        let insert_plan = key.plan;

        let session_mgr = self.session_mgr.read().clone().unwrap();
        let session = session_mgr.create_session(SessionType::HTTPQuery).await;
        let ctx = session.unwrap().create_query_context().await.unwrap();

        ctx.apply_changed_settings(key.changed_settings.clone())
            .unwrap();

        let interpreter =
            InsertInterpreter::try_create(ctx.clone(), insert_plan.as_ref().clone(), true).unwrap();

        let output_port = OutputPort::create();
        let blocks = Arc::new(Mutex::new(VecDeque::from_iter(
            data.entries.iter().map(|x| x.block.clone()),
        )));
        let source = BlocksSource::create(ctx, output_port.clone(), blocks);

        let mut builder = SourcePipeBuilder::create();

        builder.add_source(output_port.clone(), source.unwrap());

        interpreter
            .as_ref()
            .set_source_pipe_builder(Some(builder))
            .unwrap();
        interpreter.execute(None).await.unwrap();

        for entry in data.entries.into_iter() {
            entry.finish();
        }
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

    fn stale_check(self: Arc<Self>) {
        let mut keys = Vec::new();
        let mut queue = self.queue.write();

        for (key, value) in queue.iter() {
            if value.data_size == 0 {
                continue;
            }
            let time_lag = Instant::now() - value.last_update;
            if time_lag.cmp(&self.clone().stale_timeout) == Ordering::Greater {
                self.clone().schedule(key.clone(), value.clone());
                keys.push(key.clone());
            }
        }

        for key in keys.iter() {
            queue.remove(key);
        }
    }
}
