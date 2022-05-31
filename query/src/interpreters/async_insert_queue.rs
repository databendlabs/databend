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
use std::sync::Arc;
use std::hash::Hash;
use common_base::infallible::Mutex;

use common_base::base::tokio::time::Duration;
use common_base::base::tokio::time::Instant;
use common_base::base::tokio::time::interval;

use common_base::base::tokio::sync::Notify;

use common_base::base::Runtime;
use common_base::infallible::RwLock;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::InsertInputSource;
use common_planners::InsertPlan;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use futures::Future;

use crate::pipelines::new::SinkPipeBuilder;
use crate::pipelines::new::SourcePipeBuilder;
use crate::pipelines::new::executor::PipelineCompleteExecutor;
use crate::pipelines::new::processors::BlocksSource;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;
use crate::sessions::QueryContext;
use crate::sessions::Settings;
use crate::storages::memory::MemoryTable;
use crate::storages::memory::MemoryTableSink;

use super::InsertInterpreter;
use super::InterpreterFactory;
use super::SelectInterpreter;
use super::interpreter;

#[derive(Clone)]
pub struct InsertKey {
    plan: Arc<InsertPlan>,
    settings: Settings,
}

impl PartialEq for InsertKey {
    fn eq(&self, other: &Self) -> bool {
        self.plan.eq(&other.plan)
    }
}

impl Eq for InsertKey {}

impl Hash for InsertKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let table = format!("{}.{}.{}", self.plan.catalog_name, self.plan.database_name, self.plan.table_name);
        state.write(table.as_bytes());
        // self.settings.hash(state);
    }
}

impl InsertKey {
    pub fn try_create(plan: Arc<InsertPlan>, settings: Settings) -> Self {
        Self { plan, settings, }
    }
}

#[derive(Clone)]
pub struct Entry {
    block: DataBlock,
    query_id: String,
    finished: Arc<RwLock<bool>>,
    notify: Arc<Notify>,
}

impl Entry {
    pub fn try_create(block: DataBlock, query_id: String) -> Self {
        Self {
            block,
            query_id,
            finished: Arc::new(RwLock::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn finish(&self) {
        let mut finished = self.finished.write();
        *finished = true;
        self.notify.notify_one();
        // self.notify.
    }

    pub async fn wait(&self) -> bool {
        // if *self.finished.read() {
        //     return true;
        // }
        println!("wait1");
        self.notify.clone().notified().await;
        println!("wait2");
        return true;
    }

    pub fn is_finished(&self) -> bool {
        return *self.finished.read();
    }
}

#[derive(Clone)]
pub struct InsertData {
    entries: Vec<EntryPtr>,
    data_size: u64,
    // first_update: Instant,
    last_update: Instant,
    session_id: String,
    ctx: Arc<QueryContext>,
}

impl InsertData {
    pub fn try_create(entries: Vec<EntryPtr>, data_size: u64, last_update: Instant, session_id: String, ctx: Arc<QueryContext>) -> Self {
        Self { entries, data_size, last_update, session_id, ctx }
    }
}

type EntryPtr = Arc<Entry>;
type InsertDataPtr = Arc<InsertData>;
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
        session_mgr: Arc<RwLock<Option<Arc<SessionManager>>>>,
        runtime: Arc<Runtime>, 
        max_data_size: u64,
        busy_timeout: Duration,
        stale_timeout: Duration) -> Self 
    {
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
        let busy_timeout = self_arc.busy_timeout.clone();
        // let stale_timeout = self_arc.stale_timeout.clone();
        self_arc.clone().runtime.as_ref().inner().spawn(async move {
            let mut intv = interval(busy_timeout);
            loop {  
                intv.tick().await;
                self_arc.clone().busy_check();
            }
        });
    }

    pub fn push(self: Arc<Self>, plan_node: Arc<InsertPlan>, ctx: Arc<QueryContext>) {
        println!("push");
        let self_arc = self.clone();
        let plan = plan_node.clone();
        let settings = ctx.get_settings();
        
        let data_block = match &plan_node.source {
            common_planners::InsertInputSource::SelectPlan(plan) => {
                let select_interpreter = SelectInterpreter::try_create(ctx.clone(), SelectPlan {
                    input: Arc::new((**plan).clone()),
                }).unwrap();

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
                let executor = PipelineCompleteExecutor::try_create(self.runtime.clone(), pipeline).unwrap();
                executor.execute().unwrap();
                drop(executor);
                let values = ctx.clone().consume_precommit_blocks();
                values[0].clone()
            },
            common_planners::InsertInputSource::StreamingWithFormat(_) => todo!(),
            common_planners::InsertInputSource::Values(values) => {
                values.block.clone()
            }
        };

        let entry = Arc::new(Entry::try_create(data_block.clone(), ctx.get_id()));
        let key = InsertKey::try_create(plan, settings.as_ref().clone());
        let tmp = self_arc.clone();
        let mut queue = tmp.queue.write();

        match queue.get_mut(&key) {
            Some(value) => {
                // let mut entries = value.entries.write();
                // (*entries).push(entry.clone());
                value.entries.push(entry.clone());
                value.data_size += data_block.memory_size() as u64;
                // value.last_update = Instant::now();
                println!("1data_size:{}", value.data_size.clone());
                if value.data_size > self_arc.max_data_size {
                    self_arc.schedule(key.clone(), value.clone());
                    queue.remove(&key);
                }
            }
            None => {
                let entries = vec![entry.clone()];
                let data_size = data_block.memory_size();
                println!("2data_size:{}", data_size);
                let last_update = Instant::now();
                let value = InsertData::try_create(entries, data_size as u64, last_update, ctx.get_connection_id(), ctx.clone());
                queue.insert(key.clone(), value.clone());
                if data_size > self_arc.max_data_size as usize {
                    self_arc.schedule(key.clone(), value);
                    queue.remove(&key);
                }
            }
        }

        let mut current_processing_insert = self.current_processing_insert.write();

        println!("dead lock");
        current_processing_insert.insert(ctx.clone().get_id(), entry.clone());
        println!("push success");
    }

    pub fn get_entry(&self, query_id: String) -> EntryPtr {
        let current_processing_insert = self.current_processing_insert.read();
        let tmp = current_processing_insert.get(&query_id).unwrap();
        let a = tmp.clone();
        return a;
    }
    
    pub fn wait_for_processing_insert(&self, query_id: String, time_out: Duration) -> Result<()>  {
        println!("wait_for_processing_insert");
        // let self_arc = self.clone();

        // let entry = self.current_processing_insert.read().get(&query_id).unwrap();

        // {
        //     let current_processing_insert = self.current_processing_insert.read();
        //     entry = match current_processing_insert.get(&query_id) {
        //         Some(entry) => entry,
        //         None => {
        //             todo!()
        //         }
        //     };
        // }

        let entry = self.get_entry(query_id);


        let finished = futures::executor::block_on(entry.wait());
        
        println!("wait success");
        if !finished {
            return Err(ErrorCode::AbortedQuery("display_text"));
        }
        return Ok(());
    }

    fn schedule(self: Arc<Self>, key: InsertKey, data: InsertData) {
        let self_arc = self.clone();
        self_arc.runtime.as_ref().inner().spawn(async {
            self_arc.process(key, data).await;
        });
    }

    async fn process(self: Arc<Self>, key: InsertKey, data: InsertData) {
        println!("process1");
        let self_arc = self.clone();
        let mut insert_plan = key.plan;

        let session_mgr = self_arc.session_mgr.read().clone().unwrap();

        // let session = session_mgr.get_session_by_id(&data.session_id).await;

        let session = session_mgr.create_session(SessionType::HTTPQuery).await;

        println!("process2-1");

        // let session = session_mgr.create_session(SessionType::HTTPQuery).await;
        let ctx = session.unwrap().create_query_context().await.unwrap();

        // session.unwrap()

        println!("process2");

        // let ctx = data.ctx.clone();

        // let queue = self.queue.write();
        
        let interpreter = InsertInterpreter::try_create(ctx.clone(), insert_plan.as_ref().clone(), true).unwrap();

        // data.entries.write();

        // let mut entries = *data.entries.write().clone();
        
        let output_port = OutputPort::create();
        let blocks = Arc::new(Mutex::new(VecDeque::from_iter(data.entries.iter().map(|x|x.block.clone()))));
        let source = BlocksSource::create(ctx, output_port.clone(), blocks);
        
        let mut builder = SourcePipeBuilder::create();
        
        builder.add_source(
            output_port.clone(),
            source.unwrap(),
        );

        println!("process3");

        interpreter.as_ref().set_source_pipe_builder(Some(builder));
        println!("process4-2");
        interpreter.execute(None).await;
        println!("process4--1");
        for entry in data.entries.into_iter() {
            println!("process4-0");
            entry.finish();
            println!("process4");
        }

        println!("process5");
    }

    fn busy_check(self: Arc<Self>) {
        let self_arc = self.clone();
        let a = self_arc.clone();

        let mut keys = Vec::new();

        let mut queue = a.queue.write();

        for (key, value) in queue.iter() {
            if value.data_size == 0 {
                continue;
            }
            let time_lag = Instant::now() - value.last_update;
            if time_lag.cmp(&self_arc.clone().busy_timeout) == Ordering::Greater {
                self_arc.clone().schedule(key.clone(), value.clone());
                keys.push(key.clone());
            }
        }
                
        for key in keys.iter() {
            println!("remove key");
            queue.remove(key);
        }
    }

    fn stale_check(self: Arc<Self>) {
        let self_arc = self.clone();
        let a = self_arc.clone();

        let mut keys = Vec::new();

        let mut queue = a.queue.write();

        for (key, value) in queue.iter() {
            if value.data_size == 0 {
                continue;
            }
            let time_lag = Instant::now() - value.last_update;
            if time_lag.cmp(&self_arc.clone().stale_timeout) == Ordering::Greater {
                self_arc.clone().schedule(key.clone(), value.clone());
                keys.push(key.clone());
            }
        }
                
        for key in keys.iter() {
            println!("remove key");
            queue.remove(key);
        }
    }
}