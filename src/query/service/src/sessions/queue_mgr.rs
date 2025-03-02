// Copyright 2021 Datafuse Labs
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
use std::fmt::Display;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use databend_common_ast::ast::ExplainKind;
use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserInfo;
use databend_common_metrics::session::dec_session_running_acquired_queries;
use databend_common_metrics::session::inc_session_running_acquired_queries;
use databend_common_metrics::session::incr_session_queue_abort_count;
use databend_common_metrics::session::incr_session_queue_acquire_error_count;
use databend_common_metrics::session::incr_session_queue_acquire_timeout_count;
use databend_common_metrics::session::record_session_queue_acquire_duration_ms;
use databend_common_metrics::session::set_session_queued_queries;
use databend_common_sql::plans::ModifyColumnAction;
use databend_common_sql::plans::ModifyTableColumnPlan;
use databend_common_sql::plans::Plan;
use databend_common_sql::PlanExtras;
use log::info;
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use tokio::sync::AcquireError;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::time::error::Elapsed;

use crate::sessions::QueryContext;

pub trait QueueData: Send + Sync + 'static {
    type Key: Send + Sync + Eq + Hash + Display + Clone + 'static;

    fn get_key(&self) -> Self::Key;

    fn remove_error_message(key: Option<Self::Key>) -> ErrorCode;

    fn timeout(&self) -> Duration;

    fn need_acquire_to_queue(&self) -> bool;

    fn enter_wait_pending(&self) {}

    fn exit_wait_pending(&self, _wait_time: Duration) {}
}

pub(crate) struct Inner<Data: QueueData> {
    pub data: Arc<Data>,
    pub waker: Waker,
    pub instant: Instant,
    pub is_abort: Arc<AtomicBool>,
}

pub struct QueueManager<Data: QueueData> {
    semaphore: Arc<Semaphore>,
    queue: Mutex<HashMap<Data::Key, Inner<Data>>>,
}

impl<Data: QueueData> QueueManager<Data> {
    pub fn init(permits: usize) -> Result<()> {
        info!("queue manager permits: {:?}", permits);
        GlobalInstance::set(Self::create(permits));
        Ok(())
    }

    pub fn instance() -> Arc<Self> {
        GlobalInstance::get::<Arc<Self>>()
    }

    pub fn create(mut permits: usize) -> Arc<QueueManager<Data>> {
        if permits == 0 {
            permits = usize::MAX >> 4;
        }

        Arc::new(QueueManager {
            queue: Mutex::new(HashMap::new()),
            semaphore: Arc::new(Semaphore::new(permits)),
        })
    }

    /// The length of the queue.
    pub fn length(&self) -> usize {
        let queue = self.queue.lock();
        queue.values().len()
    }

    pub fn list(&self) -> Vec<Arc<Data>> {
        let queue = self.queue.lock();
        queue.values().map(|x| x.data.clone()).collect::<Vec<_>>()
    }

    pub fn remove(&self, key: Data::Key) -> bool {
        let mut queue = self.queue.lock();
        if let Some(inner) = queue.remove(&key) {
            let queue_len = queue.len();
            drop(queue);
            set_session_queued_queries(queue_len);
            inner.data.exit_wait_pending(inner.instant.elapsed());
            inner.is_abort.store(true, Ordering::SeqCst);
            inner.waker.wake();
            true
        } else {
            set_session_queued_queries(queue.len());
            false
        }
    }

    pub async fn acquire(self: &Arc<Self>, data: Data) -> Result<AcquireQueueGuard> {
        if data.need_acquire_to_queue() {
            info!(
                "preparing to acquire from query queue, length: {}",
                self.length()
            );

            let timeout = data.timeout();
            let future = AcquireQueueFuture::create(
                Arc::new(data),
                tokio::time::timeout(timeout, self.semaphore.clone().acquire_owned()),
                self.clone(),
            );
            let start_time = SystemTime::now();

            return match future.await {
                Ok(v) => {
                    info!("finished acquiring from queue, length: {}", self.length());

                    inc_session_running_acquired_queries();
                    record_session_queue_acquire_duration_ms(
                        start_time.elapsed().unwrap_or_default(),
                    );
                    Ok(v)
                }
                Err(e) => {
                    match e.code() {
                        ErrorCode::ABORTED_QUERY => {
                            incr_session_queue_abort_count();
                        }
                        ErrorCode::TIMEOUT => {
                            incr_session_queue_acquire_timeout_count();
                        }
                        _ => {
                            incr_session_queue_acquire_error_count();
                        }
                    }
                    Err(e)
                }
            };
        }

        Ok(AcquireQueueGuard::create(None))
    }

    pub(crate) fn add_entity(&self, inner: Inner<Data>) -> Data::Key {
        inner.data.enter_wait_pending();

        let key = inner.data.get_key();
        let queue_len = {
            let mut queue = self.queue.lock();
            queue.insert(key.clone(), inner);
            queue.len()
        };

        set_session_queued_queries(queue_len);
        key
    }

    pub(crate) fn remove_entity(&self, key: &Data::Key) -> Option<Arc<Data>> {
        let mut queue = self.queue.lock();
        let inner = queue.remove(key);
        let queue_len = queue.len();

        drop(queue);
        set_session_queued_queries(queue_len);
        match inner {
            None => None,
            Some(inner) => {
                inner.data.exit_wait_pending(inner.instant.elapsed());
                Some(inner.data)
            }
        }
    }
}

pub struct AcquireQueueGuard {
    #[allow(dead_code)]
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for AcquireQueueGuard {
    fn drop(&mut self) {
        if self.permit.is_some() {
            dec_session_running_acquired_queries();
        }
    }
}

impl AcquireQueueGuard {
    pub fn create(permit: Option<OwnedSemaphorePermit>) -> Self {
        AcquireQueueGuard { permit }
    }
}

pin_project! {
    pub struct AcquireQueueFuture<Data: QueueData, T>
where T: Future<Output =  std::result::Result< std::result::Result<OwnedSemaphorePermit, AcquireError>, Elapsed>>
{
    #[pin]
    inner: T,


    has_pending: bool,
    is_abort: Arc<AtomicBool>,
    data: Option<Arc<Data>>,
    key: Option<Data::Key>,
    manager: Arc<QueueManager<Data>>,
}
}

impl<Data: QueueData, T> AcquireQueueFuture<Data, T>
where T: Future<
        Output = std::result::Result<
            std::result::Result<OwnedSemaphorePermit, AcquireError>,
            Elapsed,
        >,
    >
{
    pub fn create(data: Arc<Data>, inner: T, mgr: Arc<QueueManager<Data>>) -> Self {
        AcquireQueueFuture {
            inner,
            key: None,
            manager: mgr,
            data: Some(data),
            has_pending: false,
            is_abort: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<Data: QueueData, T> Future for AcquireQueueFuture<Data, T>
where T: Future<
        Output = std::result::Result<
            std::result::Result<OwnedSemaphorePermit, AcquireError>,
            Elapsed,
        >,
    >
{
    type Output = Result<AcquireQueueGuard>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if this.is_abort.load(Ordering::SeqCst) {
            return Poll::Ready(Err(Data::remove_error_message(this.key.take())));
        }

        match this.inner.poll(cx) {
            Poll::Ready(res) => {
                if let Some(key) = this.key.take() {
                    if this.manager.remove_entity(&key).is_none() {
                        return Poll::Ready(Err(Data::remove_error_message(Some(key))));
                    }
                }

                Poll::Ready(match res {
                    Ok(Ok(v)) => Ok(AcquireQueueGuard::create(Some(v))),
                    Ok(Err(_)) => Err(ErrorCode::TokioError("acquire queue failure.")),
                    Err(_elapsed) => Err(ErrorCode::Timeout("query queuing timeout")),
                })
            }
            Poll::Pending => {
                if !*this.has_pending {
                    *this.has_pending = true;
                }

                if let Some(data) = this.data.take() {
                    let waker = cx.waker().clone();
                    *this.key = Some(this.manager.add_entity(Inner {
                        data,
                        waker,
                        instant: Instant::now(),
                        is_abort: this.is_abort.clone(),
                    }));
                }

                Poll::Pending
            }
        }
    }
}

pub struct QueryEntry {
    ctx: Arc<QueryContext>,
    pub query_id: String,
    pub create_time: SystemTime,
    pub sql: String,
    pub user_info: UserInfo,
    pub timeout: Duration,
    pub need_acquire_to_queue: bool,
}

impl QueryEntry {
    pub fn create_entry(
        ctx: &Arc<QueryContext>,
        plan_extras: &PlanExtras,
        need_acquire_to_queue: bool,
    ) -> Result<QueryEntry> {
        let settings = ctx.get_settings();
        Ok(QueryEntry {
            ctx: ctx.clone(),
            need_acquire_to_queue,
            query_id: ctx.get_id(),
            create_time: ctx.get_created_time(),
            sql: plan_extras.statement.to_mask_sql(),
            user_info: ctx.get_current_user()?,
            timeout: match settings.get_statement_queued_timeout()? {
                0 => Duration::from_secs(60 * 60 * 24 * 365 * 35),
                timeout => Duration::from_secs(timeout),
            },
        })
    }

    pub fn create(
        ctx: &Arc<QueryContext>,
        plan: &Plan,
        plan_extras: &PlanExtras,
    ) -> Result<QueryEntry> {
        let need_add_to_queue = Self::is_heavy_action(plan);
        QueryEntry::create_entry(ctx, plan_extras, need_add_to_queue)
    }

    /// Check a plan is heavy action or not.
    /// If a plan is heavy action, it should add to the queue.
    /// If a plan is light action, it will skip to the queue.
    fn is_heavy_action(plan: &Plan) -> bool {
        // Check the query can be passed.
        fn query_need_passed(plan: &Plan) -> bool {
            match plan {
                Plan::Query { metadata, .. } => {
                    let metadata = metadata.read();
                    for table in metadata.tables() {
                        let db = table.database();
                        if db != "system" && db != "information_schema" {
                            return false;
                        }
                    }
                    true
                }
                _ => true,
            }
        }

        match plan {
            // Query: Heavy actions.
            Plan::Query { .. } => {
                if !query_need_passed(plan) {
                    return true;
                }
            }

            Plan::ExplainAnalyze { plan, .. }
            | Plan::Explain {
                kind: ExplainKind::AnalyzePlan,
                plan,
                ..
            } => {
                if !query_need_passed(plan) {
                    return true;
                }
            }

            // Write: Heavy actions.
            Plan::Insert(_)
            | Plan::InsertMultiTable(_)
            | Plan::Replace(_)
            | Plan::DataMutation { .. }
            | Plan::CopyIntoTable(_)
            | Plan::CopyIntoLocation(_) => {
                return true;
            }

            // DDL: Heavy actions.
            Plan::OptimizePurge(_)
            | Plan::OptimizeCompactSegment(_)
            | Plan::OptimizeCompactBlock { .. }
            | Plan::VacuumTable(_)
            | Plan::VacuumTemporaryFiles(_)
            | Plan::RefreshIndex(_)
            | Plan::ReclusterTable(_)
            | Plan::TruncateTable(_) => {
                return true;
            }
            Plan::DropTable(v) if v.all => {
                return true;
            }
            Plan::ModifyTableColumn(box ModifyTableColumnPlan {
                action: ModifyColumnAction::SetDataType(_),
                ..
            }) => {
                return true;
            }

            // Light actions.
            _ => {
                return false;
            }
        }

        // Light actions.
        false
    }
}

impl QueueData for QueryEntry {
    type Key = String;

    fn get_key(&self) -> Self::Key {
        self.query_id.clone()
    }

    fn remove_error_message(key: Option<Self::Key>) -> ErrorCode {
        match key {
            None => ErrorCode::AbortedQuery("The query has be kill while in queries queue"),
            Some(key) => ErrorCode::AbortedQuery(format!(
                "The query {} has be kill while in queries queue",
                key
            )),
        }
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn need_acquire_to_queue(&self) -> bool {
        self.need_acquire_to_queue
    }

    fn enter_wait_pending(&self) {
        self.ctx.set_status_info("resources scheduling");
    }

    fn exit_wait_pending(&self, wait_time: Duration) {
        self.ctx
            .set_status_info(format!("resource scheduled(elapsed: {:?})", wait_time).as_str());
        self.ctx.set_query_queued_duration(wait_time)
    }
}

pub type QueriesQueueManager = QueueManager<QueryEntry>;
