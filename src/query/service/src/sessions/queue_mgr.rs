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
use databend_common_base::base::escape_for_key;
use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_semaphore::acquirer::Permit;
use databend_common_meta_semaphore::errors::AcquireError;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
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
use tokio::sync::AcquireError as TokioAcquireError;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::time::error::Elapsed;

use crate::sessions::QueryContext;

pub trait QueueData: Send + Sync + 'static {
    type Key: Send + Sync + Eq + Hash + Display + Clone + ToString + 'static;

    fn get_key(&self) -> Self::Key;

    fn get_lock_key(&self) -> String;

    fn remove_error_message(key: Option<Self::Key>) -> ErrorCode;

    fn lock_ttl(&self) -> Duration;

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
    permits: usize,
    meta_store: MetaStore,
    semaphore: Arc<Semaphore>,
    global_statement_queue: bool,
    queue: Mutex<HashMap<Data::Key, Inner<Data>>>,
}

impl<Data: QueueData> QueueManager<Data> {
    pub async fn init(permits: usize, conf: &InnerConfig) -> Result<()> {
        let metastore = {
            let provider = Arc::new(MetaStoreProvider::new(conf.meta.to_meta_grpc_client_conf()));

            provider.create_meta_store().await.map_err(|e| {
                ErrorCode::MetaServiceError(format!(
                    "[QUERY-QUEUE] Failed to create meta store: {}",
                    e
                ))
            })?
        };

        info!(
            "[QUERY-QUEUE] Queue manager initialized with permits: {:?}",
            permits
        );
        GlobalInstance::set(Self::create(
            permits,
            metastore,
            conf.query.global_statement_queue,
        ));
        Ok(())
    }

    pub fn instance() -> Arc<Self> {
        GlobalInstance::get::<Arc<Self>>()
    }

    pub fn create(
        mut permits: usize,
        meta_store: MetaStore,
        global_statement_queue: bool,
    ) -> Arc<QueueManager<Data>> {
        if permits == 0 {
            permits = usize::MAX >> 4;
        }

        Arc::new(QueueManager {
            permits,
            meta_store,
            global_statement_queue,
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
                "[QUERY-QUEUE] Preparing to acquire from query queue, current length: {}",
                self.length()
            );

            let timeout = data.timeout();

            let start_time = SystemTime::now();
            let acquire_res = match self.global_statement_queue {
                true => {
                    let semaphore_acquire = self.meta_store.new_acquired_by_time(
                        data.get_lock_key(),
                        self.permits as u64,
                        data.get_key(), // ID of this acquirer
                        data.lock_ttl(),
                    );

                    AcquireQueueFuture::create(
                        Arc::new(data),
                        tokio::time::timeout(timeout, semaphore_acquire),
                        self.clone(),
                    )
                    .await
                }
                false => {
                    AcquireQueueFuture::create(
                        Arc::new(data),
                        tokio::time::timeout(timeout, self.semaphore.clone().acquire_owned()),
                        self.clone(),
                    )
                    .await
                }
            };

            return match acquire_res {
                Ok(v) => {
                    info!(
                        "[QUERY-QUEUE] Successfully acquired from queue, current length: {}",
                        self.length()
                    );

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

        Ok(AcquireQueueGuard::create_global(None))
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

#[derive(Debug)]
pub enum AcquireQueueGuard {
    Global(Option<Permit>),
    Local(Option<OwnedSemaphorePermit>),
}

impl Drop for AcquireQueueGuard {
    fn drop(&mut self) {
        match self {
            AcquireQueueGuard::Local(Some(_)) | AcquireQueueGuard::Global(Some(_)) => {
                dec_session_running_acquired_queries();
            }
            _ => {}
        }
    }
}

impl AcquireQueueGuard {
    pub fn create_global(permit: Option<Permit>) -> Self {
        AcquireQueueGuard::Global(permit)
    }

    pub fn create_local(permit: Option<OwnedSemaphorePermit>) -> Self {
        AcquireQueueGuard::Local(permit)
    }
}

pin_project! {
pub struct AcquireQueueFuture<Data: QueueData, T, Permit, E>
where T: Future<Output =  std::result::Result<std::result::Result<Permit, E>, Elapsed>>
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

impl<Data: QueueData, T, Permit, E> AcquireQueueFuture<Data, T, Permit, E>
where T: Future<Output = std::result::Result<std::result::Result<Permit, E>, Elapsed>>
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

macro_rules! impl_acquire_queue_future {
    ($Permit:ty, $fn_name:ident, $Error:ty) => {
        impl<Data: QueueData, T> Future for AcquireQueueFuture<Data, T, $Permit, $Error>
        where T: Future<Output = std::result::Result<std::result::Result<$Permit, $Error>, Elapsed>>
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
                            Ok(Ok(v)) => Ok(AcquireQueueGuard::$fn_name(Some(v))),
                            Ok(Err(_)) => Err(ErrorCode::TokioError(
                                "[QUERY-QUEUE] Queue acquisition failed",
                            )),
                            Err(_elapsed) => Err(ErrorCode::Timeout(
                                "[QUERY-QUEUE] Query queuing timeout exceeded",
                            )),
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
    };
}

impl_acquire_queue_future!(Permit, create_global, AcquireError);
impl_acquire_queue_future!(OwnedSemaphorePermit, create_local, TokioAcquireError);

pub struct QueryEntry {
    ctx: Arc<QueryContext>,
    pub query_id: String,
    pub create_time: SystemTime,
    pub sql: String,
    pub user_info: UserInfo,
    pub timeout: Duration,
    pub lock_ttl: Duration,
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
            lock_ttl: Duration::from_secs(settings.get_statement_queue_ttl_in_seconds()?),
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

    fn get_lock_key(&self) -> String {
        let cluster = self.ctx.get_cluster();
        let local_id = escape_for_key(&cluster.local_id).unwrap();
        let mut lock_key = format!("__fd_queries_queue/lost_contact/{}", local_id);

        for node in &cluster.nodes {
            if node.id == cluster.local_id {
                let cluster_id = escape_for_key(&node.cluster_id).unwrap();
                let warehouse_id = escape_for_key(&node.warehouse_id).unwrap();
                lock_key = format!("__fd_queries_queue/queue/{}/{}", warehouse_id, cluster_id);
            }
        }

        lock_key
    }

    fn remove_error_message(key: Option<Self::Key>) -> ErrorCode {
        match key {
            None => ErrorCode::AbortedQuery("[QUERY-QUEUE] Query was killed while in queue"),
            Some(key) => ErrorCode::AbortedQuery(format!(
                "[QUERY-QUEUE] Query {} was killed while in queue",
                key
            )),
        }
    }

    fn lock_ttl(&self) -> Duration {
        self.lock_ttl
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn need_acquire_to_queue(&self) -> bool {
        self.need_acquire_to_queue
    }

    fn enter_wait_pending(&self) {
        self.ctx
            .set_status_info("[QUERY-QUEUE] Waiting for resource scheduling");
    }

    fn exit_wait_pending(&self, wait_time: Duration) {
        self.ctx.set_status_info(
            format!(
                "[QUERY-QUEUE] Resource scheduling completed, elapsed: {:?}",
                wait_time
            )
            .as_str(),
        );
        self.ctx.set_query_queued_duration(wait_time)
    }
}

pub type QueriesQueueManager = QueueManager<QueryEntry>;
