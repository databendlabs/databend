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
use std::time::SystemTime;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserInfo;
use databend_common_metrics::session::incr_session_queue_abort_count;
use databend_common_metrics::session::incr_session_queue_acquire_error_count;
use databend_common_metrics::session::incr_session_queue_acquire_timeout_count;
use databend_common_metrics::session::record_session_queue_acquire_duration_ms;
use databend_common_metrics::session::set_session_queued_queries;
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
}

pub(crate) struct Inner<Data: QueueData> {
    pub data: Arc<Data>,
    pub waker: Waker,
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

    pub fn list(&self) -> Vec<Arc<Data>> {
        let queue = self.queue.lock();
        queue.values().map(|x| x.data.clone()).collect::<Vec<_>>()
    }

    pub fn remove(&self, key: Data::Key) -> bool {
        let mut queue = self.queue.lock();
        if let Some(inner) = queue.remove(&key) {
            set_session_queued_queries(queue.len());
            inner.waker.wake();
            inner.is_abort.store(true, Ordering::SeqCst);
            true
        } else {
            set_session_queued_queries(queue.len());
            false
        }
    }

    pub async fn acquire(self: &Arc<Self>, data: Data) -> Result<AcquireQueueGuard> {
        let timeout = data.timeout();
        let future = AcquireQueueFuture::create(
            Arc::new(data),
            tokio::time::timeout(timeout, self.semaphore.clone().acquire_owned()),
            self.clone(),
        );
        let start_time = SystemTime::now();

        match future.await {
            Ok(v) => {
                record_session_queue_acquire_duration_ms(start_time.elapsed().unwrap_or_default());
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
        }
    }

    pub(crate) fn add_entity(&self, inner: Inner<Data>) -> Data::Key {
        let key = inner.data.get_key();
        let mut queue = self.queue.lock();
        queue.insert(key.clone(), inner);
        set_session_queued_queries(queue.len());
        key
    }

    pub(crate) fn remove_entity(&self, key: &Data::Key) -> Option<Arc<Data>> {
        let mut queue = self.queue.lock();
        let data = queue.remove(key).map(|inner| inner.data.clone());
        set_session_queued_queries(queue.len());
        data
    }
}

pub struct AcquireQueueGuard {
    #[allow(dead_code)]
    permit: OwnedSemaphorePermit,
}

impl AcquireQueueGuard {
    pub fn create(permit: OwnedSemaphorePermit) -> Self {
        AcquireQueueGuard { permit }
    }
}

pin_project! {
    pub struct AcquireQueueFuture<Data: QueueData, T>
where T: Future<Output = Result<Result<OwnedSemaphorePermit, AcquireError>, Elapsed>>
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
where T: Future<Output = Result<Result<OwnedSemaphorePermit, AcquireError>, Elapsed>>
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
where T: Future<Output = Result<Result<OwnedSemaphorePermit, AcquireError>, Elapsed>>
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
                    Ok(Ok(v)) => Ok(AcquireQueueGuard::create(v)),
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
                        is_abort: this.is_abort.clone(),
                    }));
                }

                Poll::Pending
            }
        }
    }
}

pub struct QueryEntry {
    pub query_id: String,
    pub create_time: SystemTime,
    pub user_info: UserInfo,
    pub timeout: Duration,
}

impl QueryEntry {
    pub fn create(ctx: &Arc<QueryContext>) -> Result<QueryEntry> {
        let settings = ctx.get_settings();
        Ok(QueryEntry {
            query_id: ctx.get_id(),
            create_time: ctx.get_created_time(),
            user_info: ctx.get_current_user()?,
            timeout: match settings.get_statement_queued_timeout()? {
                0 => Duration::from_secs(60 * 60 * 24 * 365 * 35),
                timeout => Duration::from_secs(timeout),
            },
        })
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
}

pub type QueriesQueueManager = QueueManager<QueryEntry>;
