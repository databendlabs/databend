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
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::SystemTime;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserInfo;
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use tokio::sync::AcquireError;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::sessions::QueryContext;

pub trait QueueData: Send + Sync + 'static {
    type Key: Send + Sync + Eq + Hash + Clone + 'static;

    fn get_key(&self) -> Self::Key;

    fn remove_error_message(key: Self::Key) -> ErrorCode;
}

pub struct QueueManager<Data: QueueData> {
    semaphore: Arc<Semaphore>,
    queue: Mutex<HashMap<Data::Key, Arc<Data>>>,
}

impl<Data: QueueData> QueueManager<Data> {
    pub fn init(conf: &InnerConfig) -> Result<()> {
        GlobalInstance::set(Self::create(conf.query.max_active_sessions as usize));
        Ok(())
    }

    pub fn instants() -> Arc<Self> {
        GlobalInstance::get::<Arc<Self>>()
    }

    pub fn create(mut permits: usize) -> Arc<QueueManager<Data>> {
        if permits == 0 {
            permits = usize::MAX;
        }

        Arc::new(QueueManager {
            queue: Mutex::new(HashMap::new()),
            semaphore: Arc::new(Semaphore::new(permits)),
        })
    }

    pub fn list(&self) -> Vec<Arc<Data>> {
        let queue = self.queue.lock();
        queue.values().cloned().collect::<Vec<_>>()
    }

    pub fn remove(&self, key: Data::Key) {
        let mut queue = self.queue.lock();
        queue.remove(&key);
    }

    pub async fn acquire(self: &Arc<Self>, data: Data) -> Result<AcquireQueueGuard> {
        let future = AcquireQueueFuture::create(
            Arc::new(data),
            self.semaphore.clone().acquire_owned(),
            self.clone(),
        );

        future.await
    }

    pub(crate) fn add_entity(&self, data: Arc<Data>) -> Data::Key {
        let key = data.get_key();
        let mut queue = self.queue.lock();
        queue.insert(key.clone(), data);
        key
    }

    pub(crate) fn remove_entity(&self, key: &Data::Key) -> Option<Arc<Data>> {
        let mut queue = self.queue.lock();
        queue.remove(key)
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
where T: Future<Output = Result<OwnedSemaphorePermit, AcquireError>>
{
    #[pin]
    inner: T,

    has_pending: bool,
    data: Option<Arc<Data>>,
    key: Option<Data::Key>,
    manager: Arc<QueueManager<Data>>,
}
}

impl<Data: QueueData, T> AcquireQueueFuture<Data, T>
where T: Future<Output = Result<OwnedSemaphorePermit, AcquireError>>
{
    pub fn create(data: Arc<Data>, inner: T, mgr: Arc<QueueManager<Data>>) -> Self {
        AcquireQueueFuture {
            inner,
            key: None,
            manager: mgr,
            data: Some(data),
            has_pending: false,
        }
    }
}

impl<Data: QueueData, T> Future for AcquireQueueFuture<Data, T>
where T: Future<Output = Result<OwnedSemaphorePermit, AcquireError>>
{
    type Output = Result<AcquireQueueGuard>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.inner.poll(cx) {
            Poll::Ready(res) => {
                if let Some(key) = this.key.take() {
                    if this.manager.remove_entity(&key).is_none() {
                        return Poll::Ready(Err(Data::remove_error_message(key)));
                    }
                }

                Poll::Ready(match res {
                    Ok(v) => Ok(AcquireQueueGuard::create(v)),
                    Err(_) => Err(ErrorCode::TokioError("acquire queue failure.")),
                })
            }
            Poll::Pending => {
                if !*this.has_pending {
                    *this.has_pending = true;
                }

                if let Some(data) = this.data.take() {
                    *this.key = Some(this.manager.add_entity(data));
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
}

impl QueryEntry {
    pub fn create(ctx: &Arc<QueryContext>) -> Result<QueryEntry> {
        Ok(QueryEntry {
            query_id: ctx.get_id(),
            create_time: ctx.get_created_time(),
            user_info: ctx.get_current_user()?,
        })
    }
}

impl QueueData for QueryEntry {
    type Key = String;

    fn get_key(&self) -> Self::Key {
        self.query_id.clone()
    }

    fn remove_error_message(key: Self::Key) -> ErrorCode {
        ErrorCode::AbortedQuery(format!(
            "The query {} has be kill while in queries queue",
            key
        ))
    }
}

pub type QueriesQueueManager = QueueManager<QueryEntry>;
