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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::tokio::sync::RwLock;
use databend_common_base::base::tokio::time::sleep;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use log::warn;
use parking_lot::Mutex;

use super::expiring_map::ExpiringMap;
use super::HttpQueryContext;
use crate::servers::http::v1::query::http_query::ExpireResult;
use crate::servers::http::v1::query::http_query::HttpQuery;
use crate::servers::http::v1::query::HttpQueryRequest;
use crate::sessions::Session;

#[derive(Clone, Debug)]
pub(crate) enum RemoveReason {
    Timeout,
    Canceled,
    Finished,
}

impl Display for RemoveReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{self:?}").to_lowercase())
    }
}

pub(crate) struct SizeLimitedIndexMap<K, V> {
    insert_order: VecDeque<K>,
    reasons: HashMap<K, V>,
    cap: usize,
}

impl<K: Clone + Eq + Hash, V> SizeLimitedIndexMap<K, V> {
    fn new(cap: usize) -> Self {
        Self {
            insert_order: VecDeque::new(),
            reasons: HashMap::new(),
            cap,
        }
    }

    fn insert(&mut self, key: K, value: V) {
        if self.get(&key).is_some() {
            return;
        }
        if self.insert_order.len() + 1 >= self.cap {
            let key = self.insert_order.pop_front().unwrap();
            self.reasons.remove(&key);
        }
        self.insert_order.push_back(key.clone());
        self.reasons.insert(key, value);
    }

    fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.reasons.get(key)
    }
}

pub struct HttpQueryManager {
    #[allow(clippy::type_complexity)]
    pub(crate) queries: Arc<RwLock<HashMap<String, Arc<HttpQuery>>>>,
    pub(crate) sessions: Mutex<ExpiringMap<String, Arc<Session>>>,
    pub(crate) removed_queries: Arc<RwLock<SizeLimitedIndexMap<String, RemoveReason>>>,
}

impl HttpQueryManager {
    #[async_backtrace::framed]
    pub async fn init(_cfg: &InnerConfig) -> Result<()> {
        GlobalInstance::set(Arc::new(HttpQueryManager {
            queries: Arc::new(RwLock::new(HashMap::new())),
            sessions: Mutex::new(ExpiringMap::default()),
            removed_queries: Arc::new(RwLock::new(SizeLimitedIndexMap::new(1000))),
        }));

        Ok(())
    }

    pub fn instance() -> Arc<HttpQueryManager> {
        GlobalInstance::get()
    }

    #[async_backtrace::framed]
    pub(crate) async fn try_create_query(
        self: &Arc<Self>,
        ctx: &HttpQueryContext,
        request: HttpQueryRequest,
    ) -> Result<Arc<HttpQuery>> {
        let query = HttpQuery::try_create(ctx, request).await?;
        self.add_query(&query.id, query.clone()).await;
        Ok(query)
    }

    #[async_backtrace::framed]
    pub(crate) async fn get_query(self: &Arc<Self>, query_id: &str) -> Option<Arc<HttpQuery>> {
        let queries = self.queries.read().await;
        queries.get(query_id).map(|q| q.to_owned())
    }

    #[async_backtrace::framed]
    pub(crate) async fn try_get_query(
        self: &Arc<Self>,
        query_id: &str,
    ) -> std::result::Result<Arc<HttpQuery>, Option<RemoveReason>> {
        if let Some(q) = self.get_query(query_id).await {
            Ok(q)
        } else {
            Err(self.try_get_query_tombstone(query_id).await)
        }
    }

    #[async_backtrace::framed]
    pub(crate) async fn try_get_query_tombstone(
        self: &Arc<Self>,
        query_id: &str,
    ) -> Option<RemoveReason> {
        let queries = self.removed_queries.read().await;
        queries.get(query_id).cloned()
    }

    #[async_backtrace::framed]
    async fn add_query(self: &Arc<Self>, query_id: &str, query: Arc<HttpQuery>) {
        let mut queries = self.queries.write().await;
        queries.insert(query_id.to_string(), query.clone());

        let self_clone = self.clone();
        let query_id_clone = query_id.to_string();
        let query_result_timeout_secs = query.result_timeout_secs;

        // downgrade to weak reference
        // it may cannot destroy with final or kill when we hold ref of Arc<HttpQuery>
        let http_query_weak = Arc::downgrade(&query);

        GlobalIORuntime::instance().spawn(query_id, async move {
            loop {
                let expire_res = match http_query_weak.upgrade() {
                    None => {
                        break;
                    }
                    Some(query) => query.check_expire().await,
                };

                match expire_res {
                    ExpireResult::Expired => {
                        let msg = format!(
                            "http query {} timeout after {} s",
                            &query_id_clone, query_result_timeout_secs
                        );
                        match self_clone
                            .remove_query(&query_id_clone, RemoveReason::Timeout)
                            .await
                        {
                            Ok(_) => {
                                warn!("{msg}");
                                if let Some(query) = http_query_weak.upgrade() {
                                    query.kill(&msg).await;
                                }
                            }
                            Err(_) => {
                                warn!("{msg}, but already removed");
                            }
                        };
                        break;
                    }
                    ExpireResult::Sleep(t) => {
                        sleep(t).await;
                    }
                    ExpireResult::Removed => {
                        break;
                    }
                }
            }
        });
    }

    // not remove it until timeout or cancelled by user, even if query execution is aborted
    #[async_backtrace::framed]
    pub(crate) async fn remove_query(
        self: &Arc<Self>,
        query_id: &str,
        reason: RemoveReason,
    ) -> std::result::Result<Arc<HttpQuery>, Option<RemoveReason>> {
        {
            let mut removed = self.removed_queries.write().await;
            if let Some(r) = removed.get(query_id) {
                return Err(Some(r.clone()));
            }
            removed.insert(query_id.to_string(), reason);
        }
        let mut queries = self.queries.write().await;
        let q = queries.remove(query_id);
        if let Some(q) = &q {
            q.mark_removed().await;
            Ok(q.clone())
        } else {
            Err(None)
        }
    }

    #[async_backtrace::framed]
    pub(crate) async fn get_session(self: &Arc<Self>, session_id: &str) -> Option<Arc<Session>> {
        let sessions = self.sessions.lock();
        sessions.get(session_id)
    }

    #[async_backtrace::framed]
    pub(crate) async fn add_session(self: &Arc<Self>, session: Arc<Session>, timeout: Duration) {
        let mut sessions = self.sessions.lock();
        sessions.insert(session.get_id(), session, Some(timeout));
    }

    pub(crate) fn kill_session(self: &Arc<Self>, session_id: &str) {
        let mut sessions = self.sessions.lock();
        sessions.remove(session_id);
    }
}
