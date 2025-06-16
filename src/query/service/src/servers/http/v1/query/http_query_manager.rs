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
use std::collections::VecDeque;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use chrono::SecondsFormat;
use dashmap::DashMap;
use databend_common_base::base::tokio::time::sleep;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::JoinHandle;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_session::TxnManagerRef;
use parking_lot::Mutex;

use crate::servers::http::v1::query::http_query::ExpireResult;
use crate::servers::http::v1::query::http_query::HttpQuery;
use crate::servers::http::v1::query::http_query::ServerInfo;

#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub enum RemoveReason {
    Timeout,
    Canceled,
    Finished,
}

impl Display for RemoveReason {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", format!("{self:?}").to_lowercase())
    }
}

pub struct LimitedQueue<T> {
    deque: VecDeque<T>,
    max_size: usize,
}

impl<T> LimitedQueue<T> {
    fn new(max_size: usize) -> Self {
        LimitedQueue {
            deque: VecDeque::new(),
            max_size,
        }
    }

    fn push(&mut self, item: T) -> Option<T> {
        self.deque.push_back(item);
        if self.deque.len() > self.max_size {
            self.deque.pop_front()
        } else {
            None
        }
    }
}

pub struct HttpQueryManager {
    pub(crate) start_instant: Instant,
    pub(crate) server_info: ServerInfo,
    #[allow(clippy::type_complexity)]
    pub(crate) queries: Arc<DashMap<String, Arc<HttpQuery>>>,
    pub(crate) removed_queries: Arc<parking_lot::Mutex<LimitedQueue<String>>>,
    #[allow(clippy::type_complexity)]
    pub(crate) txn_managers: Arc<Mutex<HashMap<String, (TxnManagerRef, JoinHandle<()>)>>>,
}

impl HttpQueryManager {
    #[async_backtrace::framed]
    pub async fn init(cfg: &InnerConfig) -> Result<()> {
        GlobalInstance::set(Arc::new(HttpQueryManager {
            start_instant: Instant::now(),
            server_info: ServerInfo {
                id: cfg.query.node_id.clone(),
                start_time: chrono::Local::now().to_rfc3339_opts(SecondsFormat::Millis, false),
            },
            queries: Arc::new(DashMap::new()),
            removed_queries: Arc::new(Mutex::new(LimitedQueue::new(1000))),
            txn_managers: Arc::new(Mutex::new(HashMap::new())),
        }));

        Ok(())
    }

    pub fn instance() -> Arc<HttpQueryManager> {
        GlobalInstance::get()
    }

    pub(crate) fn get_query(self: &Arc<Self>, query_id: &str) -> Option<Arc<HttpQuery>> {
        self.queries.get(query_id).map(|q| q.to_owned())
    }

    #[async_backtrace::framed]
    pub async fn add_query(self: &Arc<Self>, query: HttpQuery) -> Arc<HttpQuery> {
        let query = Arc::new(query);
        self.queries.insert(query.id.clone(), query.clone());

        let self_clone = self.clone();
        let query_id_clone = query.id.clone();
        let query_result_timeout_secs = query.result_timeout_secs;

        // downgrade to weak reference
        // it may cannot destroy with final or kill when we hold ref of Arc<HttpQuery>
        let http_query_weak = Arc::downgrade(&query);

        GlobalIORuntime::instance().spawn(async move {
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
                            "[HTTP-QUERY] Query {} timed out after {} seconds",
                            &query_id_clone, query_result_timeout_secs
                        );
                        _ = self_clone
                            .remove_query(
                                &query_id_clone,
                                &None,
                                RemoveReason::Timeout,
                                ErrorCode::AbortedQuery(&msg),
                            )
                            .await
                            .ok();
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

        query
    }

    #[async_backtrace::framed]
    pub(crate) async fn remove_query(
        self: &Arc<Self>,
        query_id: &str,
        client_session_id: &Option<String>,
        reason: RemoveReason,
        error: ErrorCode,
    ) -> poem::error::Result<Option<Arc<HttpQuery>>> {
        // deref at once to avoid holding DashMap shard guard for too long.
        let query = self.queries.get(query_id).map(|q| q.clone());
        if let Some(q) = &query {
            if reason != RemoveReason::Timeout {
                q.check_client_session_id(client_session_id)?;
            }
            if q.mark_removed(reason) {
                q.kill(error).await;
                let mut queue = self.removed_queries.lock();
                if let Some(to_evict) = queue.push(q.id.to_string()) {
                    self.queries.remove(&to_evict);
                };
            }
        }
        Ok(query)
    }

    #[async_backtrace::framed]
    pub(crate) async fn add_txn(
        self: &Arc<Self>,
        last_query_id: String,
        txn_mgr: TxnManagerRef,
        timeout_secs: u64,
    ) {
        let mut txn_managers = self.txn_managers.lock();
        let deleter = {
            let self_clone = self.clone();
            let last_query_id_clone = last_query_id.clone();
            GlobalIORuntime::instance().spawn(async move {
                sleep(Duration::from_secs(timeout_secs)).await;
                if self_clone.get_txn(&last_query_id_clone).is_some() {
                    log::info!(
                        "[HTTP-QUERY] Transaction timed out after {} seconds, last_query_id = {}",
                        timeout_secs,
                        last_query_id_clone
                    );
                }
            })
        };
        txn_managers.insert(last_query_id, (txn_mgr, deleter));
    }

    #[async_backtrace::framed]
    pub(crate) fn get_txn(self: &Arc<Self>, last_query_id: &str) -> Option<TxnManagerRef> {
        let mut txn_managers = self.txn_managers.lock();
        if let Some((txn_mgr, task_handle)) = txn_managers.remove(last_query_id) {
            task_handle.abort();
            Some(txn_mgr)
        } else {
            None
        }
    }

    pub(crate) fn check_sticky_for_txn(&self, last_server_info: &Option<ServerInfo>) -> Result<()> {
        if let Some(ServerInfo { id, start_time }) = last_server_info {
            if self.server_info.id != *id {
                return Err(ErrorCode::InvalidSessionState(format!(
                    "[HTTP-QUERY] Transaction is active but request was routed to the wrong server: current server is {}, expected server is {}",
                    self.server_info.id, id
                )));
            }
            if self.server_info.start_time != *start_time {
                return Err(ErrorCode::CurrentTransactionIsAborted(format!(
                    "[HTTP-QUERY] Transaction was aborted because server restarted at {}",
                    start_time
                )));
            }
        } else {
            return Err(ErrorCode::InvalidSessionState(
                "[HTTP-QUERY] Transaction is active but missing server_info".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn check_sticky_for_temp_table(
        &self,
        last_server_info: &Option<ServerInfo>,
    ) -> std::result::Result<(), String> {
        if let Some(ServerInfo { id, start_time }) = last_server_info {
            if self.server_info.id != *id {
                return Err(format!(
                    "contains temporary tables but request was routed to the wrong server: current server is {}, expected server is {}",
                    self.server_info.id, id
                ));
            }
            if self.server_info.start_time != *start_time {
                return Err(format!(
                    "temporary tables were lost because server restarted at {}",
                    start_time
                ));
            }
        } else {
            return Err("contains temporary tables but missing last_server_info field".to_string());
        }
        Ok(())
    }

    pub(crate) fn on_heartbeat(&self, query_ids: Vec<String>) -> Vec<String> {
        let mut failed = vec![];
        for query_id in query_ids {
            if !self
                .queries
                .get(&query_id)
                .map(|q| q.on_heartbeat())
                .unwrap_or(false)
            {
                failed.push(query_id);
            }
        }
        failed
    }
}
