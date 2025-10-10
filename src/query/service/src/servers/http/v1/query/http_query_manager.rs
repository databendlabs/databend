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
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use chrono::SecondsFormat;
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
use parking_lot::RwLock;

use crate::servers::http::v1::query::http_query::ClientStateClosed;
use crate::servers::http::v1::query::http_query::HttpQuery;
use crate::servers::http::v1::query::http_query::ServerInfo;
use crate::servers::http::v1::query::http_query::TimeoutResult;

#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub enum CloseReason {
    Finalized,
    Canceled,
    TimedOut,
}

impl Display for CloseReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            CloseReason::Finalized => write!(f, "finalized"),
            CloseReason::Canceled => write!(f, "canceled"),
            CloseReason::TimedOut => write!(f, "timed out"),
        }
    }
}

#[derive(Default)]
struct Queries {
    active: HashMap<String, Arc<HttpQuery>>,
    num_active_queries: u64,
    last_query_end_at: Option<u64>,
}

impl Queries {
    pub(crate) fn get(&self, query_id: &str) -> Option<Arc<HttpQuery>> {
        self.active.get(query_id).cloned()
    }

    pub(crate) fn insert(&mut self, query: Arc<HttpQuery>) {
        self.num_active_queries += 1;
        self.active.insert(query.id.clone(), query);
    }

    pub(crate) fn remove(&mut self, query_id: &str) -> Option<Arc<HttpQuery>> {
        self.active.remove(query_id)
    }

    pub(crate) fn close(
        &mut self,
        query_id: &str,
        reason: CloseReason,
        now: u64,
        client_session_id: &Option<String>,
        check_client_session_id: bool,
    ) -> poem::error::Result<(Option<Arc<HttpQuery>>, Option<ClientStateClosed>)> {
        let q = self.active.get(query_id).cloned();
        if let Some(q) = q {
            if check_client_session_id {
                q.check_client_session_id(client_session_id)?;
            }
            let closed_state = q.mark_closed(reason);
            if let Some(st) = closed_state {
                self.last_query_end_at = Some(now);
                self.num_active_queries = self.num_active_queries.saturating_sub(1);
                log::info!("[HTTP-QUERY] Query {query_id} closed: {st:?}");
            }
            return Ok((Some(q), closed_state));
        }
        Ok((None, None))
    }

    pub(crate) fn status(&self) -> (u64, Option<u64>) {
        (self.num_active_queries, self.last_query_end_at)
    }
}

pub struct HttpQueryManager {
    pub(crate) start_instant: Instant,
    pub(crate) server_info: ServerInfo,
    queries: RwLock<Queries>,
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
            queries: Default::default(),
            txn_managers: Arc::new(Mutex::new(HashMap::new())),
        }));

        Ok(())
    }

    pub fn instance() -> Arc<HttpQueryManager> {
        GlobalInstance::get()
    }

    pub(crate) fn status(self: &Arc<Self>) -> (u64, Option<u64>) {
        self.queries.read().status()
    }

    pub(crate) fn get_query(self: &Arc<Self>, query_id: &str) -> Option<Arc<HttpQuery>> {
        self.queries.read().get(query_id).map(|q| q.to_owned())
    }

    #[async_backtrace::framed]
    pub async fn add_query(self: &Arc<Self>, query: HttpQuery) -> Arc<HttpQuery> {
        let query = Arc::new(query);
        self.queries.write().insert(query.clone());

        let self_clone = self.clone();
        let query_id_clone = query.id.clone();

        // downgrade to weak reference
        // it may cannot destroy with final or kill when we hold ref of Arc<HttpQuery>
        let http_query_weak = Arc::downgrade(&query);

        GlobalIORuntime::instance().spawn(async move {
            loop {
                let expire_res = match http_query_weak.upgrade() {
                    None => {
                        break;
                    }
                    Some(query) => query.check_timeout().await,
                };

                match expire_res {
                    TimeoutResult::TimedOut => {
                        _ = self_clone
                            .close_query(&query_id_clone, CloseReason::TimedOut, &None, false)
                            .await
                            .ok();
                    }
                    TimeoutResult::Sleep(t) => {
                        sleep(t).await;
                    }
                    TimeoutResult::Remove => {
                        let mut queries = self_clone.queries.write();
                        queries.remove(&query_id_clone);
                        log::info!("[HTTP-QUERY] Query {query_id_clone} removed");
                        break;
                    }
                }
            }
        });
        query
    }

    #[async_backtrace::framed]
    pub(crate) async fn close_query(
        self: &Arc<Self>,
        query_id: &str,
        reason: CloseReason,
        client_session_id: &Option<String>,
        check_client_session_id: bool,
    ) -> poem::error::Result<Option<Arc<HttpQuery>>> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        let (query, closed_state) = self.queries.write().close(
            query_id,
            reason,
            now,
            client_session_id,
            check_client_session_id,
        )?;
        if let Some(q) = &query {
            if let Some(st) = closed_state {
                q.kill(st.error_code(q.result_timeout_secs)).await;
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

    pub(crate) fn check_sticky_for_txn(&self, last_node_id: &Option<String>) -> Result<()> {
        if let Some(id) = last_node_id {
            if self.server_info.id != *id {
                return Err(ErrorCode::SessionLost(format!(
                    "[HTTP-QUERY] Transaction aborted because server restart or route error: expecting server {}, current one is {} started at {} ",
                    id, self.server_info.id, self.server_info.start_time
                )));
            }
        } else {
            return Err(ErrorCode::InvalidSessionState(
                "[HTTP-QUERY] Transaction is active but missing server_info".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn on_heartbeat(&self, query_ids: Vec<String>) -> Vec<String> {
        let mut to_stop = vec![];
        for query_id in query_ids {
            let stop_heartbeat = self
                .queries
                .read()
                .get(&query_id)
                .map(|q| q.on_heartbeat())
                .unwrap_or(true);
            if stop_heartbeat {
                to_stop.push(query_id);
            }
        }
        to_stop
    }
}
