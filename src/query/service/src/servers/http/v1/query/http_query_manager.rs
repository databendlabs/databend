// Copyright 2021 Datafuse Labs.
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
use std::sync::Arc;
use std::time::Duration;

use common_base::base::tokio;
use common_base::base::tokio::sync::RwLock;
use common_base::base::tokio::time::sleep;
use common_base::base::Singleton;
use common_exception::Result;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use tracing::warn;

use super::expiring_map::ExpiringMap;
use super::HttpQueryContext;
use crate::servers::http::v1::query::http_query::HttpQuery;
use crate::servers::http::v1::query::HttpQueryRequest;
use crate::sessions::Session;
use crate::Config;

// TODO(youngsofun): may need refactor later for 2 reasons:
// 1. some can be both configured and overwritten by http query request
// 2. maybe QueryConfig can contain it directly
#[derive(Copy, Clone)]
pub(crate) struct HttpQueryConfig {
    pub(crate) result_timeout_millis: u64,
}

pub struct HttpQueryManager {
    pub(crate) queries: Arc<RwLock<HashMap<String, Arc<HttpQuery>>>>,
    pub(crate) sessions: Mutex<ExpiringMap<String, Arc<Session>>>,
    pub(crate) config: HttpQueryConfig,
}

static HTTP_QUERIES_MANAGER: OnceCell<Singleton<Arc<HttpQueryManager>>> = OnceCell::new();

impl HttpQueryManager {
    pub async fn init(cfg: &Config, v: Singleton<Arc<HttpQueryManager>>) -> Result<()> {
        v.init(Arc::new(HttpQueryManager {
            queries: Arc::new(RwLock::new(HashMap::new())),
            sessions: Mutex::new(ExpiringMap::default()),
            config: HttpQueryConfig {
                result_timeout_millis: cfg.query.http_handler_result_timeout_millis,
            },
        }))?;

        HTTP_QUERIES_MANAGER.set(v).ok();
        Ok(())
    }

    pub fn instance() -> Arc<HttpQueryManager> {
        match HTTP_QUERIES_MANAGER.get() {
            None => panic!("HttpQueryManager is not init"),
            Some(http_queries_manager) => http_queries_manager.get(),
        }
    }

    pub(crate) async fn try_create_query(
        self: &Arc<Self>,
        ctx: &HttpQueryContext,
        request: HttpQueryRequest,
    ) -> Result<Arc<HttpQuery>> {
        let query = HttpQuery::try_create(ctx, request, self.config).await?;
        self.add_query(&query.id, query.clone()).await;
        Ok(query)
    }

    pub(crate) async fn get_query(self: &Arc<Self>, query_id: &str) -> Option<Arc<HttpQuery>> {
        let queries = self.queries.read().await;
        queries.get(query_id).map(|q| q.to_owned())
    }

    async fn add_query(self: &Arc<Self>, query_id: &str, query: Arc<HttpQuery>) {
        let mut queries = self.queries.write().await;
        queries.insert(query_id.to_string(), query.clone());

        let self_clone = self.clone();
        let query_id_clone = query_id.to_string();
        let query_clone = query.clone();
        if query.is_async() {
            tokio::spawn(async move {
                while let Some(t) = query_clone.check_expire().await {
                    sleep(t).await;
                }
                if self_clone.remove_query(&query_id_clone).await.is_none() {
                    warn!("http query {} timeout, but fail to remove", &query_id_clone);
                } else {
                    warn!("http query {} timeout", &query_id_clone);
                    query.detach().await;
                }
            });
        };
    }

    // not remove it until timeout or cancelled by user, even if query execution is aborted
    pub(crate) async fn remove_query(self: &Arc<Self>, query_id: &str) -> Option<Arc<HttpQuery>> {
        let mut queries = self.queries.write().await;
        let q = queries.remove(query_id);
        if let Some(q) = queries.remove(query_id) {
            if q.is_async() {
                q.update_expire_time().await;
            }
        }
        q
    }

    pub(crate) async fn get_session(self: &Arc<Self>, session_id: &str) -> Option<Arc<Session>> {
        let sessions = self.sessions.lock();
        sessions.get(session_id)
    }

    pub(crate) async fn add_session(self: &Arc<Self>, session: Arc<Session>, timeout: Duration) {
        let mut sessions = self.sessions.lock();
        sessions.insert(session.get_id(), session, Some(timeout));
    }

    pub(crate) fn kill_session(self: &Arc<Self>, session_id: &str) {
        let mut sessions = self.sessions.lock();
        sessions.remove(session_id);
    }
}
