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

use common_base::tokio;
use common_base::tokio::sync::RwLock;
use common_base::tokio::time::sleep;
use common_exception::Result;
use common_meta_types::UserInfo;
use common_tracing::tracing;

use crate::configs::Config;
use crate::servers::http::v1::query::http_query::HttpQuery;
use crate::servers::http::v1::query::HttpQueryRequest;
use crate::sessions::SessionManager;

// TODO(youngsofun): may need refactor later for 2 reasons:
// 1. some can be both configured and overwritten by http query request
// 2. maybe QueryConfig can contain it directly
#[derive(Copy, Clone)]
pub(crate) struct HttpQueryConfig {
    pub(crate) result_timeout_millis: u64,
}

pub struct HttpQueryManager {
    pub(crate) queries: Arc<RwLock<HashMap<String, Arc<HttpQuery>>>>,
    pub(crate) config: HttpQueryConfig,
}

impl HttpQueryManager {
    pub async fn create_global(cfg: Config) -> Result<Arc<HttpQueryManager>> {
        Ok(Arc::new(HttpQueryManager {
            queries: Arc::new(RwLock::new(HashMap::new())),
            config: HttpQueryConfig {
                result_timeout_millis: cfg.query.http_handler_result_timeout_millis,
            },
        }))
    }

    pub(crate) fn next_query_id(self: &Arc<Self>) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    pub(crate) async fn try_create_query(
        self: &Arc<Self>,
        id: &str,
        request: HttpQueryRequest,
        session_manager: &Arc<SessionManager>,
        user_info: &UserInfo,
    ) -> Result<Arc<HttpQuery>> {
        let query =
            HttpQuery::try_create(id, request, session_manager, user_info, self.config).await?;
        self.add_query(id, query.clone()).await;
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
                    tracing::warn!("http query {} timeout", &query_id_clone);
                } else {
                    query.kill().await;
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
}
