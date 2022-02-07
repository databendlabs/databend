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
use common_exception::Result;
use common_tracing::tracing;

use crate::configs::Config;
use crate::servers::http::v1::query::http_query::HttpQuery;

pub struct HttpQueryManager {
    pub(crate) queries: Arc<RwLock<HashMap<String, Arc<HttpQuery>>>>,
}

impl HttpQueryManager {
    pub async fn create_global(_cfg: Config) -> Result<Arc<HttpQueryManager>> {
        Ok(Arc::new(HttpQueryManager {
            queries: Arc::new(RwLock::new(HashMap::new())),
        }))
    }

    pub(crate) fn next_query_id(self: &Arc<Self>) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    pub(crate) async fn get_query(self: &Arc<Self>, query_id: &str) -> Option<Arc<HttpQuery>> {
        let queries = self.queries.read().await;
        queries.get(query_id).map(|q| q.to_owned())
    }

    pub(crate) async fn add_query(self: &Arc<Self>, query_id: &str, query: Arc<HttpQuery>) {
        let mut queries = self.queries.write().await;
        queries.insert(query_id.to_string(), query.clone());

        let self_clone = self.clone();
        let query_id_clone = query_id.to_string();
        let query_clone = query.clone();
        tokio::spawn(async move {
            while !query_clone.check_timeout().await {}
            if self_clone.remove_query(&query_id_clone).await {
                tracing::warn!("http query {} timeout", &query_id_clone);
            }
        });
    }

    // not remove it until timeout or cancelled by user, even if query execution is aborted
    pub(crate) async fn remove_query(self: &Arc<Self>, query_id: &str) -> bool {
        let mut queries = self.queries.write().await;
        queries.remove(query_id).is_none()
    }
}
