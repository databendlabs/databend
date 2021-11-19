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

use common_base::tokio::sync::RwLock;
use common_exception::Result;

use crate::configs::Config;
use crate::servers::http::v1::query::http_query::HttpQueryRef;

pub struct HttpQueryManager {
    pub(crate) queries: Arc<RwLock<HashMap<String, HttpQueryRef>>>,
}

pub type HttpQueryManagerRef = Arc<HttpQueryManager>;

impl HttpQueryManager {
    pub async fn create_global(_cfg: Config) -> Result<HttpQueryManagerRef> {
        Ok(Arc::new(HttpQueryManager {
            queries: Arc::new(RwLock::new(HashMap::new())),
        }))
    }

    pub(crate) fn next_query_id(self: &Arc<Self>) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    pub(crate) async fn get_query_by_id(self: &Arc<Self>, query_id: &str) -> Option<HttpQueryRef> {
        let queries = self.queries.read().await;
        queries.get(query_id).map(|q| q.to_owned())
    }

    pub(crate) async fn remove_query_by_id(self: &Arc<Self>, query_id: &str) {
        let mut queries = self.queries.write().await;
        queries.remove(query_id);
    }
}
