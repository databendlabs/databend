// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;
pub use common_store_api::SessionApi;
use common_tracing::tracing;

use crate::RequestFor;
use crate::StoreClient;
use crate::StoreDoAction;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct KillQueryReq {
    pub query_id: String,
}

#[async_trait::async_trait]
impl SessionApi for StoreClient {
    #[tracing::instrument(level = "debug", skip(self))]
    async fn kill_query(&mut self, query_id: String) -> Result<()> {
        self.do_action(KillQueryReq { query_id }).await
    }
}

impl RequestFor for KillQueryReq {
    type Reply = ();
}

impl From<KillQueryReq> for StoreDoAction {
    fn from(act: KillQueryReq) -> Self {
        StoreDoAction::KillQuery(act)
    }
}
