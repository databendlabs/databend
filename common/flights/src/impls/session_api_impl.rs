// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

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
