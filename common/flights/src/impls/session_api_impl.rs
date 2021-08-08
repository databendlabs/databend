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
pub struct KillSessionReq {
    pub session_id: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct KillSessionReply;

#[async_trait::async_trait]
impl SessionApi for StoreClient {
    #[tracing::instrument(level = "debug", skip(self))]
    async fn kill(&mut self, session_id: String) -> Result<()> {
        self.do_action(KillSessionReq { session_id }).await
    }
}

impl RequestFor for KillSessionReq {
    type Reply = ();
}

impl From<KillSessionReq> for StoreDoAction {
    fn from(act: KillSessionReq) -> Self {
        StoreDoAction::KillSession(act)
    }
}
