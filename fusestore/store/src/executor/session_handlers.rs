// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_flights::session_api_impl::KillSessionReq;

use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;

#[async_trait::async_trait]
impl RequestHandler<KillSessionReq> for ActionHandler {
    async fn handle(&self, _act: KillSessionReq) -> common_exception::Result<()> {
        // business logic to be specified
        Ok(())
    }
}
