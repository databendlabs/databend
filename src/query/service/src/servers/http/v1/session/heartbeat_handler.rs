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

use poem::IntoResponse;

use crate::servers::http::error::HttpErrorCode;
use crate::servers::http::v1::session::client_session_manager::ClientSessionManager;
use crate::servers::http::v1::HttpQueryContext;

#[poem::handler]
#[async_backtrace::framed]
pub async fn heartbeat_handler(ctx: &HttpQueryContext) -> poem::error::Result<impl IntoResponse> {
    if let Some(id) = &ctx.client_session_id {
        let mgr = ClientSessionManager::instance();
        mgr.refresh_in_memory_states(&id, &ctx.user_name);

        let tenant = ctx.session.get_current_tenant();
        mgr.refresh_session_handle(tenant, ctx.user_name.clone(), &id)
            .await
            .map_err(HttpErrorCode::server_error)?;
    }
    Ok(())
}
