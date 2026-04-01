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

pub mod catalog;
mod discovery;
mod http_query_handlers;
mod query;
pub mod roles;
mod session;
mod stage;
#[cfg(feature = "storage-stage")]
mod streaming_load;
pub mod users;
mod verify;

#[cfg(not(feature = "storage-stage"))]
use databend_common_base::base::ProgressValues;
pub use discovery::discovery_nodes;
#[cfg(not(feature = "storage-stage"))]
use http::StatusCode;
pub use http_query_handlers::QueryResponse;
pub use http_query_handlers::QueryResponseField;
pub use http_query_handlers::QueryStats;
pub use http_query_handlers::make_final_uri;
pub use http_query_handlers::make_page_uri;
pub use http_query_handlers::make_state_uri;
pub use http_query_handlers::query_route;
#[cfg(not(feature = "storage-stage"))]
use poem::Request;
#[cfg(not(feature = "storage-stage"))]
use poem::Response;
#[cfg(not(feature = "storage-stage"))]
use poem::web::Multipart;
pub use query::ExecuteStateKind;
pub use query::ExpiringMap;
pub use query::ExpiringState;
pub use query::HttpQueryContext;
pub use query::HttpQueryManager;
pub use query::HttpSessionConf;
pub use query::HttpSessionStateInternal;
pub use query::blocks_serializer::BlocksCollector;
pub use query::blocks_serializer::BlocksSerializer;
pub use roles::list_roles_handler;
#[cfg(not(feature = "storage-stage"))]
use serde::Deserialize;
#[cfg(not(feature = "storage-stage"))]
use serde::Serialize;
pub use session::ClientSessionManager;
pub(crate) use session::SessionClaim;
pub use session::login_handler::LoginResponse;
pub use session::login_handler::login_handler;
pub use session::logout_handler::logout_handler;
pub use session::refresh_handler::RefreshResponse;
pub use session::refresh_handler::refresh_handler;
pub(crate) use session::unix_ts;
pub use stage::UploadToStageResponse;
pub use stage::upload_to_stage;
#[cfg(feature = "storage-stage")]
pub use streaming_load::LoadResponse;
#[cfg(feature = "storage-stage")]
pub use streaming_load::streaming_load_handler;
pub use users::create_user_handler;
pub use users::list_users_handler;
pub use verify::verify_handler;

pub use crate::servers::http::clickhouse_handler::clickhouse_router;
pub use crate::servers::http::error::QueryError;

#[cfg(not(feature = "storage-stage"))]
#[derive(Serialize, Deserialize, Debug)]
pub struct LoadResponse {
    pub id: String,
    pub stats: ProgressValues,
}

#[cfg(not(feature = "storage-stage"))]
#[poem::handler]
pub async fn streaming_load_handler(
    ctx: &HttpQueryContext,
    req: &Request,
    multipart: Multipart,
) -> Response {
    let _ = (ctx, req, multipart);
    poem::Error::from_string(
        "Streaming load support is disabled, rebuild with cargo feature 'storage-stage'",
        StatusCode::NOT_IMPLEMENTED,
    )
    .into_response()
}
