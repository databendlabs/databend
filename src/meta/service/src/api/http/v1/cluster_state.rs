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

use std::sync::Arc;

use http::StatusCode;
use poem::web::Data;
use poem::web::IntoResponse;
use poem::web::Json;

use crate::meta_node::meta_handle::MetaHandle;

/// list all nodes in current databend-meta cluster.
///
/// request: None
/// return: return a list of cluster node information
#[poem::handler]
pub async fn nodes_handler(meta_handle: Data<&Arc<MetaHandle>>) -> poem::Result<impl IntoResponse> {
    let nodes = meta_handle.handle_get_nodes().await.map_err(|e| {
        poem::Error::from_string(
            format!("Failed to get status: {}", e),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    })?;
    Ok(Json(nodes))
}

#[poem::handler]
pub async fn status_handler(
    meta_handle: Data<&Arc<MetaHandle>>,
) -> poem::Result<impl IntoResponse> {
    let status = meta_handle.handle_get_status().await.map_err(|e| {
        poem::Error::from_string(
            format!("Failed to get status: {}", e),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    })?;

    Ok(Json(status))
}
