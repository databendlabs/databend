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

use databend_meta::meta_node::meta_handle::MetaHandle;
use databend_meta_runtime_api::SpawnApi;
use http::StatusCode;
use poem::IntoResponse;
use poem::Response;
use poem::web::Json;

use crate::HttpService;

impl<SP: SpawnApi> HttpService<SP> {
    pub async fn nodes_handler(meta_handle: Arc<MetaHandle<SP>>) -> poem::Result<Response> {
        let nodes = meta_handle.handle_get_nodes().await.map_err(|e| {
            poem::Error::from_string(
                format!("Failed to get nodes: {}", e),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?;
        Ok(Json(nodes).into_response())
    }

    pub async fn status_handler(
        meta_handle: Arc<MetaHandle<SP>>,
        version: &str,
    ) -> poem::Result<Response> {
        let status = meta_handle.handle_get_status(version).await.map_err(|e| {
            poem::Error::from_string(
                format!("Failed to get status: {}", e),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?;
        Ok(Json(status).into_response())
    }
}
