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
use std::time::Duration;

use http::StatusCode;
use poem::web::Data;
use poem::web::IntoResponse;
use poem::web::Json;

use crate::meta_service::MetaNode;

/// Let raft leader send snapshot to followers/learners.
///
/// If this node is not a leader this request will be just ignored.
#[poem::handler]
pub async fn trigger_snapshot(meta_node: Data<&Arc<MetaNode>>) -> poem::Result<impl IntoResponse> {
    meta_node
        .raft
        .trigger()
        .snapshot()
        .await
        .map_err(|e| poem::Error::from_string(e.to_string(), StatusCode::INTERNAL_SERVER_ERROR))?;
    Ok(Json(()))
}

#[poem::handler]
pub async fn block_write_snapshot(
    meta_node: Data<&Arc<MetaNode>>,
) -> poem::Result<impl IntoResponse> {
    let mut sm = meta_node.sto.get_state_machine().await;
    sm.blocking_config_mut().write_snapshot = Duration::from_millis(1_000_000);
    Ok(Json(()))
}

#[poem::handler]
pub async fn block_compact_snapshot(
    meta_node: Data<&Arc<MetaNode>>,
) -> poem::Result<impl IntoResponse> {
    let mut sm = meta_node.sto.get_state_machine().await;
    sm.blocking_config_mut().compact_snapshot = Duration::from_millis(1_000_000);
    Ok(Json(()))
}
