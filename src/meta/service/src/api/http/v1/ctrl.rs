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
use poem::web::Query;

use crate::meta_service::MetaNode;

/// Let raft leader send snapshot to followers/learners.
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

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct TransferLeaderQuery {
    pub(crate) to: Option<u64>,
}

/// Transfer this Leader to another specified node.
///
/// If this node is not a Leader this request will be just ignored.
#[poem::handler]
pub async fn trigger_transfer_leader(
    meta_node: Data<&Arc<MetaNode>>,
    query: Option<Query<TransferLeaderQuery>>,
) -> poem::Result<impl IntoResponse> {
    let Some(query) = query else {
        return Err(poem::Error::from_string(
            "missing query to=<node_id_to_transfer_leader_to>",
            StatusCode::BAD_REQUEST,
        ));
    };

    let Some(to) = query.to else {
        return Err(poem::Error::from_string(
            "missing query to=<node_id_to_transfer_leader_to>",
            StatusCode::BAD_REQUEST,
        ));
    };

    meta_node
        .raft
        .trigger()
        .transfer_leader(to)
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
