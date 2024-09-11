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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_license::display_jwt_claims::DisplayJWTClaimsExt;
use databend_common_meta_sled_store::openraft::async_runtime::watch::WatchReceiver;
use databend_common_meta_types::NodeId;
use http::StatusCode;
use log::info;
use log::warn;
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

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct TransferLeaderResponse {
    pub from: NodeId,
    pub to: NodeId,
    pub voter_ids: Vec<NodeId>,
}

/// Transfer this Leader to another specified node, or next node in the cluster if not specified.
///
/// If this node is not a Leader, 404 NOT_FOUND will be returned.
/// Note that a 200 OK response does not mean the transfer is successful, it only means the request is accepted.
#[poem::handler]
pub async fn trigger_transfer_leader(
    meta_node: Data<&Arc<MetaNode>>,
    query: Option<Query<TransferLeaderQuery>>,
) -> poem::Result<impl IntoResponse> {
    let metrics = meta_node.raft.metrics().borrow_watched().clone();

    let id = metrics.id;
    let current_leader = metrics.current_leader;
    let voter_ids = metrics
        .membership_config
        .membership()
        .voter_ids()
        .collect::<Vec<_>>();

    info!(
        "id={} Received trigger_transfer_leader request: {:?}, \
        this node: current_leader={:?} voter_ids={:?}",
        id, &query, current_leader, voter_ids
    );

    let to = {
        if let Some(query) = query {
            query.to
        } else {
            None
        }
    };

    let to = if let Some(to) = to {
        to
    } else {
        // If `to` node is not specified, find the next node id in the voter list.

        // There is still chance this Leader is not a voter,
        // e.g., when Leader commit a membership without it.
        let index = voter_ids.iter().position(|&x| x == id).unwrap_or_default();
        voter_ids[(index + 1) % voter_ids.len()]
    };

    if current_leader != Some(id) {
        warn!(
            "id={} This node is not leader, can not transfer leadership; Current leader is: {:?} voter_ids={:?}",
            id, current_leader, voter_ids,
        );

        return Err(poem::Error::from_string(
            format!(
                "This node is not leader, can not transfer leadership;\n\
                 id={}\n\
                 current_leader={:?}\n\
                 voter_ids={:?}",
                id, current_leader, voter_ids,
            ),
            StatusCode::NOT_FOUND,
        ));
    }

    info!("id={} Begin to transfer leadership to node: {}", id, to);

    meta_node
        .raft
        .trigger()
        .transfer_leader(to)
        .await
        .map_err(|e| poem::Error::from_string(e.to_string(), StatusCode::INTERNAL_SERVER_ERROR))?;

    Ok(Json(TransferLeaderResponse {
        from: id,
        to,
        voter_ids,
    }))
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct UpdateLicenseQuery {
    pub(crate) license: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct UpdateLicenseResponse {
    pub from: NodeId,
    pub to: NodeId,
    pub voter_ids: Vec<NodeId>,
}

/// Update the `databend_enterprise_license` of this meta node.
#[poem::handler]
pub async fn update_license(
    meta_node: Data<&Arc<MetaNode>>,
    query: Option<Query<UpdateLicenseQuery>>,
) -> poem::Result<impl IntoResponse> {
    let Some(query) = query else {
        return Err(poem::Error::from_string(
            "Invalid license",
            StatusCode::BAD_REQUEST,
        ));
    };

    let metrics = meta_node.raft.metrics().borrow_watched().clone();
    let id = metrics.id;

    let saved = meta_node
        .ee_gate
        .parse_jwt_token(query.license.as_str())
        .map_err(|e| {
            poem::Error::from_string(format!("Invalid license: {}", e), StatusCode::BAD_REQUEST)
        })?;

    meta_node
        .ee_gate
        .update_license(query.license.clone())
        .map_err(|e| poem::Error::from_string(e.to_string(), StatusCode::BAD_REQUEST))?;

    let claim_str = saved.display_jwt_claims().to_string();
    info!("id={} Updated license: {}", id, claim_str);

    let mut resp = BTreeMap::new();
    resp.insert("Success", claim_str);

    Ok(Json(resp))
}
