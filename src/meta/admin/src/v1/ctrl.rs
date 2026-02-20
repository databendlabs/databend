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
use databend_meta_sled_store::openraft::async_runtime::watch::WatchReceiver;
use databend_meta_types::raft_types::NodeId;
use http::StatusCode;
use log::info;
use log::warn;
use poem::IntoResponse;
use poem::Response;
use poem::web::Json;

use crate::HttpService;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct TransferLeaderQuery {
    pub to: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct TransferLeaderResponse {
    pub from: NodeId,
    pub to: NodeId,
    pub voter_ids: Vec<NodeId>,
}

impl<SP: SpawnApi> HttpService<SP> {
    pub async fn trigger_snapshot(meta_handle: Arc<MetaHandle<SP>>) -> poem::Result<Response> {
        meta_handle
            .handle_trigger_snapshot()
            .await
            .map_err(internal_err)?
            .map_err(internal_err)?;
        Ok(Json(()).into_response())
    }

    pub async fn trigger_transfer_leader(
        meta_handle: Arc<MetaHandle<SP>>,
        query: Option<TransferLeaderQuery>,
    ) -> poem::Result<Response> {
        let metrics = meta_handle
            .handle_raft_metrics()
            .await
            .map_err(internal_err)?
            .borrow_watched()
            .clone();

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

        let to = query.and_then(|q| q.to);

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

        meta_handle
            .handle_trigger_transfer_leader(to)
            .await
            .map_err(internal_err)?
            .map_err(internal_err)?;

        Ok(Json(TransferLeaderResponse {
            from: id,
            to,
            voter_ids,
        })
        .into_response())
    }
}

fn internal_err(e: impl std::fmt::Display) -> poem::Error {
    poem::Error::from_string(e.to_string(), StatusCode::INTERNAL_SERVER_ERROR)
}
