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

use databend_common_meta_raft_store::StateMachineFeature;
use databend_common_meta_sled_store::openraft::async_runtime::watch::WatchReceiver;
use http::StatusCode;
use log::info;
use log::warn;
use poem::web::Data;
use poem::web::Json;
use poem::web::Query;
use poem::IntoResponse;

use crate::meta_service::MetaNode;

/// Query parameters for setting a feature.
#[derive(Debug, serde::Deserialize)]
pub struct SetFeatureQuery {
    pub(crate) feature: StateMachineFeature,
    pub(crate) enable: bool,
}

/// Response for set/list feature, contains all available features and enabled features.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct FeatureResponse {
    /// All available features
    pub features: Vec<StateMachineFeature>,
    /// All enabled features
    pub enabled: Vec<String>,
}

/// List all available state machine features
#[poem::handler]
pub async fn list(meta_node: Data<&Arc<MetaNode>>) -> poem::Result<impl IntoResponse> {
    let metrics = meta_node.raft.metrics().borrow_watched().clone();

    let id = metrics.id;
    info!("id={} Received list_feature request", id);

    let response = features_state(meta_node.0);

    Ok(Json(response))
}

/// Set (enable/disable) a state machine feature
///
/// Only the leader can set features. If this node is not a leader, 404 NOT_FOUND will be returned.
#[poem::handler]
pub async fn set(
    meta_node: Data<&Arc<MetaNode>>,
    query: Option<Query<SetFeatureQuery>>,
) -> poem::Result<impl IntoResponse> {
    let metrics = meta_node.raft.metrics().borrow_watched().clone();

    let id = metrics.id;
    let current_leader = metrics.current_leader;

    info!(
        "id={} Received set_feature request: {:?}, \
        current_leader={:?}",
        id, &query, current_leader
    );

    let Some(query) = query else {
        return Err(poem::Error::from_string(
            "'feature' and 'enable' query parameters are required".to_string(),
            StatusCode::BAD_REQUEST,
        ));
    };

    if current_leader != Some(id) {
        warn!(
            "This node is not leader, can not set feature; id={}; current_leader={:?}",
            id, current_leader
        );

        let mes = format!(
            "This node is not leader, can not set feature;\n\
                 id={}\n\
                 current_leader={:?}",
            id, current_leader
        );

        return Err(poem::Error::from_string(mes, StatusCode::NOT_FOUND));
    }

    info!(
        "id={} Begin to set feature: {} to {}",
        id, query.feature, query.enable
    );

    meta_node
        .set_feature(query.feature, query.enable)
        .await
        .map_err(|e| poem::Error::from_string(e.to_string(), StatusCode::INTERNAL_SERVER_ERROR))?;

    let response = features_state(meta_node.0);

    Ok(Json(response))
}

pub fn features_state(meta_node: &Arc<MetaNode>) -> FeatureResponse {
    let enabled = {
        let sm = meta_node.raft_store.get_sm_v003();
        let x = sm.sys_data().features().clone();
        x.into_iter().collect()
    };

    FeatureResponse {
        features: StateMachineFeature::all(),
        enabled,
    }
}
