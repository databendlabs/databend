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

use crate::meta_node::errors::MetaNodeStopped;
use crate::meta_node::meta_handle::MetaHandle;

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
pub async fn list(meta_handle: Data<&Arc<MetaHandle>>) -> poem::Result<impl IntoResponse> {
    let metrics = meta_handle
        .handle_raft_metrics()
        .await
        .map_err(|e| {
            poem::Error::from_string(
                format!("Failed to get raft metrics: {}", e),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?
        .borrow_watched()
        .clone();

    let id = metrics.id;
    info!("id={} Received list_feature request", id);

    let response = features_state(meta_handle.0)
        .await
        .map_err(|e| poem::Error::from_string(e.to_string(), StatusCode::INTERNAL_SERVER_ERROR))?;

    Ok(Json(response))
}

/// Set (enable/disable) a state machine feature
///
/// Only the leader can set features. If this node is not a leader, 404 NOT_FOUND will be returned.
#[poem::handler]
pub async fn set(
    meta_handle: Data<&Arc<MetaHandle>>,
    query: Option<Query<SetFeatureQuery>>,
) -> poem::Result<impl IntoResponse> {
    let metrics = meta_handle
        .handle_raft_metrics()
        .await
        .map_err(|e| {
            poem::Error::from_string(
                format!("Failed to get raft metrics: {}", e),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?
        .borrow_watched()
        .clone();

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

    let f = query.feature;
    let enable = query.enable;

    meta_handle
        .request(move |mn| {
            let fu = async move { mn.set_feature(f, enable).await };
            Box::pin(fu)
        })
        .await
        .map_err(|e| poem::Error::from_string(e.to_string(), StatusCode::INTERNAL_SERVER_ERROR))?
        .map_err(|e| poem::Error::from_string(e.to_string(), StatusCode::INTERNAL_SERVER_ERROR))?;

    let response = features_state(meta_handle.0).await.map_err(|e| {
        poem::Error::from_string(
            format!("Failed to get feature: {}", e),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    })?;

    Ok(Json(response))
}

pub async fn features_state(
    meta_handle: &Arc<MetaHandle>,
) -> Result<FeatureResponse, MetaNodeStopped> {
    let enabled = {
        let sys_data = meta_handle.handle_get_sys_data().await?;
        let x = sys_data.features().clone();
        x.into_iter().collect()
    };

    Ok(FeatureResponse {
        features: StateMachineFeature::all(),
        enabled,
    })
}
