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

use databend_meta::meta_node::errors::MetaNodeStopped;
use databend_meta::meta_node::meta_handle::MetaHandle;
use databend_meta_raft_store::StateMachineFeature;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_sled_store::openraft::async_runtime::watch::WatchReceiver;
use http::StatusCode;
use log::info;
use log::warn;
use poem::IntoResponse;
use poem::Response;
use poem::web::Json;

use crate::HttpService;

/// Query parameters for setting a feature.
#[derive(Debug, serde::Deserialize)]
pub struct SetFeatureQuery {
    pub feature: StateMachineFeature,
    pub enable: bool,
}

/// Response for set/list feature, contains all available features and enabled features.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct FeatureResponse {
    /// All available features
    pub features: Vec<StateMachineFeature>,
    /// All enabled features
    pub enabled: Vec<String>,
}

impl<SP: SpawnApi> HttpService<SP> {
    pub async fn features_list(meta_handle: Arc<MetaHandle<SP>>) -> poem::Result<Response> {
        let metrics = meta_handle
            .handle_raft_metrics()
            .await
            .map_err(internal_err)?
            .borrow_watched()
            .clone();

        let id = metrics.id;
        info!("id={id} Received list_feature request");

        let response = Self::features_state(&meta_handle)
            .await
            .map_err(internal_err)?;
        Ok(Json(response).into_response())
    }

    pub async fn features_set(
        meta_handle: Arc<MetaHandle<SP>>,
        query: Option<SetFeatureQuery>,
    ) -> poem::Result<Response> {
        let metrics = meta_handle
            .handle_raft_metrics()
            .await
            .map_err(internal_err)?
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
                "'feature' and 'enable' query parameters are required",
                StatusCode::BAD_REQUEST,
            ));
        };

        if current_leader != Some(id) {
            let msg = format!(
                "This node is not leader, cannot set feature; id={id}; current_leader={current_leader:?}"
            );
            warn!("{}", msg);
            return Err(poem::Error::from_string(msg, StatusCode::NOT_FOUND));
        }

        info!(
            "id={} Begin to set feature: {} to {}",
            id, query.feature, query.enable
        );

        let f = query.feature;
        let enable = query.enable;

        meta_handle
            .request(move |mn| Box::pin(async move { mn.set_feature(f, enable).await }))
            .await
            .map_err(internal_err)?
            .map_err(internal_err)?;

        let response = Self::features_state(&meta_handle)
            .await
            .map_err(internal_err)?;
        Ok(Json(response).into_response())
    }

    async fn features_state(
        meta_handle: &Arc<MetaHandle<SP>>,
    ) -> Result<FeatureResponse, MetaNodeStopped> {
        let sys_data = meta_handle.handle_get_sys_data().await?;
        Ok(FeatureResponse {
            features: StateMachineFeature::all(),
            enabled: sys_data.features().iter().cloned().collect(),
        })
    }
}

fn internal_err(e: impl std::fmt::Display) -> poem::Error {
    poem::Error::from_string(e.to_string(), StatusCode::INTERNAL_SERVER_ERROR)
}
