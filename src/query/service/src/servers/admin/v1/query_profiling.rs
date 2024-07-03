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
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::runtime::profile::get_statistics_desc;
use databend_common_base::runtime::profile::ProfileDesc;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_pipeline_core::PlanProfile;
use http::StatusCode;
use poem::web::Json;
use poem::web::Path;
use poem::IntoResponse;

use crate::clusters::ClusterHelper;
use crate::servers::flight::v1::actions::GET_PROFILE;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

#[poem::handler]
#[async_backtrace::framed]
pub async fn query_profiling_handler(
    Path(query_id): Path<String>,
) -> poem::Result<impl IntoResponse> {
    let session_manager = SessionManager::instance();
    #[derive(serde::Serialize)]
    struct QueryProfiles {
        query_id: String,
        profiles: Vec<PlanProfile>,
        statistics_desc: Arc<BTreeMap<ProfileStatisticsName, ProfileDesc>>,
    }
    let res = match session_manager.get_session_by_id(&query_id) {
        Some(session) => {
            // can get profile from current node
            session.get_profile().unwrap_or_default()
        }
        None => {
            // need get profile from clusters
            get_cluster_profile(&session_manager, &query_id)
                .await
                .map_err(|cause| {
                    poem::Error::from_string(
                        format!("Failed to fetch cluster node profile. cause: {cause}"),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    )
                })?
        }
    };

    Ok(Json(QueryProfiles {
        query_id: query_id.clone(),
        profiles: res,
        statistics_desc: get_statistics_desc(),
    }))
}

async fn get_cluster_profile(
    session_manager: &SessionManager,
    query_id: &str,
) -> Result<Vec<PlanProfile>, ErrorCode> {
    let session = session_manager
        .create_session(SessionType::HTTPAPI("QueryProfiling".to_string()))
        .await?;

    let ctx = Arc::new(session).create_query_context().await?;
    let cluster = ctx.get_cluster();
    let mut message = HashMap::with_capacity(cluster.nodes.len());

    for node_info in &cluster.nodes {
        if node_info.id != cluster.local_id {
            message.insert(node_info.id.clone(), query_id.to_owned());
        }
    }

    let settings = ctx.get_settings();
    let timeout = settings.get_flight_client_timeout()?;
    let res = cluster
        .do_action::<String, Vec<PlanProfile>>(GET_PROFILE, message, timeout)
        .await?;
    Ok(res.into_iter().flat_map(|(_key, value)| value).collect())
}
