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

use databend_common_base::base::BuildInfoRef;
use databend_common_catalog::session_type::SessionType;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_meta_types::NodeInfo;
use http::StatusCode;
use poem::web::IntoResponse;
use poem::web::Json;

use crate::clusters::ClusterHelper;
use crate::sessions::SessionManager;
use crate::sessions::TableContext;

// GET /v1/cluster/list
// list all nodes in current databend-query cluster
// request: None
// cluster_state: the shared in memory state which store all nodes known to current node
// return: return a list of cluster node information
#[poem::handler]
#[async_backtrace::framed]
pub async fn cluster_list_handler() -> poem::Result<impl IntoResponse> {
    let sessions = SessionManager::instance();
    let version = GlobalConfig::version();
    let nodes = list_nodes(&sessions, version).await.map_err(|cause| {
        poem::Error::from_string(
            format!("Failed to fetch cluster nodes list. cause: {cause}"),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    })?;
    Ok(Json(nodes))
}

async fn list_nodes(
    session_manager: &Arc<SessionManager>,
    version: BuildInfoRef,
) -> Result<Vec<Arc<NodeInfo>>> {
    let session = session_manager
        .create_session(SessionType::HTTPAPI("WatchCluster".to_string()))
        .await?;

    let session = session_manager.register_session(session)?;

    let watch_cluster_context = session.create_query_context(version).await?;
    Ok(watch_cluster_context.get_cluster().get_nodes())
}
