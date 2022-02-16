// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_meta_types::NodeInfo;
use poem::http::StatusCode;
use poem::web::Data;
use poem::web::IntoResponse;
use poem::web::Json;

use crate::sessions::SessionManager;

// GET /v1/cluster/list
// list all nodes in current databend-query cluster
// request: None
// cluster_state: the shared in memory state which store all nodes known to current node
// return: return a list of cluster node information
#[poem::handler]
pub async fn cluster_list_handler(
    sessions: Data<&Arc<SessionManager>>,
) -> poem::Result<impl IntoResponse> {
    let nodes = list_nodes(sessions.0).await.map_err(|cause| {
        poem::Error::from_string(
            format!("Failed to fetch cluster nodes list. cause: {cause}"),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
    })?;
    Ok(Json(nodes))
}

async fn list_nodes(sessions: &Arc<SessionManager>) -> Result<Vec<Arc<NodeInfo>>> {
    let watch_cluster_session = sessions.create_session("WatchCluster")?;
    let watch_cluster_context = watch_cluster_session.create_query_context().await?;
    Ok(watch_cluster_context.get_cluster().get_nodes())
}
