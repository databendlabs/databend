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

use std::collections::HashMap;

use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_settings::FlightKeepAliveParams;
use http::StatusCode;
use poem::IntoResponse;
use poem::web::Json;
use poem::web::Path;

use crate::clusters::ClusterDiscovery;
use crate::clusters::ClusterHelper;
use crate::clusters::FlightParams;
use crate::servers::flight::v1::actions::GET_RUNNING_QUERY_DUMP;

#[poem::handler]
#[async_backtrace::framed]
pub async fn running_query_dump(Path(query_id): Path<String>) -> poem::Result<impl IntoResponse> {
    #[derive(serde::Serialize)]
    struct QueryRunningGraphDump {
        query_id: String,
        graph_dump: HashMap<String, String>,
    }

    let graph_dump = match get_running_query_dump(&query_id).await {
        Ok(graph_dump) => graph_dump,
        Err(cause) => {
            return Err(poem::Error::from_string(
                format!("Failed to fetch executor dump. cause: {cause}"),
                StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }
    };

    Ok(Json(QueryRunningGraphDump {
        graph_dump,
        query_id: query_id.clone(),
    }))
}

async fn get_running_query_dump(query_id: &str) -> Result<HashMap<String, String>> {
    let config = GlobalConfig::instance();
    let cluster = ClusterDiscovery::instance().discover(&config).await?;

    let mut message = HashMap::with_capacity(cluster.nodes.len());

    for node_info in &cluster.nodes {
        message.insert(node_info.id.clone(), query_id.to_owned());
    }

    let flight_params = FlightParams {
        timeout: 60,
        retry_times: 3,
        retry_interval: 3,
        keep_alive: FlightKeepAliveParams::default(),
    };
    cluster
        .do_action::<_, String>(GET_RUNNING_QUERY_DUMP, message, flight_params)
        .await
}
