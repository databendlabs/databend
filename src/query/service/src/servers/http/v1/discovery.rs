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

use databend_common_config::GlobalConfig;
use poem::error::InternalServerError;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::Request;

use crate::clusters::ClusterDiscovery;
use crate::clusters::ClusterHelper;
use crate::servers::http::v1::HttpQueryContext;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct DiscoveryNode {
    pub address: String,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn discovery_nodes(
    _: &HttpQueryContext,
    _req: &Request,
) -> PoemResult<Json<Vec<DiscoveryNode>>> {
    let config = GlobalConfig::instance();
    let cluster = ClusterDiscovery::instance()
        .discover(&config)
        .await
        .map_err(InternalServerError)?;

    let nodes = cluster.get_nodes();
    let mut discovery_nodes = Vec::with_capacity(nodes.len());

    for node in nodes {
        discovery_nodes.push(DiscoveryNode {
            address: node.discovery_address.clone(),
        });
    }

    Ok(Json(discovery_nodes))
}
