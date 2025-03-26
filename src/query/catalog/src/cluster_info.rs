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
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use databend_common_base::base::tokio::time::sleep;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_grpc::ConnectionFactory;
use databend_common_meta_types::NodeInfo;
use serde::Deserialize;
use serde::Serialize;
use tonic::metadata::AsciiMetadataKey;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::Channel;
use tonic::Request;
use tonic::Response;
use tonic::Streaming;

#[derive(Clone, Copy, Debug)]
pub struct FlightParams {
    pub timeout: u64,
    pub retry_times: u64,
    pub retry_interval: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Cluster {
    pub unassign: bool,
    pub local_id: String,

    pub nodes: Vec<Arc<NodeInfo>>,
}

impl Cluster {
    pub fn create(nodes: Vec<Arc<NodeInfo>>, local_id: String) -> Arc<Cluster> {
        let unassign = nodes.iter().all(|node| !node.assigned_warehouse());
        Arc::new(Cluster {
            unassign,
            local_id,
            nodes,
        })
    }

    pub fn empty() -> Arc<Cluster> {
        Arc::new(Cluster {
            unassign: false,
            local_id: String::from(""),
            nodes: Vec::new(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.len() <= 1
    }

    pub fn is_local(&self, node: &NodeInfo) -> bool {
        node.id == self.local_id
    }

    pub fn local_id(&self) -> String {
        self.local_id.clone()
    }

    pub fn ordered_index(&self) -> usize {
        let mut nodes = self.get_nodes();
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        nodes
            .iter()
            .position(|x| x.id == self.local_id)
            .unwrap_or(0)
    }

    pub fn index_of_nodeid(&self, node_id: &str) -> Option<usize> {
        let mut nodes = self.get_nodes();
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        nodes.iter().position(|x| x.id == node_id)
    }

    pub fn get_nodes(&self) -> Vec<Arc<NodeInfo>> {
        self.nodes.to_vec()
    }

    pub async fn do_action<T: Serialize + Send + Clone, Res: for<'de> Deserialize<'de> + Send>(
        &self,
        path: &str,
        message: HashMap<String, T>,
        flight_params: FlightParams,
    ) -> databend_common_exception::Result<HashMap<String, Res>> {
        fn get_node<'a>(
            nodes: &'a [Arc<NodeInfo>],
            id: &str,
        ) -> databend_common_exception::Result<&'a Arc<NodeInfo>> {
            for node in nodes {
                if node.id == id {
                    return Ok(node);
                }
            }

            Err(ErrorCode::NotFoundClusterNode(format!(
                "Not found node {} in cluster",
                id
            )))
        }

        let mut response = HashMap::with_capacity(message.len());
        for (id, message) in message {
            let node = get_node(&self.nodes, &id)?;

            let do_action_with_retry = {
                let config = GlobalConfig::instance();
                let flight_address = node.flight_address.clone();
                let node_secret = node.secret.clone();

                async move {
                    let mut attempt = 0;

                    loop {
                        let mut conn = create_client(&config, &flight_address).await?;
                        let request = new_request(
                            path,
                            node_secret.clone(),
                            flight_params.timeout,
                            message.clone(),
                        )?;
                        match parse_response(path, conn.do_action(request).await).await {
                            Ok(v) => {
                                return Ok(v);
                            }
                            Err(e)
                                if e.code() == ErrorCode::CANNOT_CONNECT_NODE
                                    && attempt < flight_params.retry_times =>
                            {
                                // only retry when error is network problem
                                log::info!("retry do_action, attempt: {}", attempt);
                                attempt += 1;
                                sleep(Duration::from_secs(flight_params.retry_interval)).await;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
            };

            response.insert(id, do_action_with_retry.await?);
        }

        Ok(response)
    }
}

fn new_request<T: Serialize>(
    path: &str,
    secret: String,
    timeout: u64,
    message: T,
) -> Result<Request<Action>> {
    let mut body = Vec::with_capacity(512);
    let mut serializer = serde_json::Serializer::new(&mut body);
    let serializer = serde_stacker::Serializer::new(&mut serializer);
    message.serialize(serializer).map_err(|cause| {
        ErrorCode::BadArguments(format!(
            "Request payload serialize error while in {:?}, cause: {}",
            path, cause
        ))
    })?;

    drop(message);
    let mut request = databend_common_tracing::inject_span_to_tonic_request(Request::new(Action {
        body: body.into(),
        r#type: path.to_string(),
    }));

    request.set_timeout(Duration::from_secs(timeout));
    request.metadata_mut().insert(
        AsciiMetadataKey::from_str("secret").unwrap(),
        AsciiMetadataValue::from_str(&secret).unwrap(),
    );

    Ok(request)
}

#[async_backtrace::framed]
pub async fn create_client(
    config: &InnerConfig,
    address: &str,
) -> Result<FlightServiceClient<Channel>> {
    let timeout = if config.query.rpc_client_timeout_secs > 0 {
        Some(Duration::from_secs(config.query.rpc_client_timeout_secs))
    } else {
        None
    };

    let rpc_tls_config = if config.tls_query_cli_enabled() {
        Some(config.query.to_rpc_client_tls_config())
    } else {
        None
    };

    Ok(FlightServiceClient::new(
        ConnectionFactory::create_rpc_channel(address.to_owned(), timeout, rpc_tls_config).await?,
    ))
}

async fn parse_response<Res: for<'a> Deserialize<'a>>(
    path: &str,
    response: tonic::Result<Response<Streaming<arrow_flight::Result>>>,
) -> Result<Res> {
    match response?.into_inner().message().await? {
        Some(response) => {
            let mut deserializer = serde_json::Deserializer::from_slice(&response.body);
            deserializer.disable_recursion_limit();
            let deserializer = serde_stacker::Deserializer::new(&mut deserializer);

            Res::deserialize(deserializer).map_err(|cause| {
                ErrorCode::BadBytes(format!(
                    "Response payload deserialize error while in {:?}, cause: {}",
                    path, cause
                ))
            })
        }
        None => Err(ErrorCode::EmptyDataFromServer(format!(
            "Can not receive data from flight server, action: {:?}",
            path
        ))),
    }
}
