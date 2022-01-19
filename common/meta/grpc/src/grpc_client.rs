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

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use common_arrow::arrow_format::flight::data::BasicAuth;
use common_base::tokio::sync::RwLock;
use common_containers::ItemManager;
use common_containers::Pool;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::SerializedError;
use common_grpc::ConnectionFactory;
use common_grpc::RpcClientTlsConfig;
use common_meta_types::protobuf::meta_client::MetaClient;
use common_meta_types::protobuf::GetReply;
use common_meta_types::protobuf::GetRequest;
use common_meta_types::protobuf::HandshakeRequest;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::protobuf::RaftRequest;
use common_tracing::tracing;
use futures::stream::StreamExt;
use prost::Message;
use serde::de::DeserializeOwned;
use tonic::async_trait;
use tonic::client::GrpcService;
use tonic::codegen::InterceptedService;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::Code;
use tonic::Request;
use tonic::Status;

use crate::grpc_action::MetaGrpcReadReq;
use crate::grpc_action::MetaGrpcWriteReq;
use crate::grpc_action::RequestFor;
use crate::MetaGrpcClientConf;

#[derive(Debug)]
struct MetaChannelManager {
    timeout: Option<Duration>,
    conf: Option<RpcClientTlsConfig>,
}

#[async_trait]
impl ItemManager for MetaChannelManager {
    type Key = String;
    type Item = Channel;
    type Error = ErrorCode;

    async fn build(&self, addr: &Self::Key) -> std::result::Result<Self::Item, Self::Error> {
        ConnectionFactory::create_rpc_channel(addr, self.timeout, self.conf.clone())
    }

    async fn check(&self, mut ch: Self::Item) -> std::result::Result<Self::Item, Self::Error> {
        futures::future::poll_fn(|cx| (&mut ch).poll_ready(cx))
            .await
            .map_err(|e| {
                ErrorCode::CannotConnectNode(format!("Check rpc connect failed, error: {}", e))
            })?;
        Ok(ch)
    }
}

pub struct MetaGrpcClient {
    conn_pool: Pool<MetaChannelManager>,
    addr: String,
    username: String,
    password: String,
    token: Arc<RwLock<Option<Vec<u8>>>>,
}

const AUTH_TOKEN_KEY: &str = "auth-token-bin";

impl MetaGrpcClient {
    pub async fn try_new(conf: &MetaGrpcClientConf) -> Result<MetaGrpcClient> {
        let mgr = MetaChannelManager {
            timeout: Some(Duration::from_secs(conf.client_timeout_in_second)),
            conf: conf.meta_service_config.tls_conf.clone(),
        };
        Ok(Self {
            conn_pool: Pool::new(mgr, Duration::from_millis(50)),
            addr: conf.meta_service_config.address.to_string(),
            username: conf.meta_service_config.username.to_string(),
            password: conf.meta_service_config.password.to_string(),
            token: Arc::new(RwLock::new(None)),
        })
    }

    #[tracing::instrument(level = "debug", skip(password))]
    pub async fn try_create(
        addr: &str,
        username: &str,
        password: &str,
        timeout: Option<Duration>,
        conf: Option<RpcClientTlsConfig>,
    ) -> Result<Self> {
        let mgr = MetaChannelManager { timeout, conf };

        Ok(Self {
            conn_pool: Pool::new(mgr, Duration::from_millis(50)),
            addr: addr.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            token: Arc::new(RwLock::new(None)),
        })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn make_client(
        &self,
    ) -> Result<MetaClient<InterceptedService<Channel, AuthInterceptor>>> {
        let channel = self.conn_pool.get(&self.addr).await?;
        tracing::debug!("connecting to {}, channel: {:?}", &self.addr, channel);

        let mut client = MetaClient::new(channel.clone());
        let mut t = self.token.write().await;
        let token = match t.clone() {
            Some(t) => t,
            None => {
                let new_token =
                    MetaGrpcClient::handshake(&mut client, &self.username, &self.password).await?;
                *t = Some(new_token.clone());
                new_token
            }
        };

        let client = { MetaClient::with_interceptor(channel, AuthInterceptor { token }) };

        Ok(client)
    }

    /// Handshake.
    #[tracing::instrument(level = "debug", skip(client, password))]
    async fn handshake(
        client: &mut MetaClient<Channel>,
        username: &str,
        password: &str,
    ) -> Result<Vec<u8>> {
        let auth = BasicAuth {
            username: username.to_string(),
            password: password.to_string(),
        };
        let mut payload = vec![];
        auth.encode(&mut payload)?;

        let req = Request::new(futures::stream::once(async {
            HandshakeRequest {
                payload,
                ..HandshakeRequest::default()
            }
        }));

        let rx = client.handshake(req).await?;
        let mut rx = rx.into_inner();

        let resp = rx.next().await.expect("Must respond from handshake")?;
        let token = resp.payload;
        Ok(token)
    }

    #[tracing::instrument(level = "debug", skip(self, v))]
    pub(crate) async fn do_write<T, R>(&self, v: T) -> Result<R>
    where
        T: RequestFor<Reply = R> + Into<MetaGrpcWriteReq>,
        R: DeserializeOwned,
    {
        let act: MetaGrpcWriteReq = v.into();
        let req: Request<RaftRequest> = (&act.clone()).try_into()?;
        let req = common_tracing::inject_span_to_tonic_request(req);

        let mut client = self.make_client().await?;
        let result = client.write_msg(req).await;
        let result: std::result::Result<RaftReply, Status> = match result {
            Ok(r) => Ok(r.into_inner()),
            Err(s) => {
                if status_is_retryable(&s) {
                    {
                        let mut token = self.token.write().await;
                        *token = None;
                    }
                    let mut client = self.make_client().await?;
                    let req: Request<RaftRequest> = (&act).try_into()?;
                    let req = common_tracing::inject_span_to_tonic_request(req);
                    Ok(client.write_msg(req).await?.into_inner())
                } else {
                    Err(s)
                }
            }
        };

        let result = result?;

        if result.error.is_empty() {
            let v = serde_json::from_str::<R>(&result.data)?;
            Ok(v)
        } else {
            let e: SerializedError = serde_json::from_str(&result.error)?;
            Err(e.into())
        }
    }

    #[tracing::instrument(level = "debug", skip(self, v))]
    pub(crate) async fn do_get<T, R>(&self, v: T) -> Result<R>
    where
        T: RequestFor<Reply = R>,
        T: Into<MetaGrpcReadReq>,
        R: DeserializeOwned,
    {
        let act: MetaGrpcReadReq = v.into();
        let req: Request<GetRequest> = (&act.clone()).try_into()?;
        let req = common_tracing::inject_span_to_tonic_request(req);

        let mut client = self.make_client().await?;
        let result = client.read_msg(req).await;
        let result: std::result::Result<GetReply, Status> = match result {
            Ok(r) => Ok(r.into_inner()),
            Err(s) => {
                if status_is_retryable(&s) {
                    {
                        let mut token = self.token.write().await;
                        *token = None;
                    }
                    let mut client = self.make_client().await?;
                    let req: Request<GetRequest> = (&act).try_into()?;
                    let req = common_tracing::inject_span_to_tonic_request(req);
                    Ok(client.read_msg(req).await?.into_inner())
                } else {
                    Err(s)
                }
            }
        };
        let result = result?;
        if result.ok {
            let v = serde_json::from_str::<R>(&result.value)?;
            Ok(v)
        } else {
            Err(ErrorCode::EmptyData(format!(
                "Can not receive data from grpc server, action: {:?}",
                act
            )))
        }
    }
}

fn status_is_retryable(status: &Status) -> bool {
    matches!(status.code(), Code::Unauthenticated | Code::Internal)
}

#[derive(Clone)]
pub struct AuthInterceptor {
    pub token: Vec<u8>,
}

impl Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        let metadata = req.metadata_mut();
        metadata.insert_bin(AUTH_TOKEN_KEY, MetadataValue::from_bytes(&self.token));
        Ok(req)
    }
}
