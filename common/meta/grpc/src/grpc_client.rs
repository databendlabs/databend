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

use std::convert::Infallible;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use common_arrow::arrow_format::flight::data::BasicAuth;
use common_base::base::tokio;
use common_base::base::tokio::select;
use common_base::base::tokio::sync::oneshot;
use common_base::base::tokio::sync::oneshot::Receiver;
use common_base::base::tokio::sync::oneshot::Sender;
use common_base::base::tokio::sync::RwLock;
use common_base::base::tokio::time;
use common_base::containers::ItemManager;
use common_base::containers::Pool;
use common_exception::Result;
use common_grpc::ConnectionFactory;
use common_grpc::GrpcConnectionError;
use common_grpc::RpcClientTlsConfig;
use common_meta_types::anyerror::AnyError;
use common_meta_types::protobuf::meta_service_client::MetaServiceClient;
use common_meta_types::protobuf::HandshakeRequest;
use common_meta_types::protobuf::MemberListRequest;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::protobuf::RaftRequest;
use common_meta_types::ConnectionError;
use common_meta_types::MetaError;
use common_meta_types::MetaNetworkError;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
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
use tonic::transport::Endpoint;
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

impl MetaChannelManager {
    fn build_endpoint(&self, addr: &String) -> std::result::Result<Endpoint, MetaError> {
        let ch = ConnectionFactory::create_rpc_endpoint(addr, self.timeout, self.conf.clone())
            .map_err(|e| match e {
                GrpcConnectionError::InvalidUri { .. } => MetaNetworkError::BadAddressFormat(
                    AnyError::new(&e).add_context(|| "while creating rpc channel"),
                ),
                GrpcConnectionError::TLSConfigError { .. } => MetaNetworkError::TLSConfigError(
                    AnyError::new(&e).add_context(|| "while creating rpc channel"),
                ),
                GrpcConnectionError::CannotConnect { .. } => MetaNetworkError::ConnectionError(
                    ConnectionError::new(e, "while creating rpc channel"),
                ),
            })?;
        Ok(ch)
    }
}

#[async_trait]
impl ItemManager for MetaChannelManager {
    type Key = Vec<String>;
    type Item = Channel;
    type Error = MetaError;

    async fn build(&self, endpoints: &Self::Key) -> std::result::Result<Self::Item, Self::Error> {
        let channel_eps: std::result::Result<Vec<Endpoint>, MetaError> = (*endpoints)
            .iter()
            .map(|a| self.build_endpoint(a))
            .collect();
        let channel_eps = channel_eps?;
        let ch = Channel::balance_list(channel_eps.into_iter());
        Ok(ch)
    }

    async fn check(&self, mut ch: Self::Item) -> std::result::Result<Self::Item, Self::Error> {
        futures::future::poll_fn(|cx| ch.poll_ready(cx))
            .await
            .map_err(|e| {
                MetaNetworkError::ConnectionError(ConnectionError::new(e, "while check item"))
            })?;
        Ok(ch)
    }
}

pub struct MetaGrpcClient {
    // todo(ariesdevil): consider reuse this
    #[allow(dead_code)]
    conn_pool: Pool<MetaChannelManager>,
    endpoints: RwLock<Vec<String>>,
    #[allow(dead_code)]
    cancel_tx: Sender<()>, // Implicit using when client dropped, notify backgroud task to stop.
    username: String,
    password: String,
    token: RwLock<Option<Vec<u8>>>,
}

const AUTH_TOKEN_KEY: &str = "auth-token-bin";

impl MetaGrpcClient {
    pub async fn try_new(
        conf: &MetaGrpcClientConf,
    ) -> std::result::Result<Arc<MetaGrpcClient>, Infallible> {
        let mgr = MetaChannelManager {
            timeout: Some(Duration::from_secs(conf.client_timeout_in_second)),
            conf: conf.meta_service_config.tls_conf.clone(),
        };

        let (tx, rx) = oneshot::channel();

        let client = Arc::new(Self {
            conn_pool: Pool::new(mgr, Duration::from_millis(50)),
            endpoints: RwLock::new(conf.meta_service_config.endpoints.clone()),
            cancel_tx: tx,
            username: conf.meta_service_config.username.to_string(),
            password: conf.meta_service_config.password.to_string(),
            token: RwLock::new(None),
        });

        tokio::spawn(MetaGrpcClient::auto_sync_endpoints(client.clone(), rx));

        Ok(client)
    }

    #[tracing::instrument(level = "debug", skip(password))]
    pub async fn try_create(
        endpoints: Vec<String>,
        username: &str,
        password: &str,
        timeout: Option<Duration>,
        conf: Option<RpcClientTlsConfig>,
    ) -> Result<Arc<Self>> {
        let mgr = MetaChannelManager {
            timeout,
            conf: conf.clone(),
        };

        let (tx, rx) = oneshot::channel();

        let client = Arc::new(Self {
            conn_pool: Pool::new(mgr, Duration::from_millis(50)),
            endpoints: RwLock::new(endpoints),
            cancel_tx: tx,
            username: username.to_string(),
            password: password.to_string(),
            token: RwLock::new(None),
        });

        tokio::spawn(MetaGrpcClient::auto_sync_endpoints(client.clone(), rx));

        Ok(client)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn make_conn(
        &self,
    ) -> std::result::Result<
        MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>,
        MetaError,
    > {
        let eps = self.endpoints.read().await;
        if (*eps).is_empty() {
            return Err(MetaError::InvalidConfig("endpoints is empty".to_string()));
        }
        let channel = self.conn_pool.get(&*eps).await?;
        tracing::info!("connecting with channel: {:?}", channel);

        let mut client = MetaServiceClient::new(channel.clone());
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

        let client = { MetaServiceClient::with_interceptor(channel, AuthInterceptor { token }) };

        Ok(client)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn set_endpoints(&self, endpoints: Vec<String>) -> Result<()> {
        let mut eps = self.endpoints.write().await;
        *eps = endpoints;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn sync_endpoints(&self) -> Result<()> {
        let endpoints = {
            let eps = self.endpoints.read().await;
            (*eps).clone()
        };
        let channel = self.conn_pool.get(&endpoints).await?;
        // let channel = Channel::balance_list(channel_eps.into_iter());
        tracing::info!("connecting with channel: {:?}", channel);

        let mut client = MetaServiceClient::new(channel.clone());
        let endpoints = client
            .member_list(Request::new(MemberListRequest {
                data: "".to_string(),
            }))
            .await?;
        let result: Vec<String> = endpoints.into_inner().data;
        self.set_endpoints(result).await?;
        Ok(())
    }

    async fn auto_sync_endpoints(_client: Arc<MetaGrpcClient>, mut cancel_rx: Receiver<()>) {
        loop {
            select! {
                _ = &mut cancel_rx => {
                    return;
                }
                _ = tokio::time::sleep(time::Duration::from_secs(10)) => {
                    todo!();
                }
            }
        }
    }

    /// Handshake.
    #[tracing::instrument(level = "debug", skip(client, password))]
    async fn handshake(
        client: &mut MetaServiceClient<Channel>,
        username: &str,
        password: &str,
    ) -> std::result::Result<Vec<u8>, MetaError> {
        let auth = BasicAuth {
            username: username.to_string(),
            password: password.to_string(),
        };
        let mut payload = vec![];
        auth.encode(&mut payload)?;

        let req = Request::new(futures::stream::once(async {
            HandshakeRequest {
                protocol_version: 0,
                payload,
            }
        }));

        let rx = client.handshake(req).await?;
        let mut rx = rx.into_inner();

        let resp = rx.next().await.expect("Must respond from handshake")?;
        let token = resp.payload;
        Ok(token)
    }

    #[tracing::instrument(level = "debug", skip(self, v))]
    pub(crate) async fn do_write<T, R>(&self, v: T) -> std::result::Result<R, MetaError>
    where
        T: RequestFor<Reply = R> + Into<MetaGrpcWriteReq>,
        R: DeserializeOwned,
    {
        let act: MetaGrpcWriteReq = v.into();
        let req: Request<RaftRequest> = act.clone().try_into()?;
        let req = common_tracing::inject_span_to_tonic_request(req);

        let mut client = self.make_conn().await?;
        let result = client.write_msg(req).await;
        let result: std::result::Result<RaftReply, Status> = match result {
            Ok(r) => Ok(r.into_inner()),
            Err(s) => {
                if status_is_retryable(&s) {
                    {
                        let mut token = self.token.write().await;
                        *token = None;
                    }
                    let mut client = self.make_conn().await?;
                    let req: Request<RaftRequest> = act.try_into()?;
                    let req = common_tracing::inject_span_to_tonic_request(req);
                    Ok(client.write_msg(req).await?.into_inner())
                } else {
                    Err(s)
                }
            }
        };

        let raft_reply = result?;

        let res: std::result::Result<R, MetaError> = raft_reply.into();

        res
    }

    #[tracing::instrument(level = "debug", skip(self, v))]
    pub(crate) async fn do_read<T, R>(&self, v: T) -> std::result::Result<R, MetaError>
    where
        T: RequestFor<Reply = R>,
        T: Into<MetaGrpcReadReq>,
        R: DeserializeOwned,
    {
        let act: MetaGrpcReadReq = v.into();

        tracing::debug!(req = debug(&act), "MetaGrpcClient::do_read request");

        let req: Request<RaftRequest> = act.clone().try_into()?;
        let req = common_tracing::inject_span_to_tonic_request(req);

        let mut client = self.make_conn().await?;
        let result = client.read_msg(req).await;

        tracing::debug!(reply = debug(&result), "MetaGrpcClient::do_read reply");

        let rpc_res: std::result::Result<RaftReply, Status> = match result {
            Ok(r) => Ok(r.into_inner()),
            Err(s) => {
                if status_is_retryable(&s) {
                    {
                        let mut token = self.token.write().await;
                        *token = None;
                    }
                    let mut client = self.make_conn().await?;
                    let req: Request<RaftRequest> = act.try_into()?;
                    let req = common_tracing::inject_span_to_tonic_request(req);
                    Ok(client.read_msg(req).await?.into_inner())
                } else {
                    Err(s)
                }
            }
        };
        let raft_reply = rpc_res?;

        let res: std::result::Result<R, MetaError> = raft_reply.into();
        res
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    pub(crate) async fn transaction(
        &self,
        req: TxnRequest,
    ) -> std::result::Result<TxnReply, MetaError> {
        let txn: TxnRequest = req;

        tracing::debug!(req = display(&txn), "MetaGrpcClient::transaction request");

        let req: Request<TxnRequest> = Request::new(txn.clone());
        let req = common_tracing::inject_span_to_tonic_request(req);

        let mut client = self.make_conn().await?;
        let result = client.transaction(req).await;

        let result: std::result::Result<TxnReply, Status> = match result {
            Ok(r) => return Ok(r.into_inner()),
            Err(s) => {
                if status_is_retryable(&s) {
                    {
                        let mut token = self.token.write().await;
                        *token = None;
                    }
                    let mut client = self.make_conn().await?;
                    let req: Request<TxnRequest> = Request::new(txn);
                    let req = common_tracing::inject_span_to_tonic_request(req);
                    let ret = client.transaction(req).await?.into_inner();
                    return Ok(ret);
                } else {
                    Err(s)
                }
            }
        };

        let reply = result?;

        tracing::debug!(reply = display(&reply), "MetaGrpcClient::transaction reply");

        Ok(reply)
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
