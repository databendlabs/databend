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

use std::time::Duration;

use common_arrow::arrow_format::flight::data::BasicAuth;
use common_exception::ErrorCode;
use common_exception::Result;
use common_flight_rpc::ConnectionFactory;
use common_flight_rpc::GrpcClientTlsConfig;
use common_tracing::tracing;
use futures::stream::StreamExt;
use prost::Message;
use serde::de::DeserializeOwned;
use tonic::codegen::InterceptedService;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::Request;

use crate::grpc::grpc_action::MetaGrpcGetAction;
use crate::grpc::grpc_action::MetaGrpcWriteAction;
use crate::grpc::grpc_action::RequestFor;
use crate::protobuf::meta_service_client::MetaServiceClient;
use crate::protobuf::GetReq;
use crate::protobuf::HandshakeRequest;
use crate::protobuf::RaftRequest;
use crate::MetaGrpcClientConf;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct MetaGrpcClient {
    token: Vec<u8>,
    pub(crate) client: MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>,
}

const AUTH_TOKEN_KEY: &str = "auth-token-bin";

impl MetaGrpcClient {
    pub async fn try_new(conf: &MetaGrpcClientConf) -> Result<MetaGrpcClient> {
        Self::with_tls_conf(
            &conf.meta_service_config.address,
            &conf.meta_service_config.username,
            &conf.meta_service_config.password,
            Some(Duration::from_secs(conf.client_timeout_in_second)),
            conf.meta_service_config.tls_conf.clone(),
        )
        .await
    }

    #[tracing::instrument(level = "debug", skip(password))]
    pub async fn try_create(addr: &str, username: &str, password: &str) -> Result<Self> {
        Self::with_tls_conf(addr, username, password, None, None).await
    }

    #[tracing::instrument(level = "debug", skip(password))]
    pub async fn with_tls_conf(
        addr: &str,
        username: &str,
        password: &str,
        timeout: Option<Duration>,
        conf: Option<GrpcClientTlsConfig>,
    ) -> Result<Self> {
        let res = ConnectionFactory::create_grpc_channel(addr, timeout, conf);

        tracing::debug!("connecting to {}, res: {:?}", addr, res);

        let channel = res?;

        let mut client = MetaServiceClient::new(channel.clone());
        let token = MetaGrpcClient::handshake(&mut client, username, password).await?;

        let client = {
            let token = token.clone();
            MetaServiceClient::with_interceptor(channel, AuthInterceptor { token })
        };

        let rx = Self { token, client };
        Ok(rx)
    }

    /// Handshake.
    #[tracing::instrument(level = "debug", skip(client, password))]
    async fn handshake(
        client: &mut MetaServiceClient<Channel>,
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
        T: RequestFor<Reply = R>,
        T: Into<MetaGrpcWriteAction>,
        R: DeserializeOwned,
    {
        let act: MetaGrpcWriteAction = v.into();
        let req: Request<RaftRequest> = (&act).try_into()?;
        let req = common_tracing::inject_span_to_tonic_request(req);

        let result = self.client.clone().write(req).await?.into_inner();

        if result.error.is_empty() {
            let v = serde_json::from_str::<R>(&result.data)?;
            Ok(v)
        } else {
            Err(ErrorCode::EmptyData(format!(
                "Can not receive data from grpc server, action: {:?}, err: {}",
                act, result.error
            )))
        }
    }

    #[tracing::instrument(level = "debug", skip(self, v))]
    pub(crate) async fn do_get<T, R>(&self, v: T) -> Result<R>
    where
        T: RequestFor<Reply = R>,
        T: Into<MetaGrpcGetAction>,
        R: DeserializeOwned,
    {
        let act: MetaGrpcGetAction = v.into();
        let req: Request<GetReq> = (&act).try_into()?;
        let req = common_tracing::inject_span_to_tonic_request(req);

        let result = self.client.clone().get(req).await?.into_inner();
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
