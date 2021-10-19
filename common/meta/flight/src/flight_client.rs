// Copyright 2020 Datafuse Labs.
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

use std::convert::TryInto;
use std::time::Duration;

use common_arrow::arrow_format::flight::data::Action;
use common_arrow::arrow_format::flight::data::BasicAuth;
use common_arrow::arrow_format::flight::data::HandshakeRequest;
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_exception::ErrorCode;
use common_exception::Result;
use common_flight_rpc::ConnectionFactory;
use common_flight_rpc::FlightClientTlsConfig;
use common_tracing::tracing;
use futures::stream;
use futures::StreamExt;
use prost::Message;
use serde::de::DeserializeOwned;
use tonic::codegen::InterceptedService;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::Request;

use crate::flight_action::MetaFlightAction;
use crate::flight_action::RequestFor;
use crate::flight_client_conf::MetaFlightClientConf;

#[derive(Clone, Debug)]
pub struct MetaFlightClient {
    #[allow(dead_code)]
    token: Vec<u8>,
    pub(crate) client: FlightServiceClient<InterceptedService<Channel, AuthInterceptor>>,
}

const AUTH_TOKEN_KEY: &str = "auth-token-bin";

impl MetaFlightClient {
    pub async fn try_new(conf: &MetaFlightClientConf) -> Result<MetaFlightClient> {
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
        conf: Option<FlightClientTlsConfig>,
    ) -> Result<Self> {
        let res = ConnectionFactory::create_flight_channel(addr, timeout, conf);

        tracing::debug!("connecting to {}, res: {:?}", addr, res);

        let channel = res?;

        let mut client = FlightServiceClient::new(channel.clone());
        let token = MetaFlightClient::handshake(&mut client, username, password).await?;

        let client = {
            let token = token.clone();
            FlightServiceClient::with_interceptor(channel, AuthInterceptor { token })
        };

        let rx = Self { token, client };
        Ok(rx)
    }

    /// Handshake.
    #[tracing::instrument(level = "debug", skip(client, password))]
    async fn handshake(
        client: &mut FlightServiceClient<Channel>,
        username: &str,
        password: &str,
    ) -> Result<Vec<u8>> {
        let auth = BasicAuth {
            username: username.to_string(),
            password: password.to_string(),
        };
        let mut payload = vec![];
        auth.encode(&mut payload)?;

        let req = Request::new(stream::once(async {
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
    pub(crate) async fn do_action<T, R>(&self, v: T) -> Result<R>
    where
        T: RequestFor<Reply = R>,
        T: Into<MetaFlightAction>,
        R: DeserializeOwned,
    {
        let act: MetaFlightAction = v.into();
        let req: Request<Action> = (&act).try_into()?;
        let req = common_tracing::inject_span_to_tonic_request(req);

        let mut stream = self.client.clone().do_action(req).await?.into_inner();
        match stream.message().await? {
            None => Err(ErrorCode::EmptyData(format!(
                "Can not receive data from dfs flight server, action: {:?}",
                act
            ))),
            Some(resp) => {
                let v = serde_json::from_slice::<R>(&resp.body)?;
                Ok(v)
            }
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
