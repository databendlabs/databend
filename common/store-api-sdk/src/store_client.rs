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

use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_arrow::arrow_flight::Action;
use common_arrow::arrow_flight::BasicAuth;
use common_arrow::arrow_flight::HandshakeRequest;
use common_exception::ErrorCode;
use common_exception::Result;
use common_store_api::util::STORE_RUNTIME;
use common_store_api::util::STORE_SYNC_CALL_TIMEOUT;
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

use crate::common::flight_result_to_str;
use crate::store_client_conf::StoreClientConf;
use crate::store_do_action::RequestFor;
use crate::store_do_action::StoreDoAction;
use crate::ConnectionFactory;
use crate::RpcClientTlsConfig;

#[derive(Clone)]
pub struct StoreClient {
    #[allow(dead_code)]
    token: Vec<u8>,
    pub(crate) timeout: Duration,
    pub(crate) client: FlightServiceClient<InterceptedService<Channel, AuthInterceptor>>,
}

const AUTH_TOKEN_KEY: &str = "auth-token-bin";

impl StoreClient {
    pub async fn try_new(conf: &StoreClientConf) -> Result<StoreClient> {
        Self::with_tls_conf(
            &conf.meta_service_config.address,
            &conf.meta_service_config.username,
            &conf.meta_service_config.password,
            conf.meta_service_config.tls_conf.clone(),
        )
        .await
    }

    pub fn sync_try_new(conf: &StoreClientConf) -> Result<StoreClient> {
        let cfg = conf.clone();
        STORE_RUNTIME.block_on(
            async move { StoreClient::try_new(&cfg).await },
            STORE_SYNC_CALL_TIMEOUT.as_ref().cloned(),
        )?
    }

    #[tracing::instrument(level = "debug", skip(password))]
    pub async fn try_create(addr: &str, username: &str, password: &str) -> Result<Self> {
        Self::with_tls_conf(addr, username, password, None).await
    }

    #[tracing::instrument(level = "debug", skip(password))]
    pub async fn with_tls_conf(
        addr: &str,
        username: &str,
        password: &str,
        conf: Option<RpcClientTlsConfig>,
    ) -> Result<Self> {
        // TODO configuration
        let timeout = Duration::from_secs(60);

        let res = ConnectionFactory::create_flight_channel(addr, Some(timeout), conf);

        tracing::debug!("connecting to {}, res: {:?}", addr, res);

        let channel = res?;

        let mut client = FlightServiceClient::new(channel.clone());
        let token = StoreClient::handshake(&mut client, timeout, username, password).await?;

        let client = {
            let token = token.clone();
            FlightServiceClient::with_interceptor(channel, AuthInterceptor { token })
        };

        let rx = Self {
            token,
            timeout,
            client,
        };
        Ok(rx)
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    /// Handshake.
    #[tracing::instrument(level = "debug", skip(client, password))]
    async fn handshake(
        client: &mut FlightServiceClient<Channel>,
        timeout: Duration,
        username: &str,
        password: &str,
    ) -> Result<Vec<u8>> {
        let auth = BasicAuth {
            username: username.to_string(),
            password: password.to_string(),
        };
        let mut payload = vec![];
        auth.encode(&mut payload)?;

        let mut req = Request::new(stream::once(async {
            HandshakeRequest {
                payload,
                ..HandshakeRequest::default()
            }
        }));
        req.set_timeout(timeout);

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
        T: Into<StoreDoAction>,
        R: DeserializeOwned,
    {
        let act: StoreDoAction = v.into();
        let req: Request<Action> = (&act).try_into()?;
        let mut req = common_tracing::inject_span_to_tonic_request(req);

        req.set_timeout(self.timeout);

        let mut stream = self.client.clone().do_action(req).await?.into_inner();
        match stream.message().await? {
            None => Err(ErrorCode::EmptyData(format!(
                "Can not receive data from store flight server, action: {:?}",
                act
            ))),
            Some(resp) => {
                log::debug!("do_action: resp: {:}", flight_result_to_str(&resp));
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
