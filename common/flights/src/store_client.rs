// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;
use std::time::Duration;

use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_arrow::arrow_flight::Action;
use common_arrow::arrow_flight::BasicAuth;
use common_arrow::arrow_flight::HandshakeRequest;
use common_exception::ErrorCode;
use futures::stream;
use futures::StreamExt;
use log::info;
use prost::Message;
use serde::de::DeserializeOwned;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

use crate::flight_result_to_str;
use crate::store_do_action::RequestFor;
use crate::store_do_action::StoreDoAction;
use crate::ConnectionFactory;

#[derive(Clone)]
pub struct StoreClient {
    token: Vec<u8>,
    pub(crate) timeout: Duration,
    pub(crate) client: FlightServiceClient<tonic::transport::channel::Channel>,
}

static AUTH_TOKEN_KEY: &str = "auth-token-bin";

impl StoreClient {
    pub async fn try_create(addr: &str, username: &str, password: &str) -> anyhow::Result<Self> {
        // TODO configuration
        let timeout = Duration::from_secs(60);

        let channel = ConnectionFactory::create_flight_channel(addr, Some(timeout)).await?;

        let mut client = FlightServiceClient::new(channel.clone());
        let token = StoreClient::handshake(&mut client, timeout, username, password).await?;

        let client = {
            let token = token.clone();
            FlightServiceClient::with_interceptor(channel, move |mut req: Request<()>| {
                let metadata = req.metadata_mut();
                metadata.insert_bin(AUTH_TOKEN_KEY, MetadataValue::from_bytes(&token));
                Ok(req)
            })
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
    async fn handshake(
        client: &mut FlightServiceClient<Channel>,
        timeout: Duration,
        username: &str,
        password: &str,
    ) -> anyhow::Result<Vec<u8>> {
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

    pub(crate) async fn do_action<T, R>(&mut self, v: T) -> common_exception::Result<R>
    where
        T: RequestFor<Reply = R>,
        T: Into<StoreDoAction>,
        R: DeserializeOwned,
    {
        let act: StoreDoAction = v.into();
        let mut req: Request<Action> = (&act).try_into()?;
        req.set_timeout(self.timeout);

        let mut stream = self.client.do_action(req).await?.into_inner();
        match stream.message().await? {
            None => Err(ErrorCode::EmptyData(format!(
                "Can not receive data from store flight server, action: {:?}",
                act
            ))),
            Some(resp) => {
                info!("do_action: resp: {:}", flight_result_to_str(&resp));
                let v = serde_json::from_slice::<R>(&resp.body)?;
                Ok(v)
            }
        }
    }
}
