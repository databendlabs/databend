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
use crate::ConnectionFactory;
use crate::StoreDoAction;

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

    //    pub async fn add_user(
    //        &mut self,
    //        username: impl Into<String>,
    //        password: impl Into<String>,
    //        salt: impl Into<String>,
    //    ) -> common_exception::Result<AddUserActionResult> {
    //        let action = StoreDoAction::AddUser(AddUserAction {
    //            username: username.into(),
    //            password: password.into(),
    //            salt: salt.into(),
    //        });
    //        let rst = self.do_action(&action).await?;
    //        match_action_res!(StoreDoActionResult::AddUser, rst)
    //    }
    //
    //    pub async fn drop_user(
    //        &mut self,
    //        username: impl Into<String>,
    //    ) -> common_exception::Result<DropUserActionResult> {
    //        let action = StoreDoAction::DropUser(DropUserAction {
    //            username: username.into(),
    //        });
    //        let rst = self.do_action(&action).await?;
    //        match_action_res!(StoreDoActionResult::DropUser, rst)
    //    }
    //
    //    pub async fn update_user(
    //        &mut self,
    //        username: impl Into<String>,
    //        new_password: Option<impl Into<String>>,
    //        new_salt: Option<impl Into<String>>,
    //    ) -> common_exception::Result<UpdateUserActionResult> {
    //        let action = StoreDoAction::UpdateUser(UpdateUserAction {
    //            username: username.into(),
    //            new_password: new_password.map(Into::into),
    //            new_salt: new_salt.map(Into::into),
    //        });
    //        let rst = self.do_action(&action).await?;
    //        match_action_res!(StoreDoActionResult::UpdateUser, rst)
    //    }
    //
    //    pub async fn get_all_users(&mut self) -> common_exception::Result<GetAllUsersActionResult> {
    //        let action = StoreDoAction::GetAllUsersInfo(GetAllUsersAction {});
    //        let rst = self.do_action(&action).await?;
    //        match_action_res!(StoreDoActionResult::GetAllUsersInfo, rst)
    //    }
    //
    //    pub async fn get_users(
    //        &mut self,
    //        usernames: &[impl AsRef<str>],
    //    ) -> common_exception::Result<GetUsersActionResult> {
    //        let action = StoreDoAction::GetUsersInfo(GetUsersAction {
    //            usernames: usernames.iter().map(|n| n.as_ref().to_string()).collect(),
    //        });
    //        let rst = self.do_action(&action).await?;
    //        match_action_res!(StoreDoActionResult::GetUsersInfo, rst)
    //    }
    //
    //    pub async fn get_user(
    //        &mut self,
    //        username: impl AsRef<str>,
    //    ) -> common_exception::Result<GetUserActionResult> {
    //        let action = StoreDoAction::GetUserInfo(GetUserAction {
    //            username: username.as_ref().to_string(),
    //        });
    //        let rst = self.do_action(&action).await?;
    //        match_action_res!(StoreDoActionResult::GetUserInfo, rst)
    //    }
}
