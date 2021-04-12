// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryInto;

use common_arrow::arrow_flight;
use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_arrow::arrow_flight::Action;
use common_arrow::arrow_flight::BasicAuth;
use common_arrow::arrow_flight::HandshakeRequest;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use futures::stream;
use futures::StreamExt;
use log::info;
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::Request;

use crate::flight_result_to_str;
use crate::status_err;
use crate::store_do_action::CreateDatabaseAction;
use crate::store_do_action::CreateTableAction;
use crate::store_do_action::StoreDoAction;
use crate::store_do_action::StoreDoActionResult;

#[derive(Clone)]
pub struct StoreClient {
    token: Vec<u8>,
    client: FlightServiceClient<tonic::transport::channel::Channel>,
}

impl StoreClient {
    pub async fn try_create(addr: &str, username: &str, password: &str) -> anyhow::Result<Self> {
        let client = FlightServiceClient::connect(format!("http://{}", addr)).await?;
        let mut rx = Self {
            token: vec![],
            client,
        };
        rx.handshake(username, password).await?;
        Ok(rx)
    }

    /// Create database call.
    pub async fn create_database(
        &mut self,
        plan: CreateDatabasePlan,
    ) -> anyhow::Result<StoreDoActionResult> {
        let action = StoreDoAction::CreateDatabase(CreateDatabaseAction { plan });
        let rst = self.do_action(&action).await?;
        let action_rst: StoreDoActionResult = rst.try_into()?;
        Ok(action_rst)
    }

    /// Create table call.
    pub async fn create_table(
        &mut self,
        plan: CreateTablePlan,
    ) -> anyhow::Result<StoreDoActionResult> {
        let action = StoreDoAction::CreateTable(CreateTableAction { plan });
        let rst = self.do_action(&action).await?;
        let action_rst: StoreDoActionResult = rst.try_into()?;
        Ok(action_rst)
    }

    /// Handshake.
    async fn handshake(&mut self, username: &str, password: &str) -> anyhow::Result<()> {
        let auth = BasicAuth {
            username: username.to_string(),
            password: password.to_string(),
        };
        let mut payload = vec![];
        auth.encode(&mut payload)?;

        let req = stream::once(async {
            HandshakeRequest {
                payload,
                ..HandshakeRequest::default()
            }
        });
        let rx = self.client.handshake(Request::new(req)).await?;
        let mut rx = rx.into_inner();

        let resp = rx.next().await.expect("Must respond from handshake")?;
        self.token = resp.payload;

        Ok(())
    }

    /// Execute do_action.
    async fn do_action(&mut self, action: &StoreDoAction) -> anyhow::Result<arrow_flight::Result> {
        // TODO: an action can always be able to serialize, or it is a bug.
        let mut request: Request<Action> = action.try_into().unwrap();
        let metadata = request.metadata_mut();
        metadata.insert_bin(
            "auth-token-bin",
            MetadataValue::from_bytes(&self.token.clone()),
        );

        let mut stream = self
            .client
            .do_action(request)
            .await
            .map_err(status_err)?
            .into_inner();
        match stream.message().await? {
            None => anyhow::bail!(
                "Can not receive data from store flight server, action: {:?}",
                action
            ),
            Some(resp) => {
                info!("do_action: resp: {:}", flight_result_to_str(&resp));
                Ok(resp)
            }
        }
    }
}
