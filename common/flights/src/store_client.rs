// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::convert::TryInto;
use std::time::Duration;

use common_arrow::arrow::datatypes::SchemaRef;
use common_arrow::arrow::ipc::writer::IpcWriteOptions;
use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::arrow_flight::flight_service_client::FlightServiceClient;
use common_arrow::arrow_flight::utils::flight_data_from_arrow_batch;
use common_arrow::arrow_flight::utils::flight_data_from_arrow_schema;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_arrow::arrow_flight::Action;
use common_arrow::arrow_flight::BasicAuth;
use common_arrow::arrow_flight::HandshakeRequest;
use common_arrow::arrow_flight::Ticket;
use common_datablocks::DataBlock;
use common_exception::ErrorCodes;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_planners::ScanPlan;
use common_streams::SendableDataBlockStream;
use futures::stream;
use futures::SinkExt;
use futures::StreamExt;
use log::info;
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

use crate::flight_result_to_str;
use crate::status_err;
use crate::store_do_action::CreateDatabaseAction;
use crate::store_do_action::CreateTableAction;
use crate::store_do_action::DropDatabaseAction;
use crate::store_do_action::DropDatabaseActionResult;
use crate::store_do_action::StoreDoAction;
use crate::store_do_action::StoreDoActionResult;
use crate::store_do_get::ReadAction;
use crate::store_do_put;
use crate::store_do_put::AppendResult;
use crate::ConnectionFactory;
use crate::CreateDatabaseActionResult;
use crate::CreateTableActionResult;
use crate::DropTableAction;
use crate::DropTableActionResult;
use crate::GetTableAction;
use crate::GetTableActionResult;
use crate::ScanPartitionAction;
use crate::ScanPartitionResult;
use crate::StoreDoGet;

pub type BlockStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = DataBlock> + Sync + Send + 'static>>;

#[derive(Clone)]
pub struct StoreClient {
    token: Vec<u8>,
    timeout: Duration,
    client: FlightServiceClient<tonic::transport::channel::Channel>,
}

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
                metadata.insert_bin("auth-token-bin", MetadataValue::from_bytes(&token));
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

    /// Create database call.
    pub async fn create_database(
        &mut self,
        plan: CreateDatabasePlan,
    ) -> anyhow::Result<CreateDatabaseActionResult> {
        let action = StoreDoAction::CreateDatabase(CreateDatabaseAction { plan });
        let rst = self.do_action(&action).await?;

        if let StoreDoActionResult::CreateDatabase(rst) = rst {
            return Ok(rst);
        }
        anyhow::bail!("invalid response")
    }

    /// Drop database call.
    pub async fn drop_database(
        &mut self,
        plan: DropDatabasePlan,
    ) -> anyhow::Result<DropDatabaseActionResult> {
        let action = StoreDoAction::DropDatabase(DropDatabaseAction { plan });
        let rst = self.do_action(&action).await?;

        if let StoreDoActionResult::DropDatabase(rst) = rst {
            return Ok(rst);
        }
        anyhow::bail!("invalid response")
    }

    /// Create table call.
    pub async fn create_table(
        &mut self,
        plan: CreateTablePlan,
    ) -> anyhow::Result<CreateTableActionResult> {
        let action = StoreDoAction::CreateTable(CreateTableAction { plan });
        let rst = self.do_action(&action).await?;

        if let StoreDoActionResult::CreateTable(rst) = rst {
            return Ok(rst);
        }
        anyhow::bail!("invalid response")
    }

    /// Drop table call.
    pub async fn drop_table(
        &mut self,
        plan: DropTablePlan,
    ) -> anyhow::Result<DropTableActionResult> {
        let action = StoreDoAction::DropTable(DropTableAction { plan });
        let rst = self.do_action(&action).await?;

        if let StoreDoActionResult::DropTable(rst) = rst {
            return Ok(rst);
        }
        anyhow::bail!("invalid response")
    }

    /// Get table.
    pub async fn get_table(
        &mut self,
        db: String,
        table: String,
    ) -> anyhow::Result<GetTableActionResult> {
        let action = StoreDoAction::GetTable(GetTableAction { db, table });
        let rst = self.do_action(&action).await?;

        if let StoreDoActionResult::GetTable(rst) = rst {
            return Ok(rst);
        }
        anyhow::bail!("invalid response")
    }

    pub async fn scan_partition(
        &mut self,
        db_name: String,
        tbl_name: String,
        scan_plan: &ScanPlan,
    ) -> anyhow::Result<ScanPartitionResult> {
        let mut plan = scan_plan.clone();
        plan.schema_name = format!("{}/{}", db_name, tbl_name);
        let action = StoreDoAction::ScanPartition(ScanPartitionAction { scan_plan: plan });
        let rst = self.do_action(&action).await?;

        if let StoreDoActionResult::ScanPartition(rst) = rst {
            return Ok(rst);
        }
        anyhow::bail!("invalid response")
    }

    /// Get partition.
    pub async fn read_partition(
        &mut self,
        schema: SchemaRef,
        read_action: &ReadAction,
    ) -> anyhow::Result<SendableDataBlockStream> {
        let cmd = StoreDoGet::Read(read_action.clone());
        let mut req = tonic::Request::<Ticket>::from(&cmd);
        req.set_timeout(self.timeout);
        let res = self.client.do_get(req).await?.into_inner();
        let res_stream = res.map(move |item| {
            item.map_err(|status| ErrorCodes::TokioError(status.to_string()))
                .and_then(|item| {
                    flight_data_to_arrow_batch(&item, schema.clone(), &[]).map_err(ErrorCodes::from)
                })
                .and_then(DataBlock::try_from)
        });
        Ok(Box::pin(res_stream))
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

    /// Execute do_action.
    async fn do_action(&mut self, action: &StoreDoAction) -> anyhow::Result<StoreDoActionResult> {
        // TODO: an action can always be able to serialize, or it is a bug.
        let mut req: Request<Action> = action.try_into()?;
        req.set_timeout(self.timeout);

        let mut stream = self
            .client
            .do_action(req)
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

                let action_rst: StoreDoActionResult = resp.try_into()?;
                Ok(action_rst)
            }
        }
    }

    /// Appends data partitions to specified table
    pub async fn append_data(
        &mut self,
        db_name: String,
        tbl_name: String,
        scheme_ref: SchemaRef,
        mut block_stream: BlockStream,
    ) -> anyhow::Result<AppendResult> {
        let ipc_write_opt = IpcWriteOptions::default();
        let flight_schema = flight_data_from_arrow_schema(&scheme_ref, &ipc_write_opt);
        let (mut tx, flight_stream) = futures::channel::mpsc::channel(100);

        tx.send(flight_schema).await?;

        tokio::spawn(async move {
            while let Some(block) = block_stream.next().await {
                info!("next data block");
                match RecordBatch::try_from(block) {
                    Ok(batch) => {
                        if let Err(_e) = tx
                            .send(flight_data_from_arrow_batch(&batch, &ipc_write_opt).1)
                            .await
                        {
                            log::info!("failed to send flight-data to downstream, breaking out");
                            break;
                        }
                    }
                    Err(e) => {
                        log::info!(
                            "failed to convert DataBlock to RecordBatch , breaking out, {:?}",
                            e
                        );
                        break;
                    }
                }
            }
        });

        let mut req = Request::new(flight_stream);
        let meta = req.metadata_mut();
        store_do_put::set_do_put_meta(meta, &db_name, &tbl_name);

        let res = self.client.do_put(req).await?;

        use anyhow::Context;
        let put_result = res.into_inner().next().await.context("empty response")??;
        let vec = serde_json::from_slice(&put_result.app_metadata)?;
        Ok(vec)
    }
}
