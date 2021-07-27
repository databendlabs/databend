// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::convert::TryFrom;

use common_arrow::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use common_arrow::arrow::ipc::writer::IpcWriteOptions;
use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::arrow_flight::utils::flight_data_from_arrow_batch;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_arrow::arrow_flight::SchemaAsIpc;
use common_arrow::arrow_flight::Ticket;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_planners::ScanPlan;
use common_runtime::tokio;
pub use common_store_api::AppendResult;
pub use common_store_api::BlockStream;
pub use common_store_api::DataPartInfo;
pub use common_store_api::ReadAction;
pub use common_store_api::ReadPlanResult;
pub use common_store_api::StorageApi;
pub use common_store_api::TruncateTableResult;
use common_streams::SendableDataBlockStream;
use futures::SinkExt;
use futures::StreamExt;
use tonic::Request;

use crate::action_declare;
use crate::impls::storage_api_impl_utils;
pub use crate::impls::storage_api_impl_utils::get_meta;
use crate::RequestFor;
use crate::StoreClient;
use crate::StoreDoAction;
use crate::StoreDoGet;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ReadPlanAction {
    pub scan_plan: ScanPlan,
}
action_declare!(ReadPlanAction, ReadPlanResult, StoreDoAction::ReadPlan);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct TruncateTableAction {
    pub db: String,
    pub table: String,
}
action_declare!(
    TruncateTableAction,
    TruncateTableResult,
    StoreDoAction::TruncateTable
);

#[async_trait::async_trait]
impl StorageApi for StoreClient {
    async fn read_plan(
        &mut self,
        db_name: String,
        tbl_name: String,
        scan_plan: &ScanPlan,
    ) -> common_exception::Result<ReadPlanResult> {
        let mut plan = scan_plan.clone();
        plan.schema_name = format!("{}/{}", db_name, tbl_name);
        let plan = ReadPlanAction { scan_plan: plan };
        self.do_action(plan).await
    }

    async fn read_partition(
        &mut self,
        schema: DataSchemaRef,
        read_action: &ReadAction,
    ) -> common_exception::Result<SendableDataBlockStream> {
        let cmd = StoreDoGet::Read(read_action.clone());
        let mut req = tonic::Request::<Ticket>::from(&cmd);
        req.set_timeout(self.timeout);
        let res = self.client.do_get(req).await?.into_inner();
        let arrow_schema: ArrowSchemaRef = Arc::new(schema.to_arrow());
        let res_stream = res.map(move |item| {
            item.map_err(|status| ErrorCode::TokioError(status.to_string()))
                .and_then(|item| {
                    flight_data_to_arrow_batch(&item, arrow_schema.clone(), &[])
                        .map_err(ErrorCode::from)
                })
                .and_then(DataBlock::try_from)
        });
        Ok(Box::pin(res_stream))
    }

    async fn append_data(
        &mut self,
        db_name: String,
        tbl_name: String,
        scheme_ref: DataSchemaRef,
        mut block_stream: BlockStream,
    ) -> common_exception::Result<AppendResult> {
        let ipc_write_opt = IpcWriteOptions::default();
        let arrow_schema: ArrowSchemaRef = Arc::new(scheme_ref.to_arrow());
        let flight_schema = SchemaAsIpc::new(arrow_schema.as_ref(), &ipc_write_opt).into();
        let (mut tx, flight_stream) = futures::channel::mpsc::channel(100);
        tx.send(flight_schema)
            .await
            .map_err(|send_err| ErrorCode::BrokenChannel(send_err.to_string()))?;

        tokio::spawn(async move {
            while let Some(block) = block_stream.next().await {
                log::info!("next data block");
                match RecordBatch::try_from(block) {
                    Ok(batch) => {
                        if let Err(_e) = tx
                            .send(flight_data_from_arrow_batch(&batch, &ipc_write_opt).1)
                            .await
                        {
                            log::error!("failed to send flight-data to downstream, breaking out");
                            break;
                        }
                    }
                    Err(e) => {
                        log::error!(
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
        storage_api_impl_utils::put_meta(meta, &db_name, &tbl_name);

        let res = self.client.do_put(req).await?;

        use anyhow::Context;
        let put_result = res.into_inner().next().await.context("empty response")??;
        let vec = serde_json::from_slice(&put_result.app_metadata)?;
        Ok(vec)
    }

    async fn truncate(
        &mut self,
        db: String,
        table: String,
    ) -> common_exception::Result<TruncateTableResult> {
        self.do_action(TruncateTableAction { db, table }).await
    }
}
