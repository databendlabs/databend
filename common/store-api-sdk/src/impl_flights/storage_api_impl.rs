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
//

use std::convert::TryFrom;
use std::sync::Arc;

// io::ipc::write::common::{encoded_batch, DictionaryTracker, EncodedData, IpcWriteOptions}
use common_arrow::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use common_arrow::arrow::io::ipc::write::common::IpcWriteOptions;
use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::arrow_flight::utils::flight_data_from_arrow_batch;
use common_arrow::arrow_flight::utils::flight_data_from_arrow_schema;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_arrow::arrow_flight::Ticket;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
pub use common_dfs_api::StorageApi;
pub use common_dfs_api_vo::AppendResult;
pub use common_dfs_api_vo::BlockStream;
pub use common_dfs_api_vo::DataPartInfo;
pub use common_dfs_api_vo::ReadAction;
pub use common_dfs_api_vo::ReadPlanResult;
pub use common_dfs_api_vo::TruncateTableResult;
use common_exception::ErrorCode;
use common_planners::PlanNode;
use common_planners::ScanPlan;
use common_runtime::tokio;
use common_streams::SendableDataBlockStream;
use futures::SinkExt;
use futures::StreamExt;
use tonic::Request;

use crate::action_declare;
use crate::impl_flights::storage_api_impl_utils;
pub use crate::impl_flights::storage_api_impl_utils::get_meta;
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
        &self,
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
        &self,
        schema: DataSchemaRef,
        read_action: &ReadAction,
    ) -> common_exception::Result<SendableDataBlockStream> {
        let cmd = StoreDoGet::Read(read_action.clone());
        let mut req = tonic::Request::<Ticket>::from(&cmd);
        req.set_timeout(self.timeout);
        let res = self.client.clone().do_get(req).await?.into_inner();
        let mut arrow_schema: ArrowSchemaRef = Arc::new(schema.to_arrow());

        // replace table schema with projected schema
        // TODO tweak method signature, only ReadDataSourcePlan are supposed to be passed in
        if let PlanNode::ReadSource(plan) = &read_action.push_down {
            arrow_schema = Arc::new(plan.schema.to_arrow())
        }

        let res_stream = res.map(move |item| {
            item.map_err(|status| ErrorCode::TokioError(status.to_string()))
                .and_then(|item| {
                    flight_data_to_arrow_batch(&item, arrow_schema.clone(), true, &[])
                        .map_err(ErrorCode::from)
                })
                .and_then(DataBlock::try_from)
        });
        Ok(Box::pin(res_stream))
    }

    async fn append_data(
        &self,
        db_name: String,
        tbl_name: String,
        scheme_ref: DataSchemaRef,
        mut block_stream: BlockStream,
    ) -> common_exception::Result<AppendResult> {
        let ipc_write_opt = IpcWriteOptions::default();
        let arrow_schema: ArrowSchemaRef = Arc::new(scheme_ref.to_arrow());

        let flight_schema = flight_data_from_arrow_schema(arrow_schema.as_ref(), &ipc_write_opt);
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

        let res = self.client.clone().do_put(req).await?;

        match res.into_inner().message().await? {
            Some(res) => Ok(serde_json::from_slice(&res.app_metadata)?),
            None => Err(ErrorCode::UnknownException("Put result is empty")),
        }
    }

    async fn truncate(
        &self,
        db: String,
        table: String,
    ) -> common_exception::Result<TruncateTableResult> {
        self.do_action(TruncateTableAction { db, table }).await
    }
}
