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

use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;

use common_arrow::arrow::io::ipc::write::common::IpcWriteOptions;
use common_arrow::arrow::io::parquet::read;
use common_arrow::arrow_flight::utils::flight_data_from_arrow_batch;
use common_arrow::arrow_flight::FlightData;
use common_exception::ErrorCode;
use common_flights::storage_api_impl::AppendResult;
use common_flights::storage_api_impl::ReadAction;
use common_flights::RequestFor;
use common_flights::StoreDoAction;
use common_planners::PlanNode;
use common_runtime::tokio::sync::mpsc::Sender;
use futures::Stream;
use serde::Serialize;
use tokio_stream::StreamExt;
use tonic::Status;
use tonic::Streaming;

use crate::data_part::appender::Appender;
use crate::fs::FileSystem;
use crate::meta_service::MetaNode;

pub trait ReplySerializer {
    type Output;
    fn serialize<T>(&self, v: T) -> Result<Self::Output, ErrorCode>
    where T: Serialize;
}

pub struct ActionHandler {
    /// The raft-based meta data entry.
    /// In our design meta serves for both the distributed file system and the catalogs storage such as db,tabel etc.
    /// Thus in case the `fs` is a Dfs impl, `meta_node` is just a reference to the `Dfs.meta_node`.
    pub(crate) meta_node: Arc<MetaNode>,
    fs: Arc<dyn FileSystem>,
}

// TODO did this already defined somewhere?
type DoGetStream =
    Pin<Box<dyn Stream<Item = Result<FlightData, tonic::Status>> + Send + Sync + 'static>>;

#[async_trait::async_trait]
pub trait RequestHandler<T>: Sync + Send
where T: RequestFor
{
    async fn handle(&self, req: T) -> common_exception::Result<T::Reply>;
}

impl ActionHandler {
    pub fn create(fs: Arc<dyn FileSystem>, meta_node: Arc<MetaNode>) -> Self {
        ActionHandler { meta_node, fs }
    }

    /// Handle pull-file request, which is used internally for replicating data copies.
    /// In DatafuseStore impl there is no internal file id etc, thus replication use the same `key` in communication with DatabendQuery as in internal replication.
    pub async fn do_pull_file(
        &self,
        key: String,
        tx: Sender<Result<FlightData, tonic::Status>>,
    ) -> Result<(), Status> {
        // TODO: stream read if the file is too large.
        let buf = self
            .fs
            .read_all(&key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        tx.send(Ok(FlightData {
            data_body: buf,
            ..Default::default()
        }))
        .await
        .map_err(|e| Status::internal(format!("{:?}", e)))
    }

    pub async fn execute<S, R>(&self, action: StoreDoAction, s: S) -> common_exception::Result<R>
    where S: ReplySerializer<Output = R> {
        // To keep the code IDE-friendly, we manually expand the enum variants and dispatch them one by one

        match action {
            // database
            StoreDoAction::CreateDatabase(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::GetDatabase(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::DropDatabase(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::GetDatabaseMeta(a) => s.serialize(self.handle(a).await?),

            // table
            StoreDoAction::CreateTable(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::DropTable(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::GetTable(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::GetTableExt(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::TruncateTable(a) => s.serialize(self.handle(a).await?),

            // part
            StoreDoAction::ReadPlan(a) => s.serialize(self.handle(a).await?),

            // general-purpose kv
            StoreDoAction::UpsertKV(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::UpdateKVMeta(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::GetKV(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::MGetKV(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::PrefixListKV(a) => s.serialize(self.handle(a).await?),
        }
    }

    pub(crate) async fn do_put(
        &self,
        db_name: String,
        table_name: String,
        parts: Streaming<FlightData>,
    ) -> common_exception::Result<AppendResult> {
        {
            // TODO:  Validates the schema of input stream:
            // The schema of `parts` should be a subset of
            // table's current schema (or following the evolution rules of table schema)
        }

        let appender = Appender::new(self.fs.clone());
        let parts = parts
            .take_while(|item| item.is_ok())
            .map(|item| item.unwrap());

        let res = appender
            .append_data(format!("{}/{}", &db_name, &table_name), Box::pin(parts))
            .await?;

        self.meta_node
            .append_data_parts(&db_name, &table_name, &res)
            .await;
        Ok(res)
    }

    pub async fn read_partition(
        &self,
        action: ReadAction,
    ) -> common_exception::Result<DoGetStream> {
        log::info!("entering read");
        let part_file = action.part.name;

        let plan = if let PlanNode::ReadSource(read_source_plan) = action.push_down {
            read_source_plan
        } else {
            return Err(ErrorCode::IllegalScanPlan("invalid PlanNode passed in"));
        };

        // before push_down is passed in, we returns all the columns
        let schema = plan.schema;
        let projection = (0..schema.fields().len()).collect::<Vec<_>>();

        // TODO expose a reader from fs
        let content = self.fs.read_all(&part_file).await?;
        let reader = Cursor::new(content);

        let reader =
            read::RecordReader::try_new(reader, Some(projection.to_vec()), None, None, None)?;

        // For simplicity, we do the conversion in-memory, to be optimized later
        // TODO consider using `parquet_table` and `stream_parquet`
        let write_opt = IpcWriteOptions::default();
        let flights =
            reader
                .into_iter()
                .map(|batch| {
                    batch.map(
                    |b| flight_data_from_arrow_batch(&b, &write_opt).1, /*dictionary ignored*/
                ).map_err(|arrow_err| Status::internal(arrow_err.to_string()))
                })
                .collect::<Vec<_>>();
        let stream = futures::stream::iter(flights);
        Ok(Box::pin(stream))
    }
}
