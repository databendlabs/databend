// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::pin::Pin;
use std::sync::Arc;

use common_arrow::arrow::ipc::writer::IpcWriteOptions;
use common_arrow::arrow_flight::utils::flight_data_from_arrow_batch;
use common_arrow::arrow_flight::FlightData;
use common_arrow::parquet::arrow::ArrowReader;
use common_arrow::parquet::arrow::ParquetFileArrowReader;
use common_arrow::parquet::file::reader::SerializedFileReader;
use common_arrow::parquet::file::serialized_reader::SliceableCursor;
use common_exception::ErrorCode;
use common_flights::RequestFor;
use common_flights::StoreDoAction;
use common_infallible::Mutex;
use common_planners::PlanNode;
use common_runtime::tokio::sync::mpsc::Sender;
use common_store_api::AppendResult;
use common_store_api::ReadAction;
use futures::Stream;
use serde::Serialize;
use tokio_stream::StreamExt;
use tonic::Status;
use tonic::Streaming;

use crate::data_part::appender::Appender;
use crate::engine::MemEngine;
use crate::fs::FileSystem;
use crate::meta_service::MetaNode;

pub trait ReplySerializer {
    type Output;
    fn serialize<T>(&self, v: T) -> Result<Self::Output, ErrorCode>
    where T: Serialize;
}

pub struct ActionHandler {
    pub(crate) meta: Arc<Mutex<MemEngine>>,
    /// The raft-based meta data entry.
    /// In our design meta serves for both the distributed file system and the catalog storage such as db,tabel etc.
    /// Thus in case the `fs` is a Dfs impl, `meta_node` is just a reference to the `Dfs.meta_node`.
    /// TODO(xp): turn on dead_code warning when we finished action handler unit test.
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
        ActionHandler {
            meta: MemEngine::create(),
            meta_node,
            fs,
        }
    }

    /// Handle pull-file request, which is used internally for replicating data copies.
    /// In FuseStore impl there is no internal file id etc, thus replication use the same `key` in communication with FuseQuery as in internal replication.
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
        //
        // Technically we can eliminate these kind of duplications by using proc-macros, or eliminate
        // parts of them by introducing another func like this:
        //#  async fn invoke<S, O, A, R>(&self, a: A, s: S) -> common_exception::Result<O>
        //#      where
        //#          A: Serialize + RequestFor<Reply = R>,
        //#          S: ReplySerializer<Output = O>,
        //#          Self: RequestHandler<A>,
        //#          R: Serialize,
        //#          <S as ReplySerializer>::Error: Into<ErrorCode>,
        //#  {
        //#      let r = self.handle(a).await?;
        //#      let v = s.serialize(r).map_err(Into::into)?;
        //#      Ok(v)
        //#  }
        //
        // But, that may be too much, IDEs like "clion" will be confused (and unable to jump around)
        //
        // New suggestions/ideas are welcome.

        match action {
            // database
            StoreDoAction::CreateDatabase(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::GetDatabase(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::DropDatabase(a) => s.serialize(self.handle(a).await?),

            // table
            StoreDoAction::CreateTable(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::DropTable(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::GetTable(a) => s.serialize(self.handle(a).await?),

            // part
            StoreDoAction::ReadPlan(a) => s.serialize(self.handle(a).await?),

            // general-purpose kv
            StoreDoAction::UpsertKV(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::GetKV(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::MGetKV(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::PrefixListKV(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::DeleteKV(a) => s.serialize(self.handle(a).await?),
            StoreDoAction::UpdateKV(a) => s.serialize(self.handle(a).await?),
        }
    }

    pub(crate) async fn do_put(
        &self,
        db_name: String,
        table_name: String,
        parts: Streaming<FlightData>,
    ) -> common_exception::Result<AppendResult> {
        {
            let mut meta = self.meta.lock();
            let _tbl_meta = meta.get_table(db_name.clone(), table_name.clone())?;

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

        let mut meta = self.meta.lock();
        meta.append_data_parts(&db_name, &table_name, &res);
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

        let content = self.fs.read_all(&part_file).await?;
        let cursor = SliceableCursor::new(content);

        let file_reader = SerializedFileReader::new(cursor)
            .map_err(|pe| ErrorCode::ReadFileError(format!("parquet error: {}", pe.to_string())))?;
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

        // before push_down is passed in, we returns all the columns
        let schema = plan.schema;
        let projection = (0..schema.fields().len()).collect::<Vec<_>>();

        // TODO config
        let batch_size = 2048;

        let batch_reader = arrow_reader
            .get_record_reader_by_columns(projection, batch_size)
            .map_err(|pe| ErrorCode::ReadFileError(format!("parquet error: {}", pe.to_string())))?;

        // For simplicity, we do the conversion in-memory, to be optimized later
        // TODO consider using `parquet_table` and `stream_parquet`
        let write_opt = IpcWriteOptions::default();
        let flights =
            batch_reader
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
