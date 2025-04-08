// Copyright 2021 Datafuse Labs
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

use std::ops::Deref;
use std::pin::pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::LazyLock;

use arrow_flight::FlightData;
use arrow_flight::SchemaAsIpc;
use arrow_ipc::writer;
use arrow_ipc::MessageBuilder;
use arrow_ipc::MessageHeader;
use arrow_ipc::MetadataVersion;
use arrow_schema::Schema as ArrowSchema;
use async_stream::stream;
use bytes::Bytes;
use databend_common_base::base::tokio;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_sql::get_query_kind;
use databend_common_sql::plans::Plan;
use databend_common_sql::PlanExtras;
use databend_common_sql::Planner;
use databend_common_storages_fuse::TableContext;
use futures::Stream;
use futures::StreamExt;
use prost::bytes;
use serde::Deserialize;
use serde::Serialize;
use tonic::Status;

use super::status;
use super::DoGetStream;
use super::FlightSqlServiceImpl;
use crate::interpreters::InterpreterFactory;
use crate::sessions::QueryContext;
use crate::sessions::Session;

/// A app_metakey which indicates the data is a progress type
static H_PROGRESS: u8 = 0x01;

/// The generated app metadata for our progress.
static APP_METADATA_PROGRESS: LazyLock<Bytes> = LazyLock::new(|| Bytes::from(vec![H_PROGRESS]));

/// The data header for our progress.
///
/// This build process is inspired from [arrow_ipc::writer::IpcDataGenerator](https://docs.rs/arrow-ipc/51.0.0/arrow_ipc/writer/struct.IpcDataGenerator.html#method.schema_to_bytes)
static DATA_HEADER_PROGRESS: LazyLock<Bytes> = LazyLock::new(|| {
    let mut fbb = flatbuffers::FlatBufferBuilder::new();

    let mut builder = MessageBuilder::new(&mut fbb);
    // Use the same version with arrow_ipc.
    builder.add_version(MetadataVersion::V5);
    // Use NONE as the header type.
    builder.add_header_type(MessageHeader::NONE);
    // We don't have other data to write in this message, just finish.
    let data = builder.finish();

    // finish the flat buffers.
    fbb.finish(data, None);

    Bytes::copy_from_slice(fbb.finished_data())
});

impl FlightSqlServiceImpl {
    pub(crate) fn schema_to_flight_data(data_schema: DataSchema) -> FlightData {
        let arrow_schema = ArrowSchema::from(&data_schema);
        let options = writer::IpcWriteOptions::default();
        SchemaAsIpc::new(&arrow_schema, &options).into()
    }

    pub fn block_to_flight_data_stream(
        block: DataBlock,
        data_schema: &DataSchema,
    ) -> impl Stream<Item = Result<FlightData>> + '_ {
        stream! {
            let remain_size = 4 * 1024 * 1024;
            let options = writer::IpcWriteOptions::default();
            let data_gen = writer::IpcDataGenerator::default();

            if block.memory_size() > remain_size {
                let row = block.num_rows() / ((block.memory_size() / remain_size) + 1);
                for block in block.split_by_rows_no_tail(row) {
                    yield Self::block_to_flight_data(block, data_schema, &options, &data_gen);
                }
            } else {
                yield Self::block_to_flight_data(block, data_schema, &options, &data_gen);
            }
        }
    }

    fn block_to_flight_data(
        block: DataBlock,
        data_schema: &DataSchema,
        options: &writer::IpcWriteOptions,
        data_gen: &writer::IpcDataGenerator,
    ) -> Result<FlightData> {
        let batch = block
            .to_record_batch_with_dataschema(data_schema)
            .map_err(|e| ErrorCode::Internal(format!("{e:?}")))?;
        let mut dictionary_tracker = writer::DictionaryTracker::new(false);

        let (_encoded_dictionaries, encoded_batch) = data_gen
            .encoded_batch(&batch, &mut dictionary_tracker, options)
            .map_err(|e| ErrorCode::Internal(format!("{e:?}")))?;

        Ok(encoded_batch.into())
    }

    fn progress_to_flight_data(progress: &ProgressValue) -> Result<FlightData> {
        let progress = serde_json::to_vec(&progress)
            .map_err(|e| ErrorCode::Internal(format!("encode progress into json failed: {e:?}")))?;

        Ok(FlightData::new()
            .with_app_metadata(APP_METADATA_PROGRESS.deref().clone())
            .with_data_header(DATA_HEADER_PROGRESS.deref().clone())
            .with_data_body(Bytes::from(progress)))
    }

    #[async_backtrace::framed]
    pub async fn plan_sql(
        &self,
        session: &Arc<Session>,
        query: &str,
    ) -> Result<(Plan, PlanExtras)> {
        let context = session
            .create_query_context()
            .await
            .map_err(|e| status!("Could not create_query_context", e))?;

        let mut planner = Planner::new(context.clone());
        planner.plan_sql(query).await
    }

    #[async_backtrace::framed]
    pub(super) async fn execute_update(
        &self,
        session: Arc<Session>,
        plan: &Plan,
        plan_extras: &PlanExtras,
    ) -> Result<i64> {
        let context = session
            .create_query_context()
            .await
            .map_err(|e| status!("Could not create_query_context", e))?;

        context.attach_query_str(
            get_query_kind(&plan_extras.statement),
            plan_extras.statement.to_mask_sql(),
        );
        let interpreter = InterpreterFactory::get(context.clone(), plan).await?;

        let mut blocks = interpreter.execute(context.clone()).await?;
        while let Some(block) = blocks.next().await {
            block?;
        }

        let affected_rows = context.get_write_progress_value().rows;
        Ok(affected_rows as i64)
    }

    pub async fn execute_query(
        &self,
        session: Arc<Session>,
        plan: &Plan,
        plan_extras: &PlanExtras,
    ) -> Result<DoGetStream> {
        let is_native_client = session.get_status().read().is_native_client;

        let context = session
            .create_query_context()
            .await
            .map_err(|e| status!("Could not create_query_context", e))?;

        context.attach_query_str(
            get_query_kind(&plan_extras.statement),
            plan_extras.statement.to_mask_sql(),
        );
        let interpreter = InterpreterFactory::get(context.clone(), plan).await?;

        let data_schema = plan.schema();
        let data_stream = interpreter.execute(context.clone()).await?;

        let is_finished = Arc::new(AtomicBool::new(false));
        let is_finished_clone = is_finished.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let _ = sender
            .send(Ok(Self::schema_to_flight_data((*data_schema).clone())))
            .await;

        let s1 = sender.clone();
        databend_common_base::runtime::spawn(async move {
            let mut data_stream = data_stream;

            while let Some(block) = data_stream.next().await {
                match block {
                    Ok(block) => {
                        let mut stream = pin!(FlightSqlServiceImpl::block_to_flight_data_stream(
                            block,
                            &data_schema,
                        ));
                        while let Some(res) = stream.next().await {
                            let _ = s1
                                .send(res.map_err(|err| status!("Could not convert batches", err)))
                                .await;
                        }
                    }
                    Err(err) => {
                        let _ = s1
                            .send(Err(status!("Could not convert batches", err)))
                            .await;
                        break;
                    }
                }
            }
            is_finished_clone.store(true, Ordering::SeqCst);
        });

        if is_native_client {
            databend_common_base::runtime::spawn(async move {
                let total_scan_value = context.get_total_scan_value();
                let mut current_scan_value = context.get_scan_progress_value();

                const TICK_MS: usize = 20;
                const MAX_WAIT_MS: usize = 500;
                const MIN_PERCENT_PROGRESS: usize = 3;

                let mut interval =
                    tokio::time::interval(tokio::time::Duration::from_millis(TICK_MS as u64));
                let mut wait_times = 0;

                let mut get_progress =
                    |context: &Arc<QueryContext>, is_final: bool| -> Option<FlightData> {
                        let progress = context.get_scan_progress_value();
                        // only send progress when the increment progress is more than 3% or MAX_WAIT_MS elapsed
                        if !is_final
                            && progress.bytes - current_scan_value.bytes
                                < total_scan_value.bytes * MIN_PERCENT_PROGRESS / 100
                            && wait_times < MAX_WAIT_MS / TICK_MS
                        {
                            wait_times += 1;
                            return None;
                        }

                        wait_times = 0;
                        current_scan_value = progress;

                        let mut progress = ProgressValue {
                            total_rows: total_scan_value.rows,
                            total_bytes: total_scan_value.bytes,

                            read_rows: current_scan_value.rows,
                            read_bytes: current_scan_value.bytes,
                            write_rows: 0,
                            write_bytes: 0,
                        };

                        if is_final {
                            let write_progress = context.get_write_progress_value();
                            progress.write_rows = write_progress.rows;
                            progress.write_bytes = write_progress.bytes;
                        }

                        Some(Self::progress_to_flight_data(&progress).unwrap())
                    };

                while !is_finished.load(Ordering::SeqCst) {
                    interval.tick().await;
                    if let Some(progress_flight_data) = get_progress(&context, false) {
                        let _ = sender.send(Ok(progress_flight_data)).await;
                    }
                }

                if let Some(progress_flight_data) = get_progress(&context, true) {
                    let _ = sender.send(Ok(progress_flight_data)).await;
                }
            });
        }

        fn receiver_to_stream<T>(
            receiver: tokio::sync::mpsc::Receiver<T>,
        ) -> impl Stream<Item = T> {
            futures::stream::unfold(receiver, |mut receiver| async {
                receiver.recv().await.map(|value| (value, receiver))
            })
        }

        let st = receiver_to_stream(receiver);
        Ok(Box::pin(st))
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct ProgressValue {
    pub total_rows: usize,
    pub total_bytes: usize,

    pub read_rows: usize,
    pub read_bytes: usize,

    pub write_rows: usize,
    pub write_bytes: usize,
}
