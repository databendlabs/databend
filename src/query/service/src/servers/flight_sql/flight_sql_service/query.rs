// Copyright 2023 Datafuse Labs.
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

use std::sync::Arc;

use arrow_flight::FlightData;
use arrow_flight::SchemaAsIpc;
use arrow_ipc::writer;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::Schema as ArrowSchema;
use async_stream::stream;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_sql::plans::Plan;
use common_sql::PlanExtras;
use common_sql::Planner;
use common_storages_fuse::TableContext;
use futures_util::StreamExt;
use tonic::Status;

use super::status;
use super::DoGetStream;
use super::FlightSqlServiceImpl;
use crate::interpreters::InterpreterFactory;
use crate::sessions::Session;

impl FlightSqlServiceImpl {
    pub(crate) fn schema_to_flight_data(data_schema: DataSchema) -> FlightData {
        let arrow_schema = ArrowSchema::from(&data_schema);
        let options = IpcWriteOptions::default();
        SchemaAsIpc::new(&arrow_schema, &options).into()
    }

    pub fn block_to_flight_data(block: DataBlock, data_schema: &DataSchema) -> Result<FlightData> {
        let batch = block
            .to_record_batch(data_schema)
            .map_err(|e| ErrorCode::Internal(format!("{e:?}")))?;
        let options = IpcWriteOptions::default();
        let data_gen = writer::IpcDataGenerator::default();
        let mut dictionary_tracker = writer::DictionaryTracker::new(false);

        let (_encoded_dictionaries, encoded_batch) = data_gen
            .encoded_batch(&batch, &mut dictionary_tracker, &options)
            .map_err(|e| ErrorCode::Internal(format!("{e:?}")))?;

        Ok(encoded_batch.into())
    }

    #[async_backtrace::framed]
    pub(super) async fn plan_sql(
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

        context.attach_query_str(plan.to_string(), plan_extras.statement.to_mask_sql());
        let interpreter = InterpreterFactory::get(context.clone(), plan).await?;

        let mut blocks = interpreter.execute(context.clone()).await?;
        while let Some(block) = blocks.next().await {
            block?;
        }

        let affected_rows = context.get_write_progress_value().rows;
        Ok(affected_rows as i64)
    }

    #[async_backtrace::framed]
    pub(super) async fn execute_query(
        &self,
        session: Arc<Session>,
        plan: &Plan,
        plan_extras: &PlanExtras,
    ) -> Result<DoGetStream> {
        let context = session
            .create_query_context()
            .await
            .map_err(|e| status!("Could not create_query_context", e))?;

        context.attach_query_str(plan.to_string(), plan_extras.statement.to_mask_sql());
        let interpreter = InterpreterFactory::get(context.clone(), plan).await?;
        let data_schema = interpreter.schema();
        let schema_flight_data = Self::schema_to_flight_data((*data_schema).clone());

        let mut data_stream = interpreter.execute(context.clone()).await?;

        let stream = stream! {
            yield Ok(schema_flight_data);
            while let Some(block) = data_stream.next().await {
                match block {
                    Ok(block) => {
                        match Self::block_to_flight_data(block, &data_schema) {
                            Ok(flight_data) => {
                               yield Ok(flight_data)
                            }
                            Err(err) => {
                                yield Err(status!("Could not convert batches", err))
                            }
                        }
                    }
                    Err(err) => {
                        yield Err(status!("Could not convert batches", err))
                    }
                };
            }

            // to hold session ref until stream is all consumed
            let _ = session.get_id();
        };

        Ok(Box::pin(stream))
    }
}
