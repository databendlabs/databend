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

use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::FlightData;
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
    pub(super) fn block_to_flight_data(
        block: DataBlock,
        data_schema: &DataSchema,
    ) -> Result<Vec<FlightData>> {
        let batch = block
            .to_record_batch(data_schema)
            .map_err(|e| ErrorCode::Internal(format!("{e:?}")))?;
        let schema = (*batch.schema()).clone();
        let batches = vec![batch];
        batches_to_flight_data(schema, batches).map_err(|e| ErrorCode::Internal(format!("{e:?}")))
    }

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

        context.attach_query_str(plan.to_string(), plan_extras.stament.to_mask_sql());
        let interpreter = InterpreterFactory::get(context.clone(), plan).await?;

        let mut blocks = interpreter.execute(context.clone()).await?;
        while let Some(block) = blocks.next().await {
            block?;
        }

        let affected_rows = context.get_write_progress_value().rows;
        Ok(affected_rows as i64)
    }

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

        context.attach_query_str(plan.to_string(), plan_extras.stament.to_mask_sql());
        let interpreter = InterpreterFactory::get(context.clone(), plan).await?;
        let data_schema = interpreter.schema();

        let mut data_stream = interpreter.execute(context.clone()).await?;

        let stream = stream! {
            while let Some(block) = data_stream.next().await {
                match block {
                    Ok(block) => {
                        match Self::block_to_flight_data(block, &data_schema) {
                            Ok(data) => {
                                for d in data {
                                   yield Ok(d)
                                }
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
