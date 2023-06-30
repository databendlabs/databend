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

use std::sync::Arc;
use arrow_array::RecordBatch;
use tracing::error;
use background_service::background_service::BackgroundServiceHandlerWrapper;
use common_exception::{ErrorCode, ToErrorCode};
use common_expression::{DataBlock, DataField, DataSchema, DataSchemaRefExt};
use common_expression::types::DataType;
use common_license::license::Feature;
use common_license::license_manager::get_license_manager;
use common_sql::Planner;
use common_exception::Result;
use crate::interpreters::InterpreterFactory;
use crate::procedures::OneBlockProcedure;
use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct SuggestedBackgroundTasksProcedure;

impl SuggestedBackgroundTasksProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(SuggestedBackgroundTasksProcedure {}.into_procedure())
    }
}

impl SuggestedBackgroundTasksProcedure {
    #[async_backtrace::framed]
    pub async fn do_execute_sql(ctx: Arc<QueryContext>, sql: String) -> Result<Option<RecordBatch>> {
        let mut planner = Planner::new(ctx.clone());
        let (plan, plan_extras) = planner.plan_sql(sql.as_str()).await?;
        ctx.attach_query_str(plan.to_string(), plan_extras.statement.to_mask_sql());
        let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let data_schema = interpreter.schema();
        let stream = interpreter.execute(ctx.clone()).await?;
        let blocks = stream.map(|v| v).collect::<Vec<_>>().await;

        let mut result = vec![];
        for block in blocks {
            match block {
                Ok(block) => {
                    result.push(block);
                }
                Err(e) => {
                    error!("execute sql error: {:?}", e);
                }
            }
        }
        if result.is_empty() {
            return Ok(None);
        }
        let record = DataBlock::concat(&result)?;
        let record = record
            .to_record_batch(data_schema.as_ref())
            .map_err(|e| ErrorCode::Internal(format!("{e:?}")))?;
        Ok(Some(record))
    }
}

#[async_trait::async_trait]
impl OneBlockProcedure for SuggestedBackgroundTasksProcedure {
    fn name(&self) -> &str {
        "SUGGESTED_BACKGROUND_TASKS"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default()
            .variadic_arguments(0, 1)
            .management_mode_required(false)
    }

    #[async_backtrace::framed]
    async fn all_data(&self, ctx: Arc<QueryContext>, _args: Vec<String>) -> Result<DataBlock> {
        let suggestions = BackgroundServiceHandlerWrapper::get_all_suggested_tasks().await?;
    }

    fn schema(&self) -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("license_issuer", DataType::String),
            DataField::new("license_type", DataType::String),
            DataField::new("organization", DataType::String),
            DataField::new("issued_at", DataType::Timestamp),
            DataField::new("expire_at", DataType::Timestamp),
            // formatted string calculate the available time from now to expiry of license
            DataField::new("available_time_until_expiry", DataType::String),
        ])
    }
}

impl SuggestedBackgroundTasksProcedure {
    fn to_block(&self) -> Result<DataBlock> {
        todo!()
    }
}
