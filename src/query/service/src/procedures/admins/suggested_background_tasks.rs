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
use background_service::Suggestion;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::StringType;
use common_expression::types::VariantType;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRefExt;
use common_expression::FromData;
use common_expression::FromOptData;
use common_license::license::Feature;
use common_license::license_manager::get_license_manager;
use common_sql::Planner;
use log::error;
use log::info;
use tokio_stream::StreamExt;

use crate::interpreters::InterpreterFactory;
use crate::procedures::OneBlockProcedure;
use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::query_ctx::Origin;
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
    pub async fn do_execute_sql(
        ctx: Arc<QueryContext>,
        sql: String,
    ) -> Result<Option<RecordBatch>> {
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
        let license_mgr = get_license_manager();

        license_mgr.manager.check_enterprise_enabled(
            &ctx.get_settings(),
            ctx.get_tenant(),
            Feature::BackgroundService,
        )?;
        let suggestions = Self::all_suggestions(ctx).await?;
        self.to_block(suggestions)
    }

    fn schema(&self) -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("type", DataType::String),
            DataField::new("database_name", DataType::String),
            DataField::new("table_name", DataType::String),
            DataField::new("table_statistics", DataType::Variant),
            DataField::new("should_do_segment_compact", DataType::Boolean),
            DataField::new("should_do_compact", DataType::Boolean),
        ])
    }
}

impl SuggestedBackgroundTasksProcedure {
    #[async_backtrace::framed]
    pub async fn all_suggestions(ctx: Arc<QueryContext>) -> Result<Vec<Suggestion>> {
        let ctx = ctx.clone();
        ctx.set_origin(Origin::BuiltInProcedure);
        info!(
            background = true,
            tenant = ctx.get_tenant();
            "list all lsuggestions"
        );
        Self::get_suggested_compaction_tasks(ctx).await
    }

    fn to_block(&self, suggestions: Vec<Suggestion>) -> Result<DataBlock> {
        let mut suggestion_type = vec![];
        let mut should_do_segment_compact = vec![];
        let mut should_do_compact = vec![];
        let mut database_name = vec![];
        let mut table_names = vec![];
        let mut table_statistics = vec![];
        for suggestion in suggestions {
            match suggestion {
                Suggestion::Compaction {
                    need_compact_segment,
                    need_compact_block,
                    db_name,
                    table_name,
                    table_stats,
                    ..
                } => {
                    suggestion_type.push("compaction".to_string().into_bytes().to_vec());
                    should_do_segment_compact.push(Some(need_compact_segment));
                    should_do_compact.push(Some(need_compact_block));
                    database_name.push(db_name.into_bytes().to_vec());
                    table_names.push(table_name.into_bytes().to_vec());
                    table_statistics.push(serde_json::to_vec(&table_stats).unwrap());
                }
            }
        }
        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(suggestion_type),
            StringType::from_data(database_name),
            StringType::from_data(table_names),
            VariantType::from_data(table_statistics),
            BooleanType::from_opt_data(should_do_segment_compact),
            BooleanType::from_opt_data(should_do_compact),
        ]))
    }
}
