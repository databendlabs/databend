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

use chrono::Duration;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::FromData;
use databend_common_expression::SendableDataBlockStream;
use databend_common_meta_app::schema::Constraint;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::binder::ConstraintExprBinder;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::plans::Insert;
use databend_common_sql::plans::InsertInputSource;
use databend_common_sql::plans::InsertValue;
use databend_common_sql::plans::Plan;
use databend_common_sql::NameResolutionContext;
use databend_common_storages_fuse::operations::TransformConstraintVerify;
use databend_common_storages_stage::build_streaming_load_pipeline;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use log::info;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::dml_build_update_stream_req;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::physical_plans::DistributedInsertSelect;
use crate::physical_plans::Exchange;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::PhysicalPlanCast;
use crate::physical_plans::PhysicalPlanMeta;
use crate::pipelines::processors::transforms::TransformAddConstColumns;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::RawValueSource;
use crate::pipelines::ValueSource;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::stream::DataBlockStream;

pub struct InsertInterpreter {
    ctx: Arc<QueryContext>,
    plan: Insert,
}

impl InsertInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: Insert) -> Result<InterpreterPtr> {
        Ok(Arc::new(InsertInterpreter { ctx, plan }))
    }

    fn check_schema_cast(&self, plan: &Plan) -> Result<bool> {
        let output_schema = &self.plan.schema;
        let select_schema = plan.schema();

        // validate schema
        if select_schema.fields().len() != output_schema.fields().len() {
            return Err(ErrorCode::BadArguments(format!(
                "Fields in select statement is not equal with expected, select fields: {}, insert fields: {}",
                select_schema.fields().len(),
                output_schema.fields().len(),
            )));
        }

        // check if cast needed
        let cast_needed = select_schema.as_ref() != &DataSchema::from(output_schema.as_ref());
        Ok(cast_needed)
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertInterpreter {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }
        let table = if let Some(table_info) = &self.plan.table_info {
            // if table_info is provided, we should instantiated table with it.
            self.ctx
                .get_catalog(&self.plan.catalog)
                .await?
                .get_table_by_info(table_info)?
        } else {
            self.ctx
                .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
                .await?
        };

        let mut table_constraints = Vec::new();
        // check mutability
        table.check_mutable()?;
        let table_meta_timestamps = if table.engine() == "FUSE" {
            let fuse_table =
                databend_common_storages_fuse::FuseTable::try_from_table(table.as_ref())?;

            // bind constraints
            let dest_schema = self.plan.dest_schema();
            let mut expr_binder = ConstraintExprBinder::try_new(self.ctx.clone(), dest_schema)?;
            for (constraint_name, constraint) in fuse_table.get_table_info().meta.constraints.iter()
            {
                match &constraint {
                    Constraint::Check(expr) => {
                        let constraint_expr = expr_binder.get_expr(constraint_name, expr)?;
                        table_constraints.push((constraint_name.clone(), constraint_expr))
                    }
                }
            }

            let snapshot = fuse_table.read_table_snapshot().await?;
            self.ctx
                .get_table_meta_timestamps(table.as_ref(), snapshot)?
        } else {
            // For non-fuse table, the table meta timestamps does not matter,
            // just passes a placeholder value here
            TableMetaTimestamps::new(None, Duration::hours(1))
        };

        let mut build_res = PipelineBuildResult::create();

        match &self.plan.source {
            InsertInputSource::Stage(_) => {
                unreachable!()
            }
            InsertInputSource::Values(InsertValue::Values { rows }) => {
                build_res.main_pipeline.add_source(
                    |output| {
                        let inner = ValueSource::new(rows.clone(), self.plan.dest_schema());
                        AsyncSourcer::create(self.ctx.clone(), output, inner)
                    },
                    1,
                )?;
            }
            InsertInputSource::Values(InsertValue::RawValues { data, start }) => {
                build_res.main_pipeline.add_source(
                    |output| {
                        let name_resolution_ctx = NameResolutionContext {
                            deny_column_reference: true,
                            ..Default::default()
                        };
                        let inner = RawValueSource::new(
                            data.to_string(),
                            self.ctx.clone(),
                            name_resolution_ctx,
                            self.plan.dest_schema(),
                            *start,
                        );
                        AsyncSourcer::create(self.ctx.clone(), output, inner)
                    },
                    1,
                )?;
            }
            InsertInputSource::SelectPlan(plan) => {
                let table1 = table.clone();
                let (select_plan, select_column_bindings, metadata) = match plan.as_ref() {
                    Plan::Query {
                        s_expr,
                        metadata,
                        bind_context,
                        ..
                    } => {
                        let mut builder1 =
                            PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
                        (
                            builder1.build(s_expr, bind_context.column_set()).await?,
                            bind_context.columns.clone(),
                            metadata,
                        )
                    }
                    _ => unreachable!(),
                };

                let explain_plan = {
                    let metadata = metadata.read();
                    select_plan
                        .format(&metadata, Default::default())?
                        .format_pretty()?
                };

                info!("Insert select plan: \n{}", explain_plan);

                let update_stream_meta = dml_build_update_stream_req(self.ctx.clone()).await?;

                // here we remove the last exchange merge plan to trigger distribute insert
                let mut insert_select_plan = {
                    if table.support_distributed_insert()
                        && let Some(exchange) = Exchange::from_physical_plan(&select_plan)
                    {
                        // insert can be dispatched to different nodes if table support_distributed_insert
                        let input = exchange.input.clone();
                        exchange.derive(vec![PhysicalPlan::new(DistributedInsertSelect {
                            input,
                            table_info: table1.get_table_info().clone(),
                            select_schema: plan.schema(),
                            select_column_bindings,
                            insert_schema: self.plan.dest_schema(),
                            cast_needed: self.check_schema_cast(plan)?,
                            table_meta_timestamps,
                            meta: PhysicalPlanMeta::new("DistributedInsertSelect"),
                        })])
                    } else {
                        // insert should wait until all nodes finished
                        PhysicalPlan::new(DistributedInsertSelect {
                            // TODO: we reuse the id of other plan here,
                            // which is not correct. We should generate a new id for insert.
                            input: select_plan,
                            table_info: table1.get_table_info().clone(),
                            select_schema: plan.schema(),
                            select_column_bindings,
                            insert_schema: self.plan.dest_schema(),
                            cast_needed: self.check_schema_cast(plan)?,
                            table_meta_timestamps,
                            meta: PhysicalPlanMeta::new("DistributedInsertSelect"),
                        })
                    }
                };

                insert_select_plan.adjust_plan_id(&mut 0);
                let mut build_res =
                    build_query_pipeline_without_render_result_set(&self.ctx, &insert_select_plan)
                        .await?;

                table.commit_insertion(
                    self.ctx.clone(),
                    &mut build_res.main_pipeline,
                    None,
                    update_stream_meta,
                    self.plan.overwrite,
                    None,
                    unsafe { self.ctx.get_settings().get_deduplicate_label()? },
                    table_meta_timestamps,
                )?;

                //  Execute the hook operator.
                {
                    let hook_operator = HookOperator::create(
                        self.ctx.clone(),
                        self.plan.catalog.clone(),
                        self.plan.database.clone(),
                        self.plan.table.clone(),
                        MutationKind::Insert,
                        LockTableOption::LockNoRetry,
                    );
                    hook_operator.execute(&mut build_res.main_pipeline).await;
                }

                return Ok(build_res);
            }
            InsertInputSource::StreamingLoad(plan) => {
                build_streaming_load_pipeline(
                    self.ctx.clone(),
                    &mut build_res.main_pipeline,
                    &plan.file_format,
                    plan.receiver.clone(),
                    plan.required_source_schema.clone(),
                    plan.default_exprs.clone(),
                    plan.block_thresholds,
                )?;
                if !plan.values_consts.is_empty() {
                    let input_schema = Arc::new(DataSchema::from(&plan.required_source_schema));
                    build_res.main_pipeline.try_add_transformer(|| {
                        TransformAddConstColumns::try_new(
                            self.ctx.clone(),
                            input_schema.clone(),
                            plan.required_values_schema.clone(),
                            plan.values_consts.clone(),
                        )
                    })?;
                }
            }
        };
        build_res
            .main_pipeline
            .try_add_async_accumulating_transformer(|| {
                Ok(TransformConstraintVerify::new(
                    table_constraints.clone(),
                    self.ctx.get_function_context()?,
                    table.name().to_string(),
                ))
            })?;

        PipelineBuilder::build_append2table_with_commit_pipeline(
            self.ctx.clone(),
            &mut build_res.main_pipeline,
            table.clone(),
            self.plan.dest_schema(),
            None,
            vec![],
            self.plan.overwrite,
            unsafe { self.ctx.get_settings().get_deduplicate_label()? },
            table_meta_timestamps,
        )?;

        //  Execute the hook operator.
        {
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                self.plan.catalog.clone(),
                self.plan.database.clone(),
                self.plan.table.clone(),
                MutationKind::Insert,
                LockTableOption::LockNoRetry,
            );
            hook_operator.execute(&mut build_res.main_pipeline).await;
        }

        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let binding = self.ctx.get_mutation_status();
        let status = binding.read();
        let blocks = vec![DataBlock::new_from_columns(vec![UInt64Type::from_data(
            vec![status.insert_rows],
        )])];
        Ok(Box::pin(DataBlockStream::create(None, blocks)))
    }
}
