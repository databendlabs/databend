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

use common_base::runtime::GlobalIORuntime;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataSchemaRef;
use common_sql::executor::AsyncSourcerPlan;
use common_sql::executor::Deduplicate;
use common_sql::executor::MutationAggregate;
use common_sql::executor::MutationKind;
use common_sql::executor::OnConflictField;
use common_sql::executor::PhysicalPlan;
use common_sql::executor::ReplaceInto;
use common_sql::executor::SelectCtx;
use common_sql::plans::CopyPlan;
use common_sql::plans::InsertInputSource;
use common_sql::plans::OptimizeTableAction;
use common_sql::plans::OptimizeTablePlan;
use common_sql::plans::Plan;
use common_sql::plans::Replace;
use common_storages_factory::Table;
use common_storages_fuse::FuseTable;
use storages_common_table_meta::meta::TableSnapshot;
use tracing::info;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::interpreter_copy::CopyInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::OptimizeTableInterpreter;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;

#[allow(dead_code)]
pub struct ReplaceInterpreter {
    ctx: Arc<QueryContext>,
    plan: Replace,
}

impl ReplaceInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: Replace) -> Result<InterpreterPtr> {
        Ok(Arc::new(ReplaceInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for ReplaceInterpreter {
    fn name(&self) -> &str {
        "ReplaceIntoInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        self.check_on_conflicts()?;
        let physical_plan = self.build_physical_plan().await?;
        let mut pipeline =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan, false)
                .await?;

        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;
        let has_cluster_key = !table.cluster_keys(self.ctx.clone()).is_empty();
        if !pipeline.main_pipeline.is_empty()
            && has_cluster_key
            && self.ctx.get_settings().get_enable_auto_reclustering()?
        {
            let ctx = self.ctx.clone();
            let catalog = self.plan.catalog.clone();
            let database = self.plan.database.to_string();
            let table = self.plan.table.to_string();
            pipeline.main_pipeline.set_on_finished(|err| {
                    if err.is_none() {
                        info!("execute replace into finished successfully. running table optimization job.");
                         match  GlobalIORuntime::instance().block_on({
                             async move {
                                 ctx.evict_table_from_cache(&catalog, &database, &table)?;
                                 let optimize_interpreter = OptimizeTableInterpreter::try_create(ctx.clone(),
                                 OptimizeTablePlan {
                                     catalog,
                                     database,
                                     table,
                                     action: OptimizeTableAction::CompactBlocks,
                                     limit: None,
                                 }
                                 )?;

                                 let mut build_res = optimize_interpreter.execute2().await?;

                                 if build_res.main_pipeline.is_empty() {
                                     return Ok(());
                                 }

                                 let settings = ctx.get_settings();
                                 let query_id = ctx.get_id();
                                 build_res.set_max_threads(settings.get_max_threads()? as usize);
                                 let settings = ExecutorSettings::try_create(&settings, query_id)?;

                                 if build_res.main_pipeline.is_complete_pipeline()? {
                                     let mut pipelines = build_res.sources_pipelines;
                                     pipelines.push(build_res.main_pipeline);

                                     let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;

                                     ctx.set_executor(complete_executor.get_inner())?;
                                     complete_executor.execute()?;
                                 }
                                 Ok(())
                             }
                         }) {
                            Ok(_) => {
                                info!("execute replace into finished successfully. table optimization job finished.");
                            }
                            Err(e) => { info!("execute replace into finished successfully. table optimization job failed. {:?}", e)}
                        }

                        return Ok(());
                    }
                    Ok(())
                });
        }
        Ok(pipeline)
    }
}

impl ReplaceInterpreter {
    async fn build_physical_plan(&self) -> Result<Box<PhysicalPlan>> {
        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;
        let schema = table.schema();
        let mut on_conflicts = Vec::with_capacity(plan.on_conflict_fields.len());
        for f in &plan.on_conflict_fields {
            let field_name = f.name();
            let (field_index, _) = match schema.column_with_name(field_name) {
                Some(idx) => idx,
                None => {
                    return Err(ErrorCode::Internal(
                        "not expected, on conflict field not found (after binding)",
                    ));
                }
            };
            on_conflicts.push(OnConflictField {
                table_field: f.clone(),
                field_index,
            })
        }
        let fuse_table =
            table
                .as_any()
                .downcast_ref::<FuseTable>()
                .ok_or(ErrorCode::Unimplemented(format!(
                    "table {}, engine type {}, does not support REPLACE INTO",
                    table.name(),
                    table.get_table_info().engine(),
                )))?;
        let table_info = fuse_table.get_table_info();
        let base_snapshot = fuse_table.read_table_snapshot().await?.unwrap_or_else(|| {
            Arc::new(TableSnapshot::new_empty_snapshot(schema.as_ref().clone()))
        });

        let table_is_empty = base_snapshot.segments.is_empty();
        let table_level_range_index = base_snapshot.summary.col_stats.clone();
        let max_threads = self.ctx.get_settings().get_max_threads()?;
        let segment_partition_num =
            std::cmp::min(base_snapshot.segments.len(), max_threads as usize);
        let (mut root, select_ctx) = self
            .connect_input_source(self.ctx.clone(), &self.plan.source, self.plan.schema())
            .await?;
        root = Box::new(PhysicalPlan::Deduplicate(Deduplicate {
            input: root,
            on_conflicts: on_conflicts.clone(),
            table_is_empty,
            table_info: table_info.clone(),
            catalog_name: plan.catalog.clone(),
            select_ctx,
            table_schema: plan.schema.clone(),
            table_level_range_index,
        }));
        root = Box::new(PhysicalPlan::ReplaceInto(ReplaceInto {
            input: root,
            segment_partition_num,
            block_thresholds: fuse_table.get_block_thresholds(),
            table_info: table_info.clone(),
            catalog_name: plan.catalog.clone(),
            on_conflicts,
            snapshot: (*base_snapshot).clone(),
        }));
        root = Box::new(PhysicalPlan::MutationAggregate(Box::new(
            MutationAggregate {
                input: root,
                snapshot: (*base_snapshot).clone(),
                table_info: table_info.clone(),
                catalog_name: plan.catalog.clone(),
                mutation_kind: MutationKind::Replace,
            },
        )));
        Ok(root)
    }
    fn check_on_conflicts(&self) -> Result<()> {
        if self.plan.on_conflict_fields.is_empty() {
            Err(ErrorCode::BadArguments(
                "at least one column must be specified in the replace into .. on [conflict] statement",
            ))
        } else {
            Ok(())
        }
    }
    #[async_backtrace::framed]
    async fn connect_input_source<'a>(
        &'a self,
        ctx: Arc<QueryContext>,
        source: &'a InsertInputSource,
        schema: DataSchemaRef,
    ) -> Result<(Box<PhysicalPlan>, Option<SelectCtx>)> {
        match source {
            InsertInputSource::Values(data) => self
                .connect_value_source(schema.clone(), data)
                .map(|x| (x, None)),

            InsertInputSource::SelectPlan(plan) => {
                self.connect_query_plan_source(ctx.clone(), plan).await
            }
            InsertInputSource::Stage(plan) => match *plan.clone() {
                Plan::Copy(copy_plan) => match copy_plan.as_ref() {
                    CopyPlan::IntoTable(copy_into_table_plan) => {
                        let interpreter =
                            CopyInterpreter::try_create(ctx.clone(), *copy_plan.clone())?;
                        interpreter
                            .build_physical_plan(copy_into_table_plan)
                            .await
                            .map(|x| (Box::new(x.0), None))
                    }
                    _ => unreachable!("plan in InsertInputSource::Stag must be CopyIntoTable"),
                },
                _ => unreachable!("plan in InsertInputSource::Stag must be Copy"),
            },
            _ => Err(ErrorCode::Unimplemented(
                "input source other than literal VALUES and sub queries are NOT supported yet.",
            )),
        }
    }

    fn connect_value_source(
        &self,
        schema: DataSchemaRef,
        value_data: &str,
    ) -> Result<Box<PhysicalPlan>> {
        Ok(Box::new(PhysicalPlan::AsyncSourcer(AsyncSourcerPlan {
            value_data: value_data.to_string(),
            schema,
        })))
    }

    #[async_backtrace::framed]
    async fn connect_query_plan_source<'a>(
        &'a self,
        ctx: Arc<QueryContext>,
        query_plan: &Plan,
    ) -> Result<(Box<PhysicalPlan>, Option<SelectCtx>)> {
        let (s_expr, metadata, bind_context, formatted_ast) = match query_plan {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                formatted_ast,
                ..
            } => (s_expr, metadata, bind_context, formatted_ast),
            v => unreachable!("Input plan must be Query, but it's {}", v),
        };

        let select_interpreter = SelectInterpreter::try_create(
            ctx.clone(),
            *(bind_context.clone()),
            *s_expr.clone(),
            metadata.clone(),
            formatted_ast.clone(),
            false,
        )?;

        let physical_plan = select_interpreter
            .build_physical_plan()
            .await
            .map(Box::new)?;
        let select_ctx = SelectCtx {
            select_column_bindings: bind_context.columns.clone(),
            select_schema: query_plan.schema(),
        };
        Ok((physical_plan, Some(select_ctx)))
    }
}
