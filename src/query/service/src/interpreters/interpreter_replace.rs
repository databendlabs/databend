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
use std::time::Instant;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataSchemaRef;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::principal::StageInfo;
use common_sql::executor::cast_expr_to_non_null_boolean;
use common_sql::executor::AsyncSourcerPlan;
use common_sql::executor::CommitSink;
use common_sql::executor::Deduplicate;
use common_sql::executor::Exchange;
use common_sql::executor::MutationKind;
use common_sql::executor::OnConflictField;
use common_sql::executor::PhysicalPlan;
use common_sql::executor::ReplaceInto;
use common_sql::executor::SelectCtx;
use common_sql::plans::CopyPlan;
use common_sql::plans::InsertInputSource;
use common_sql::plans::Plan;
use common_sql::plans::Replace;
use common_sql::BindContext;
use common_sql::Metadata;
use common_sql::NameResolutionContext;
use common_sql::ScalarBinder;
use common_storage::StageFileInfo;
use common_storages_factory::Table;
use common_storages_fuse::FuseTable;
use parking_lot::RwLock;
use storages_common_table_meta::meta::TableSnapshot;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::hook_compact;
use crate::interpreters::common::CompactHookTraceCtx;
use crate::interpreters::common::CompactTargetTableDescription;
use crate::interpreters::interpreter_copy::CopyInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::builders::set_copy_on_finished;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;

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
        let start = Instant::now();

        // replace
        let (physical_plan, purge_info) = self.build_physical_plan().await?;
        let mut pipeline =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan, false)
                .await?;

        // purge
        if let Some((files, stage_info)) = purge_info {
            set_copy_on_finished(
                self.ctx.clone(),
                files,
                stage_info.copy_options.purge,
                stage_info,
                &mut pipeline.main_pipeline,
            )?;
        }

        // hook compact
        let compact_target = CompactTargetTableDescription {
            catalog: self.plan.catalog.clone(),
            database: self.plan.database.clone(),
            table: self.plan.table.clone(),
        };

        let compact_hook_trace_ctx = CompactHookTraceCtx {
            start,
            operation_name: "replace_into".to_owned(),
        };

        hook_compact(
            self.ctx.clone(),
            &mut pipeline.main_pipeline,
            compact_target,
            compact_hook_trace_ctx,
        )
        .await;

        Ok(pipeline)
    }
}

impl ReplaceInterpreter {
    async fn build_physical_plan(
        &self,
    ) -> Result<(Box<PhysicalPlan>, Option<(Vec<StageFileInfo>, StageInfo)>)> {
        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;
        let catalog = self.ctx.get_catalog(&plan.catalog).await?;
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

        let is_multi_node = !self.ctx.get_cluster().is_empty();
        let is_value_source = matches!(self.plan.source, InsertInputSource::Values { .. });
        let is_distributed = is_multi_node
            && !is_value_source
            && self.ctx.get_settings().get_enable_distributed_replace()?;
        let table_is_empty = base_snapshot.segments.is_empty();
        let table_level_range_index = base_snapshot.summary.col_stats.clone();
        let mut purge_info = None;

        let (mut root, select_ctx, bind_context) = self
            .connect_input_source(
                self.ctx.clone(),
                &self.plan.source,
                self.plan.schema(),
                &mut purge_info,
            )
            .await?;

        let delete_when = if let Some(expr) = &plan.delete_when {
            if bind_context.is_none() {
                return Err(ErrorCode::Unimplemented(
                    "Delte semantic is only supported in subquery",
                ));
            }
            let mut bind_context = bind_context.unwrap();
            let name_resolution_ctx =
                NameResolutionContext::try_from(self.ctx.get_settings().as_ref())?;
            let metadata = Arc::new(RwLock::new(Metadata::default()));
            let mut scalar_binder = ScalarBinder::new(
                &mut bind_context,
                self.ctx.clone(),
                &name_resolution_ctx,
                metadata,
                &[],
                Default::default(),
                Default::default(),
            );
            let (scalar, _) = scalar_binder.bind(expr).await?;
            let filter = cast_expr_to_non_null_boolean(
                scalar.as_expr()?.project_column_ref(|col| col.index),
            )?;

            let filter = filter.as_remote_expr();

            let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
            if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
                return Err(ErrorCode::Unimplemented(
                    "Delete must have deterministic predicate",
                ));
            }
            Some(filter)
        } else {
            None
        };

        // remove top exchange
        if let PhysicalPlan::Exchange(Exchange { input, .. }) = root.as_ref() {
            root = input.clone();
        }
        if is_distributed {
            root = Box::new(PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: root,
                kind: common_sql::executor::FragmentKind::Expansive,
                keys: vec![],
                ignore_exchange: false,
            }));
        }

        let max_num_pruning_columns = self
            .ctx
            .get_settings()
            .get_replace_into_bloom_pruning_max_column_number()?;
        let bloom_filter_column_indexes = if !table.cluster_keys(self.ctx.clone()).is_empty() {
            fuse_table
                .choose_bloom_filter_columns(&on_conflicts, max_num_pruning_columns)
                .await?
        } else {
            vec![]
        };

        root = Box::new(PhysicalPlan::Deduplicate(Deduplicate {
            input: root,
            on_conflicts: on_conflicts.clone(),
            bloom_filter_column_indexes: bloom_filter_column_indexes.clone(),
            table_is_empty,
            table_info: table_info.clone(),
            catalog_info: catalog.info(),
            select_ctx,
            table_schema: plan.schema.clone(),
            table_level_range_index,
            need_insert: true,
            delete_when,
        }));
        root = Box::new(PhysicalPlan::ReplaceInto(ReplaceInto {
            input: root,
            block_thresholds: fuse_table.get_block_thresholds(),
            table_info: table_info.clone(),
            catalog_info: catalog.info(),
            on_conflicts,
            bloom_filter_column_indexes,
            segments: base_snapshot
                .segments
                .clone()
                .into_iter()
                .enumerate()
                .collect(),
            block_slots: None,
            need_insert: true,
        }));
        if is_distributed {
            root = Box::new(PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: root,
                kind: common_sql::executor::FragmentKind::Merge,
                keys: vec![],
                ignore_exchange: false,
            }));
        }
        root = Box::new(PhysicalPlan::CommitSink(CommitSink {
            input: root,
            snapshot: base_snapshot,
            table_info: table_info.clone(),
            catalog_info: catalog.info(),
            mutation_kind: MutationKind::Replace,
            merge_meta: false,
        }));
        Ok((root, purge_info))
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
        purge_info: &mut Option<(Vec<StageFileInfo>, StageInfo)>,
    ) -> Result<(Box<PhysicalPlan>, Option<SelectCtx>, Option<BindContext>)> {
        match source {
            InsertInputSource::Values { data, start } => self
                .connect_value_source(schema.clone(), data, *start)
                .map(|x| (x, None, None)),

            InsertInputSource::SelectPlan(plan) => {
                self.connect_query_plan_source(ctx.clone(), plan).await
            }
            InsertInputSource::Stage(plan) => match *plan.clone() {
                Plan::Copy(copy_plan) => match copy_plan.as_ref() {
                    CopyPlan::IntoTable(copy_into_table_plan) => {
                        let interpreter =
                            CopyInterpreter::try_create(ctx.clone(), *copy_plan.clone())?;
                        let (physical_plan, files) = interpreter
                            .build_physical_plan(copy_into_table_plan)
                            .await?;
                        *purge_info = Some((
                            files,
                            copy_into_table_plan.stage_table_info.stage_info.clone(),
                        ));
                        Ok((Box::new(physical_plan), None, None))
                    }
                    _ => unreachable!("plan in InsertInputSource::Stage must be CopyIntoTable"),
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
        span_offset: usize,
    ) -> Result<Box<PhysicalPlan>> {
        Ok(Box::new(PhysicalPlan::AsyncSourcer(AsyncSourcerPlan {
            value_data: value_data.to_string(),
            start: span_offset,
            schema,
        })))
    }

    #[async_backtrace::framed]
    async fn connect_query_plan_source<'a>(
        &'a self,
        ctx: Arc<QueryContext>,
        query_plan: &Plan,
    ) -> Result<(Box<PhysicalPlan>, Option<SelectCtx>, Option<BindContext>)> {
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
        Ok((physical_plan, Some(select_ctx), Some(*bind_context.clone())))
    }
}
