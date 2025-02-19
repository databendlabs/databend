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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_expression::SendableDataBlockStream;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::EmptySink;
use databend_common_pipeline_sources::EmptySource;
use databend_common_sql::binder::MutationStrategy;
use databend_common_sql::binder::MutationType;
use databend_common_sql::executor::physical_plans::create_push_down_filters;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::MutationBuildInfo;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::planner::MetadataRef;
use databend_common_sql::plans;
use databend_common_sql::plans::Mutation;
use databend_common_storage::MutationStatus;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::operations::TruncateMode;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::info;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::dml_build_update_stream_req;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::stream::DataBlockStream;

pub struct MutationInterpreter {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    schema: DataSchemaRef,
    metadata: MetadataRef,
}

impl MutationInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        s_expr: SExpr,
        schema: DataSchemaRef,
        metadata: MetadataRef,
    ) -> Result<MutationInterpreter> {
        Ok(MutationInterpreter {
            ctx,
            s_expr,
            schema,
            metadata,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for MutationInterpreter {
    fn name(&self) -> &str {
        "MutationInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        let mutation: Mutation = self.s_expr.plan().clone().try_into()?;

        let table = self
            .ctx
            .get_table(
                &mutation.catalog_name,
                &mutation.database_name,
                &mutation.table_name,
            )
            .await?;
        // Check if the table supports mutation.
        table.check_mutable()?;
        let fuse_table = table.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::Unimplemented(format!(
                "table {}, engine type {}, does not support {}",
                table.name(),
                table.get_table_info().engine(),
                mutation.mutation_type,
            ))
        })?;

        let table_snapshot = fuse_table.read_table_snapshot().await?;
        if let Some(build_res) = self
            .fast_mutation(&mutation, fuse_table, &table_snapshot)
            .await?
        {
            return Ok(build_res);
        }

        // Prepare MutationBuildInfo for PhysicalPlanBuilder to build Mutation physical plan.
        let mutation_build_info = self
            .build_mutation_info(&mutation, fuse_table, table_snapshot)
            .await?;

        // Build physical plan.
        let physical_plan = self
            .build_physical_plan(&mutation, Some(mutation_build_info))
            .await?;

        let query_plan = physical_plan
            .format(self.metadata.clone(), Default::default())?
            .format_pretty()?;

        info!("Query physical plan: \n{}", query_plan);

        // Build pipeline.
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;

        // Execute hook.
        self.execute_hook(&mutation, &mut build_res).await;

        build_res.main_pipeline.add_lock_guard(mutation.lock_guard);

        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let blocks = self.get_mutation_table_result()?;
        Ok(Box::pin(DataBlockStream::create(None, blocks)))
    }
}

impl MutationInterpreter {
    pub async fn execute_hook(
        &self,
        mutation: &databend_common_sql::plans::Mutation,
        build_res: &mut PipelineBuildResult,
    ) {
        let hook_lock_opt = if mutation.lock_guard.is_some() {
            LockTableOption::NoLock
        } else {
            LockTableOption::LockNoRetry
        };

        let mutation_kind = match mutation.mutation_type {
            MutationType::Update => MutationKind::Update,
            MutationType::Delete => MutationKind::Delete,
            MutationType::Merge => MutationKind::MergeInto,
        };

        let hook_operator = HookOperator::create(
            self.ctx.clone(),
            mutation.catalog_name.clone(),
            mutation.database_name.clone(),
            mutation.table_name.clone(),
            mutation_kind,
            hook_lock_opt,
        );
        match mutation.mutation_type {
            MutationType::Update | MutationType::Delete => {
                hook_operator
                    .execute_refresh(&mut build_res.main_pipeline)
                    .await
            }
            MutationType::Merge => hook_operator.execute(&mut build_res.main_pipeline).await,
        };
    }

    pub async fn build_mutation_info(
        &self,
        mutation: &Mutation,
        fuse_table: &FuseTable,
        table_snapshot: Option<Arc<TableSnapshot>>,
    ) -> Result<MutationBuildInfo> {
        let table_info = fuse_table.get_table_info().clone();
        let update_stream_meta = dml_build_update_stream_req(self.ctx.clone()).await?;
        let (statistics, partitions) = self
            .mutation_source_partitions(mutation, fuse_table, table_snapshot.clone())
            .await?;
        Ok(MutationBuildInfo {
            table_info,
            table_snapshot,
            update_stream_meta,
            partitions,
            statistics,
        })
    }

    pub async fn build_physical_plan(
        &self,
        mutation: &Mutation,
        mutation_build_info: Option<MutationBuildInfo>,
    ) -> Result<PhysicalPlan> {
        let mutation_build_info = if let Some(mutation_build_info) = mutation_build_info {
            mutation_build_info
        } else {
            let table = self
                .ctx
                .get_table(
                    &mutation.catalog_name,
                    &mutation.database_name,
                    &mutation.table_name,
                )
                .await?;

            // Check if the table supports DataMutation.
            table.check_mutable()?;
            let fuse_table = table.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
                ErrorCode::Unimplemented(format!(
                    "table {}, engine type {}, does not support {}",
                    table.name(),
                    table.get_table_info().engine(),
                    mutation.mutation_type,
                ))
            })?;

            // Prepare MutationBuildInfo for PhysicalPlanBuilder to build DataMutation physical plan.
            let table_snapshot = fuse_table.read_table_snapshot().await?;
            self.build_mutation_info(mutation, fuse_table, table_snapshot)
                .await?
        };

        // Build physical plan.
        let mut builder =
            PhysicalPlanBuilder::new(mutation.metadata.clone(), self.ctx.clone(), false);
        builder.set_mutation_build_info(mutation_build_info);
        builder
            .build(&self.s_expr, *mutation.required_columns.clone())
            .await
    }

    fn get_mutation_table_result(&self) -> Result<Vec<DataBlock>> {
        let binding = self.ctx.get_mutation_status();
        let status = binding.read();
        let mut columns = Vec::new();
        for field in self.schema.as_ref().fields() {
            match field.name().as_str() {
                plans::INSERT_NAME => columns.push(UInt64Type::from_data(vec![status.insert_rows])),
                plans::UPDATE_NAME => columns.push(UInt64Type::from_data(vec![status.update_rows])),
                plans::DELETE_NAME => {
                    columns.push(UInt64Type::from_data(vec![status.deleted_rows]))
                }
                _ => unreachable!(),
            }
        }
        Ok(vec![DataBlock::new_from_columns(columns)])
    }

    async fn fast_mutation(
        &self,
        mutation: &Mutation,
        fuse_table: &FuseTable,
        snapshot: &Option<Arc<TableSnapshot>>,
    ) -> Result<Option<PipelineBuildResult>> {
        if mutation.mutation_type == MutationType::Merge {
            return Ok(None);
        }

        // Check if table is empty.
        let Some(snapshot) = snapshot else {
            // No snapshot, no mutation.
            return self.no_effect_mutation();
        };
        if snapshot.summary.row_count == 0 {
            // Empty snapshot, no mutation.
            return self.no_effect_mutation();
        }

        if mutation.mutation_type == MutationType::Delete && mutation.direct_filter.is_empty() {
            // There is no filter and the mutation type is delete,
            // we can truncate the table directly.
            let mut build_res = PipelineBuildResult::create();
            self.ctx.add_mutation_status(MutationStatus {
                insert_rows: 0,
                deleted_rows: snapshot.summary.row_count,
                update_rows: 0,
            });
            // deleting the whole table... just a truncate
            fuse_table
                .do_truncate(
                    self.ctx.clone(),
                    &mut build_res.main_pipeline,
                    TruncateMode::Delete,
                )
                .await?;
            Ok(Some(build_res))
        } else {
            Ok(None)
        }
    }

    fn no_effect_mutation(&self) -> Result<Option<PipelineBuildResult>> {
        let mut build_res = PipelineBuildResult::create();
        build_res.main_pipeline.add_source(EmptySource::create, 1)?;
        build_res
            .main_pipeline
            .add_sink(|input| Ok(ProcessorPtr::create(EmptySink::create(input))))?;
        Ok(Some(build_res))
    }

    async fn mutation_source_partitions(
        &self,
        mutation: &Mutation,
        fuse_table: &FuseTable,
        table_snapshot: Option<Arc<TableSnapshot>>,
    ) -> Result<(PartStatistics, Partitions)> {
        if mutation.strategy == MutationStrategy::Direct {
            let Some(table_snapshot) = table_snapshot else {
                return Ok(Default::default());
            };
            let (filters, filter_used_columns) = if !mutation.direct_filter.is_empty() {
                let filters = create_push_down_filters(
                    &self.ctx.get_function_context()?,
                    &mutation.direct_filter,
                )?;
                let filter_used_columns = mutation
                    .direct_filter
                    .iter()
                    .flat_map(|expr| expr.used_columns())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                (Some(filters), filter_used_columns)
            } else {
                (None, vec![])
            };
            let (is_lazy, is_delete) = if mutation.mutation_type == MutationType::Delete {
                let cluster = self.ctx.get_cluster();
                let is_lazy =
                    !cluster.is_empty() && table_snapshot.segments.len() >= cluster.nodes.len();
                (is_lazy, true)
            } else {
                (false, false)
            };
            fuse_table
                .mutation_read_partitions(
                    self.ctx.clone(),
                    table_snapshot,
                    filter_used_columns,
                    filters,
                    is_lazy,
                    is_delete,
                )
                .await
        } else {
            Ok(Default::default())
        }
    }
}
