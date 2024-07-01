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

use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_expression::SendableDataBlockStream;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::MergeIntoBuildInfo;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::plans;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::interpreters::common::dml_build_update_stream_req;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::stream::DataBlockStream;

pub struct MergeIntoInterpreter {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    schema: DataSchemaRef,
}

impl MergeIntoInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        s_expr: SExpr,
        schema: DataSchemaRef,
    ) -> Result<MergeIntoInterpreter> {
        Ok(MergeIntoInterpreter {
            ctx,
            s_expr,
            schema,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for MergeIntoInterpreter {
    fn name(&self) -> &str {
        "MergeIntoInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let merge_into: databend_common_sql::plans::MergeInto =
            self.s_expr.plan().clone().try_into()?;

        // Build physical plan.
        let physical_plan = self.build_physical_plan(&merge_into).await?;

        // Build pipeline.
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;

        // Execute hook.
        {
            let hook_lock_opt = if merge_into.lock_guard.is_some() {
                LockTableOption::NoLock
            } else {
                LockTableOption::LockNoRetry
            };
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                merge_into.catalog.clone(),
                merge_into.database.clone(),
                merge_into.table.clone(),
                MutationKind::MergeInto,
                hook_lock_opt,
            );
            hook_operator.execute(&mut build_res.main_pipeline).await;
        }

        build_res
            .main_pipeline
            .add_lock_guard(merge_into.lock_guard);

        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let blocks = self.get_merge_into_table_result()?;
        Ok(Box::pin(DataBlockStream::create(None, blocks)))
    }
}

impl MergeIntoInterpreter {
    pub async fn build_physical_plan(
        &self,
        merge_into: &databend_common_sql::plans::MergeInto,
    ) -> Result<PhysicalPlan> {
        let table = self
            .ctx
            .get_table(&merge_into.catalog, &merge_into.database, &merge_into.table)
            .await?;

        // Check if the table supports MERGE INTO.
        table.check_mutable()?;
        let fuse_table = table.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::Unimplemented(format!(
                "table {}, engine type {}, does not support MERGE INTO",
                table.name(),
                table.get_table_info().engine(),
            ))
        })?;

        // Prepare MergeIntoBuildInfo for PhysicalPlanBuilder to build MergeInto physical plan.
        let table_info = fuse_table.get_table_info();
        let table_snapshot = fuse_table.read_table_snapshot().await?.unwrap_or_else(|| {
            Arc::new(TableSnapshot::new_empty_snapshot(
                fuse_table.schema().as_ref().clone(),
                Some(table_info.ident.seq),
            ))
        });
        let update_stream_meta =
            dml_build_update_stream_req(self.ctx.clone(), &merge_into.meta_data).await?;
        let merge_into_build_info = MergeIntoBuildInfo {
            table_snapshot,
            update_stream_meta,
        };

        // Build physical plan.
        let mut builder =
            PhysicalPlanBuilder::new(merge_into.meta_data.clone(), self.ctx.clone(), false);
        builder.set_merge_into_build_info(merge_into_build_info);
        let physical_plan = builder
            .build(&self.s_expr, *merge_into.columns_set.clone())
            .await?;

        Ok(physical_plan)
    }

    fn get_merge_into_table_result(&self) -> Result<Vec<DataBlock>> {
        let binding = self.ctx.get_merge_status();
        let status = binding.read();
        let mut columns = Vec::new();
        for field in self.schema.as_ref().fields() {
            match field.name().as_str() {
                plans::INSERT_NAME => {
                    columns.push(UInt32Type::from_data(vec![status.insert_rows as u32]))
                }
                plans::UPDATE_NAME => {
                    columns.push(UInt32Type::from_data(vec![status.update_rows as u32]))
                }
                plans::DELETE_NAME => {
                    columns.push(UInt32Type::from_data(vec![status.deleted_rows as u32]))
                }
                _ => unreachable!(),
            }
        }
        Ok(vec![DataBlock::new_from_columns(columns)])
    }
}
