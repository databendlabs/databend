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
use databend_common_sql::binder::DataMutationInputType;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::DataMutationBuildInfo;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::plans;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::dml_build_update_stream_req;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::stream::DataBlockStream;

pub struct DataMutationInterpreter {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    schema: DataSchemaRef,
}

impl DataMutationInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        s_expr: SExpr,
        schema: DataSchemaRef,
    ) -> Result<DataMutationInterpreter> {
        Ok(DataMutationInterpreter {
            ctx,
            s_expr,
            schema,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for DataMutationInterpreter {
    fn name(&self) -> &str {
        "DataMutationInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        let data_mutation: databend_common_sql::plans::DataMutation =
            self.s_expr.plan().clone().try_into()?;

        // Build physical plan.
        let physical_plan = self.build_physical_plan(&data_mutation).await?;

        // Build pipeline.
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;

        // Execute hook.
        self.execute_hook(&data_mutation, &mut build_res).await;

        build_res
            .main_pipeline
            .add_lock_guard(data_mutation.lock_guard);

        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let blocks = self.get_data_mutation_table_result()?;
        Ok(Box::pin(DataBlockStream::create(None, blocks)))
    }
}

impl DataMutationInterpreter {
    pub async fn execute_hook(
        &self,
        data_mutation: &databend_common_sql::plans::DataMutation,
        build_res: &mut PipelineBuildResult,
    ) {
        let hook_lock_opt = if data_mutation.lock_guard.is_some() {
            LockTableOption::NoLock
        } else {
            LockTableOption::LockNoRetry
        };

        let mutation_kind = match data_mutation.input_type {
            DataMutationInputType::Update => MutationKind::Update,
            DataMutationInputType::Delete => MutationKind::Delete,
            DataMutationInputType::Merge => MutationKind::MergeInto,
        };

        let hook_operator = HookOperator::create(
            self.ctx.clone(),
            data_mutation.catalog_name.clone(),
            data_mutation.database_name.clone(),
            data_mutation.table_name.clone(),
            mutation_kind,
            hook_lock_opt,
        );
        match data_mutation.input_type {
            DataMutationInputType::Update | DataMutationInputType::Delete => {
                hook_operator
                    .execute_refresh(&mut build_res.main_pipeline)
                    .await
            }
            DataMutationInputType::Merge => {
                hook_operator.execute(&mut build_res.main_pipeline).await
            }
        };
    }

    pub async fn build_physical_plan(
        &self,
        data_mutation: &databend_common_sql::plans::DataMutation,
    ) -> Result<PhysicalPlan> {
        let table = self
            .ctx
            .get_table(
                &data_mutation.catalog_name,
                &data_mutation.database_name,
                &data_mutation.table_name,
            )
            .await?;

        // Check if the table supports DataMutation.
        table.check_mutable()?;
        let fuse_table = table.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::Unimplemented(format!(
                "table {}, engine type {}, does not support {}",
                table.name(),
                table.get_table_info().engine(),
                data_mutation.input_type,
            ))
        })?;

        // Prepare DataMutationBuildInfo for PhysicalPlanBuilder to build DataMutation physical plan.
        let table_snapshot = fuse_table.read_table_snapshot().await?;
        let update_stream_meta =
            dml_build_update_stream_req(self.ctx.clone(), &data_mutation.meta_data).await?;
        let data_mutation_build_info = DataMutationBuildInfo {
            table_snapshot,
            update_stream_meta,
        };

        // Build physical plan.
        let mut builder =
            PhysicalPlanBuilder::new(data_mutation.meta_data.clone(), self.ctx.clone(), false);
        builder.set_data_mutation_build_info(data_mutation_build_info);
        let physical_plan = builder
            .build(&self.s_expr, *data_mutation.required_columns.clone())
            .await?;

        Ok(physical_plan)
    }

    fn get_data_mutation_table_result(&self) -> Result<Vec<DataBlock>> {
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
