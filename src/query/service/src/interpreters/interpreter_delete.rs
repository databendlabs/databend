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
use common_catalog::plan::Partitions;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::schema::TableInfo;
use common_sql::executor::cast_expr_to_non_null_boolean;
use common_sql::executor::DeleteFinal;
use common_sql::executor::DeletePartial;
use common_sql::executor::Exchange;
use common_sql::executor::FragmentKind;
use common_sql::executor::PhysicalPlan;
use common_storages_factory::Table;
use common_storages_fuse::FuseTable;
use storages_common_table_meta::meta::TableSnapshot;
use table_lock::TableLockHandlerWrapper;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_distributed_pipeline;
use crate::schedulers::build_local_pipeline;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::DeletePlan;

/// interprets DeletePlan
pub struct DeleteInterpreter {
    ctx: Arc<QueryContext>,
    plan: DeletePlan,
}

impl DeleteInterpreter {
    /// Create the DeleteInterpreter from DeletePlan
    pub fn try_create(ctx: Arc<QueryContext>, plan: DeletePlan) -> Result<Self> {
        Ok(DeleteInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DeleteInterpreter {
    /// Get the name of current interpreter
    fn name(&self) -> &str {
        "DeleteInterpreter"
    }

    #[tracing::instrument(level = "debug", name = "delete_interpreter_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let is_distributed = !self.ctx.get_cluster().is_empty();
        let catalog_name = self.plan.catalog_name.as_str();
        let db_name = self.plan.database_name.as_str();
        let tbl_name = self.plan.table_name.as_str();

        let tbl = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;
        let table_info = tbl.get_table_info().clone();

        let heartbeat = if !is_distributed {
            // Add table lock heartbeat.
            let handler = TableLockHandlerWrapper::instance(self.ctx.clone());
            Some(
                handler
                    .try_lock(self.ctx.clone(), table_info.clone())
                    .await?,
            )
        } else {
            None
        };

        // refresh table.
        let tbl = self
            .ctx
            .get_catalog(catalog_name)?
            .get_table(self.ctx.get_tenant().as_str(), db_name, tbl_name)
            .await?;

        let (filter, col_indices) = if let Some(scalar) = &self.plan.selection {
            let filter = cast_expr_to_non_null_boolean(
                scalar
                    .as_expr()?
                    .project_column_ref(|col| col.column_name.clone()),
            )?
            .as_remote_expr();

            let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
            if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
                return Err(ErrorCode::Unimplemented(
                    "Delete must have deterministic predicate",
                ));
            }

            let col_indices = scalar.used_columns().into_iter().collect();
            (Some(filter), col_indices)
        } else {
            if self.plan.input_expr.is_some() {
                return Err(ErrorCode::Unimplemented(
                    "Delete with subquery isn't supported",
                ));
            }
            (None, vec![])
        };

        let fuse_table =
            tbl.as_any()
                .downcast_ref::<FuseTable>()
                .ok_or(ErrorCode::StorageUnsupported(
                    "delete must be executed on a FuseTable",
                ))?;
        let mut build_res = PipelineBuildResult::create();
        if let Some((partitions, snapshot)) = fuse_table
            .fast_delete(self.ctx.clone(), filter.as_ref(), col_indices.clone())
            .await?
        {
            let filter = filter.unwrap();
            let physical_plan = Self::build_physical_plan(
                filter,
                partitions,
                fuse_table.get_table_info().clone(),
                col_indices,
                snapshot,
                self.plan.catalog_name.clone(),
                is_distributed,
            )?;
            if is_distributed {
                build_res = build_distributed_pipeline(&self.ctx, &physical_plan).await?
            } else {
                build_res = build_local_pipeline(&self.ctx, &physical_plan, false).await?
            }
        }
        if let Some(mut heartbeat) = heartbeat {
            if build_res.main_pipeline.is_empty() {
                heartbeat.shutdown().await?;
            } else {
                build_res.main_pipeline.set_on_finished(move |may_error| {
                    // shutdown table lock heartbeat.
                    GlobalIORuntime::instance()
                        .block_on(async move { heartbeat.shutdown().await })?;
                    match may_error {
                        None => Ok(()),
                        Some(error_code) => Err(error_code.clone()),
                    }
                });
            }
        }
        Ok(build_res)
    }
}

impl DeleteInterpreter {
    pub fn build_physical_plan(
        filter: RemoteExpr<String>,
        partitions: Partitions,
        table_info: TableInfo,
        col_indices: Vec<usize>,
        snapshot: TableSnapshot,
        catalog_name: String,
        is_distributed: bool,
    ) -> Result<PhysicalPlan> {
        let root = PhysicalPlan::DeletePartial(Box::new(DeletePartial {
            parts: partitions,
            filter,
            table_info: table_info.clone(),
            catalog_name: catalog_name.clone(),
            col_indices,
        }));
        let root = if is_distributed {
            PhysicalPlan::Exchange(Exchange {
                input: Box::new(root),
                kind: FragmentKind::Merge,
                keys: vec![],
            })
        } else {
            root
        };
        let root = PhysicalPlan::DeleteFinal(Box::new(DeleteFinal {
            input: Box::new(root),
            snapshot,
            table_info,
            catalog_name,
        }));
        Ok(root)
    }
}
