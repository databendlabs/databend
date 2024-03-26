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

use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_exception::Result;
use databend_common_sql::executor::physical_plans::ChunkAppendData;
use databend_common_sql::executor::physical_plans::ChunkCommitInsert;
use databend_common_sql::executor::physical_plans::ChunkMerge;
use databend_common_sql::executor::physical_plans::SerializableTable;
use databend_common_sql::executor::physical_plans::ShuffleStrategy;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::plans::Else;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::InsertMultiTable;
use databend_common_sql::plans::Into;
use databend_common_sql::plans::Plan;
use databend_common_sql::ScalarExpr;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::cast_expr_to_non_null_boolean;
use crate::sql::executor::physical_plans::ChunkFilter;
use crate::sql::executor::physical_plans::Duplicate;
use crate::sql::executor::physical_plans::Shuffle;
pub struct InsertMultiTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: InsertMultiTable,
}

impl InsertMultiTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: InsertMultiTable) -> Result<InterpreterPtr> {
        Ok(Arc::new(Self { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertMultiTableInterpreter {
    fn name(&self) -> &str {
        "InsertMultiTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let physical_plan = self.build_physical_plan().await?;
        let build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
        Ok(build_res)
    }
}

impl InsertMultiTableInterpreter {
    async fn build_physical_plan(&self) -> Result<PhysicalPlan> {
        let InsertMultiTable {
            input_source,
            whens,
            opt_else,
            overwrite,
            is_first,
            intos,
        } = &self.plan;

        let (source_plan, _select_column_bindings, _metadata) = match input_source {
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
        let mut root: PhysicalPlan = source_plan;

        // Table, Filter, Projection, CastedSchema
        let mut branches = vec![];

        for when in whens {
            for into in &when.intos {
                let Into {
                    catalog,
                    database,
                    table,
                    projection,
                    casted_schema,
                } = into;
                let table = self.ctx.get_table(catalog, database, table).await?;
                branches.push((table, Some(&when.condition), projection, casted_schema));
            }
        }
        if let Some(Else { intos }) = opt_else {
            for into in intos {
                let Into {
                    catalog,
                    database,
                    table,
                    projection,
                    casted_schema,
                } = into;
                let table = self.ctx.get_table(catalog, database, table).await?;
                branches.push((table, None, projection, casted_schema));
            }
        }

        for into in intos {
            let Into {
                catalog,
                database,
                table,
                projection,
                casted_schema,
            } = into;
            let table = self.ctx.get_table(catalog, database, table).await?;
            branches.push((table, None, projection, casted_schema));
        }

        let mut serialable_tables = vec![];
        let catalog_info = self.ctx.get_catalog(CATALOG_DEFAULT).await?.info();
        for (table, _, _, _) in &branches {
            let table_info = table.get_table_info();
            serialable_tables.push(SerializableTable {
                target_catalog_info: catalog_info.clone(),
                target_table_info: table_info.clone(),
            });
        }

        let mut predicates = vec![];
        match is_first {
            true => {
                let mut previous_not: Option<ScalarExpr> = None;
                for (_, opt_scalar_expr, _, _) in branches.iter() {
                    if let Some(curr) = *opt_scalar_expr {
                        let merged_curr = if let Some(prev_not) = &previous_not {
                            let merged_scalar_expr = and(prev_not.clone(), curr.clone());
                            previous_not = Some(and(prev_not.clone(), not(curr.clone())));
                            merged_scalar_expr
                        } else {
                            previous_not = Some(not(curr.clone()));
                            curr.clone()
                        };

                        let expr = cast_expr_to_non_null_boolean(
                            merged_curr.as_expr()?.project_column_ref(|col| col.index),
                        )?;
                        predicates.push(Some(expr.as_remote_expr()));
                    } else {
                        let previous_not = previous_not.take().unwrap();
                        let expr = cast_expr_to_non_null_boolean(
                            previous_not.as_expr()?.project_column_ref(|col| col.index),
                        )?;
                        predicates.push(Some(expr.as_remote_expr()));
                    }
                }
            }
            false => {
                for (_, opt_scalar_expr, _, _) in branches.iter() {
                    if let Some(scalar_expr) = opt_scalar_expr {
                        let expr = cast_expr_to_non_null_boolean(
                            scalar_expr.as_expr()?.project_column_ref(|col| col.index),
                        )?;
                        predicates.push(Some(expr.as_remote_expr()));
                    } else {
                        predicates.push(None);
                    }
                }
            }
        };

        root = PhysicalPlan::Duplicate(Box::new(Duplicate {
            plan_id: 0,
            input: Box::new(root),
            n: branches.len(),
        }));

        let shuffle_strategy = ShuffleStrategy::Transpose(branches.len());
        root = PhysicalPlan::Shuffle(Box::new(Shuffle {
            plan_id: 0,
            input: Box::new(root),
            strategy: shuffle_strategy,
        }));

        root = PhysicalPlan::ChunkFilter(Box::new(ChunkFilter {
            plan_id: 0,
            input: Box::new(root),
            predicates,
        }));

        root = PhysicalPlan::ChunkAppendData(Box::new(ChunkAppendData {
            plan_id: 0,
            input: Box::new(root),
            target_tables: serialable_tables.clone(),
        }));

        root = PhysicalPlan::ChunkMerge(Box::new(ChunkMerge {
            plan_id: 0,
            input: Box::new(root),
            chunk_num: branches.len(),
        }));

        root = PhysicalPlan::ChunkCommitInsert(Box::new(ChunkCommitInsert {
            plan_id: 0,
            input: Box::new(root),
            update_stream_meta: vec![],
            overwrite: *overwrite,
            deduplicated_label: None,
            targets: serialable_tables.clone(),
        }));
        Ok(root)
    }
}

fn and(left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "and".to_string(),
        params: vec![],
        arguments: vec![left, right],
    })
}

fn not(expr: ScalarExpr) -> ScalarExpr {
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "not".to_string(),
        params: vec![],
        arguments: vec![expr],
    })
}
