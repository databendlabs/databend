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

use databend_common_ast::ast::Engine;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::infer_schema_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_sql::executor::physical_plans::UnionAll;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_sql::plans::DropTablePlan;
use databend_common_sql::IndexType;
use databend_common_storages_memory::MemoryTable;
use futures_util::TryStreamExt;

use crate::interpreters::CreateTableInterpreter;
use crate::interpreters::DropTableInterpreter;
use crate::interpreters::Interpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::stream::PullingExecutorStream;

// The whole recursive cte as source.
pub struct TransformRecursiveCteSource {
    ctx: Arc<QueryContext>,
    union_plan: UnionAll,
    left_outputs: Vec<(IndexType, Option<Expr>)>,
    right_outputs: Vec<(IndexType, Option<Expr>)>,

    recursive_step: usize,
    cte_scan_tables: Vec<Arc<dyn Table>>,
}

impl TransformRecursiveCteSource {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        output_port: Arc<OutputPort>,
        union_plan: UnionAll,
    ) -> Result<ProcessorPtr> {
        let left_outputs = union_plan
            .left_outputs
            .iter()
            .map(|(idx, remote_expr)| {
                if let Some(remote_expr) = remote_expr {
                    (*idx, Some(remote_expr.as_expr(&BUILTIN_FUNCTIONS)))
                } else {
                    (*idx, None)
                }
            })
            .collect::<Vec<_>>();
        let right_outputs = union_plan
            .right_outputs
            .iter()
            .map(|(idx, remote_expr)| {
                if let Some(remote_expr) = remote_expr {
                    (*idx, Some(remote_expr.as_expr(&BUILTIN_FUNCTIONS)))
                } else {
                    (*idx, None)
                }
            })
            .collect::<Vec<_>>();
        AsyncSourcer::create(ctx.clone(), output_port, TransformRecursiveCteSource {
            ctx,
            union_plan,
            left_outputs,
            right_outputs,
            recursive_step: 0,
            cte_scan_tables: vec![],
        })
    }

    async fn execute_r_cte(
        ctx: Arc<QueryContext>,
        recursive_step: usize,
        union_plan: UnionAll,
    ) -> Result<(Vec<DataBlock>, Vec<Arc<dyn Table>>)> {
        if ctx.get_settings().get_max_cte_recursive_depth()? < recursive_step {
            return Err(ErrorCode::Internal("Recursive depth is reached"));
        }
        let mut cte_scan_tables = vec![];
        let plan = if recursive_step == 0 {
            // Find all cte scan in the union right child plan, then create memory table for them.
            create_memory_table_for_cte_scan(&ctx, &union_plan.right).await?;
            // Cache cte scan tables, avoid to get them from catalog every time.
            for table_name in union_plan.cte_scan_names.iter() {
                let table = ctx
                    .get_table(
                        &ctx.get_current_catalog(),
                        &ctx.get_current_database(),
                        table_name,
                    )
                    .await?;
                cte_scan_tables.push(table);
            }
            union_plan.left.clone()
        } else {
            union_plan.right.clone()
        };
        ctx.clear_runtime_filter();
        let build_res = build_query_pipeline_without_render_result_set(&ctx, &plan).await?;
        let settings = ExecutorSettings::try_create(ctx.clone())?;
        let pulling_executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;
        ctx.set_executor(pulling_executor.get_inner())?;
        let isolate_runtime = Runtime::with_worker_threads(2, Some("r-cte-source".to_string()))?;
        let join_handle = isolate_runtime.spawn(async move {
            let stream = PullingExecutorStream::create(pulling_executor)?;
            stream.try_collect::<Vec<DataBlock>>().await
        });
        let data_blocks = join_handle.await.flatten()?;
        Ok((data_blocks, cte_scan_tables))
    }
}

#[async_trait::async_trait]
impl AsyncSource for TransformRecursiveCteSource {
    const NAME: &'static str = "TransformRecursiveCteSource";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        let mut res = None;
        let mut data = DataBlock::empty();
        match Self::execute_r_cte(
            self.ctx.clone(),
            self.recursive_step,
            self.union_plan.clone(),
        )
        .await
        {
            Ok(res) => {
                if !res.0.is_empty() {
                    data = DataBlock::concat(&res.0)?;
                }
                if !res.1.is_empty() {
                    self.cte_scan_tables = res.1;
                }
            }
            Err(e) => {
                return Err(ErrorCode::Internal(format!(
                    "Failed to execute recursive cte: {:?}",
                    e
                )));
            }
        };
        self.recursive_step += 1;

        let row_size = data.num_rows();
        if row_size > 0 {
            let func_ctx = self.ctx.get_function_context()?;
            data = project_block(
                &func_ctx,
                data,
                &self.union_plan.left.output_schema()?,
                &self.union_plan.right.output_schema()?,
                &self.left_outputs,
                &self.right_outputs,
                self.recursive_step == 1,
            )?;
            // Prepare the data of next round recursive.
            for table in self.cte_scan_tables.iter() {
                let memory_table = table.as_any().downcast_ref::<MemoryTable>().unwrap();
                memory_table.update(vec![data.clone()]);
            }
            res = Some(data);
        } else {
            let ctx = self.ctx.clone();
            let table_names = self.union_plan.cte_scan_names.clone();
            // Recursive end, remove all tables
            let _ = drop_tables(ctx, table_names).await?;
        }
        Ok(res)
    }
}

async fn drop_tables(ctx: Arc<QueryContext>, table_names: Vec<String>) -> Result<()> {
    for table_name in table_names {
        let drop_table_plan = DropTablePlan {
            if_exists: true,
            tenant: Tenant {
                tenant: ctx.get_tenant().tenant,
            },
            catalog: ctx.get_current_catalog(),
            database: ctx.get_current_database(),
            table: table_name.to_string(),
            all: true,
        };
        let drop_table_interpreter =
            DropTableInterpreter::try_create(ctx.clone(), drop_table_plan)?;
        drop_table_interpreter.execute2().await?;
    }
    Ok(())
}

#[async_recursion::async_recursion(#[recursive::recursive])]
async fn create_memory_table_for_cte_scan(
    ctx: &Arc<QueryContext>,
    plan: &PhysicalPlan,
) -> Result<()> {
    match plan {
        PhysicalPlan::Filter(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::EvalScalar(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::ProjectSet(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::AggregateExpand(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::AggregatePartial(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::AggregateFinal(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::Window(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::WindowPartition(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::Sort(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::Limit(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::RowFetch(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::HashJoin(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.build.as_ref()).await?;
            create_memory_table_for_cte_scan(ctx, plan.probe.as_ref()).await?;
        }
        PhysicalPlan::RangeJoin(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.left.as_ref()).await?;
            create_memory_table_for_cte_scan(ctx, plan.right.as_ref()).await?;
        }
        PhysicalPlan::Exchange(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::UnionAll(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.left.as_ref()).await?;
            create_memory_table_for_cte_scan(ctx, plan.right.as_ref()).await?;
        }

        PhysicalPlan::Udf(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::RecursiveCteScan(plan) => {
            // Create memory table for cte scan
            let table_fields = plan
                .output_schema
                .fields()
                .iter()
                .map(|field| {
                    Ok(TableField::new(
                        field.name(),
                        infer_schema_type(field.data_type())?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let schema = TableSchemaRefExt::create(table_fields);

            let create_table_plan = CreateTablePlan {
                create_option: CreateOption::CreateIfNotExists,
                tenant: Tenant {
                    tenant: ctx.get_tenant().tenant,
                },
                catalog: ctx.get_current_catalog(),
                database: ctx.get_current_database(),
                table: plan.table_name.clone(),
                schema,
                engine: Engine::Memory,
                engine_options: Default::default(),
                table_properties: Default::default(),
                table_partition: None,
                storage_params: None,
                options: Default::default(),
                field_comments: vec![],
                cluster_key: None,
                as_select: None,
                table_indexes: None,
                attached_columns: None,
            };
            let create_table_interpreter =
                CreateTableInterpreter::try_create(ctx.clone(), create_table_plan)?;
            let _ = create_table_interpreter.execute(ctx.clone()).await?;
        }
        PhysicalPlan::Shuffle(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::AsyncFunction(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::MaterializedCTE(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.input.as_ref()).await?;
        }
        PhysicalPlan::Sequence(plan) => {
            create_memory_table_for_cte_scan(ctx, plan.left.as_ref()).await?;
            create_memory_table_for_cte_scan(ctx, plan.right.as_ref()).await?;
        }
        PhysicalPlan::TableScan(_)
        | PhysicalPlan::ConstantTableScan(_)
        | PhysicalPlan::ExpressionScan(_)
        | PhysicalPlan::CacheScan(_)
        | PhysicalPlan::DistributedInsertSelect(_)
        | PhysicalPlan::ExchangeSource(_)
        | PhysicalPlan::ExchangeSink(_)
        | PhysicalPlan::CopyIntoTable(_)
        | PhysicalPlan::CopyIntoLocation(_)
        | PhysicalPlan::ReplaceAsyncSourcer(_)
        | PhysicalPlan::ReplaceDeduplicate(_)
        | PhysicalPlan::ReplaceInto(_)
        | PhysicalPlan::ColumnMutation(_)
        | PhysicalPlan::MutationSource(_)
        | PhysicalPlan::Mutation(_)
        | PhysicalPlan::MutationSplit(_)
        | PhysicalPlan::MutationManipulate(_)
        | PhysicalPlan::MutationOrganize(_)
        | PhysicalPlan::AddStreamColumn(_)
        | PhysicalPlan::CompactSource(_)
        | PhysicalPlan::CommitSink(_)
        | PhysicalPlan::Recluster(_)
        | PhysicalPlan::HilbertPartition(_)
        | PhysicalPlan::Duplicate(_)
        | PhysicalPlan::ChunkFilter(_)
        | PhysicalPlan::ChunkEvalScalar(_)
        | PhysicalPlan::ChunkCastSchema(_)
        | PhysicalPlan::ChunkFillAndReorder(_)
        | PhysicalPlan::ChunkAppendData(_)
        | PhysicalPlan::ChunkMerge(_)
        | PhysicalPlan::ChunkCommitInsert(_)
        | PhysicalPlan::BroadcastSource(_)
        | PhysicalPlan::MaterializeCTERef(_)
        | PhysicalPlan::BroadcastSink(_) => {}
    }
    Ok(())
}

fn project_block(
    func_ctx: &FunctionContext,
    block: DataBlock,
    left_schema: &DataSchemaRef,
    right_schema: &DataSchemaRef,
    left_outputs: &[(IndexType, Option<Expr>)],
    right_outputs: &[(IndexType, Option<Expr>)],
    is_left: bool,
) -> Result<DataBlock> {
    let num_rows = block.num_rows();
    let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);
    let columns = left_outputs
        .iter()
        .zip(right_outputs.iter())
        .map(|(left, right)| {
            if is_left {
                if let Some(expr) = &left.1 {
                    let entry = BlockEntry::new(evaluator.run(expr)?, || {
                        (expr.data_type().clone(), num_rows)
                    });
                    Ok(entry)
                } else {
                    Ok(block
                        .get_by_offset(left_schema.index_of(&left.0.to_string())?)
                        .clone())
                }
            } else if let Some(expr) = &right.1 {
                let entry = BlockEntry::new(evaluator.run(expr)?, || {
                    (expr.data_type().clone(), num_rows)
                });
                Ok(entry)
            } else if left.1.is_some() {
                Ok(block
                    .get_by_offset(right_schema.index_of(&right.0.to_string())?)
                    .clone())
            } else {
                check_type(
                    &left.0.to_string(),
                    &right.0.to_string(),
                    &block,
                    left_schema,
                    right_schema,
                )
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DataBlock::new(columns, num_rows))
}

fn check_type(
    left_name: &str,
    right_name: &str,
    block: &DataBlock,
    left_schema: &DataSchemaRef,
    right_schema: &DataSchemaRef,
) -> Result<BlockEntry> {
    let left_field = left_schema.field_with_name(left_name)?;
    let left_data_type = left_field.data_type();

    let right_field = right_schema.field_with_name(right_name)?;
    let right_data_type = right_field.data_type();

    let index = right_schema.index_of(right_name)?;

    if left_data_type == right_data_type {
        return Ok(block.get_by_offset(index).clone());
    }

    if left_data_type.remove_nullable() == right_data_type.remove_nullable() {
        let origin = block.get_by_offset(index).clone();
        let mut builder = ColumnBuilder::with_capacity(left_data_type, block.num_rows());
        for idx in 0..block.num_rows() {
            let scalar = origin.index(idx).unwrap();
            builder.push(scalar);
        }
        Ok(builder.build().into())
    } else {
        Err(ErrorCode::IllegalDataType(
            "The data type on both sides of the union does not match",
        ))
    }
}
