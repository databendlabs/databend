// use std::sync::Arc;
// use common_datablocks::DataBlock;
// use common_datavalues::DataSchemaRef;
// use crate::pipelines::new::processors::Processor;
// use crate::pipelines::new::processors::processor::Event;
// use crate::sessions::QueryContext;
// use common_exception::{ErrorCode, Result};
// use common_planners::{Expression, PlanNode, SelectPlan};
// use crate::interpreters::SelectInterpreter;
// use crate::pipelines::new::executor::PipelinePullingExecutor;
// use crate::pipelines::new::processors::transforms::transform::Transform;
// use crate::pipelines::new::QueryPipelineBuilder;
//
// pub struct TransformCreateSets {
//     ctx: Arc<QueryContext>,
//     schema: DataSchemaRef,
//     sub_queries_expr: Vec<Expression>,
// }
//
// impl TransformCreateSets {
//     fn receive_subquery_res(&mut self, plan: &PlanNode) -> Result<()> {
//         match plan {
//             PlanNode::Select(select_plan) => {
//                 self.execute_sub_query(select_plan)
//             }
//             _ => Err(ErrorCode::LogicalError(""))
//         };
//
//         // PipelinePullingExecutor::try_create()
//         // let subquery_future = async move {
//         //     let mut stream = pipeline.execute().await?;
//         //     let mut columns = Vec::with_capacity(schema.fields().len());
//         //
//         //     for field in schema.fields() {
//         //         let data_type = field.data_type().clone();
//         //         columns.push((data_type, Vec::new()))
//         //     }
//         //
//         //     while let Some(data_block) = stream.next().await {
//         //         let data_block = data_block?;
//         //
//         //         #[allow(clippy::needless_range_loop)]
//         //         for column_index in 0..data_block.num_columns() {
//         //             let col = data_block.column(column_index);
//         //             let mut values = col.to_values();
//         //             columns[column_index].1.append(&mut values)
//         //         }
//         //     }
//         //
//         //     let mut struct_fields = Vec::with_capacity(columns.len());
//         //
//         //     for (_, values) in columns {
//         //         struct_fields.push(DataValue::Array(values))
//         //     }
//         //
//         //     match struct_fields.len() {
//         //         1 => Ok(struct_fields.remove(0)),
//         //         _ => Ok(DataValue::Struct(struct_fields)),
//         //     }
//         // };
//         //
//         // subquery_future.boxed().shared()
//         unimplemented!()
//     }
//
//     fn execute_sub_query(&mut self, select_plan: &SelectPlan) {
//         let select = select_plan.clone();
//         let subquery_ctx = QueryContext::create_from(self.ctx.clone());
//
//         let interpreter = SelectInterpreter::try_create(subquery_ctx, select)?;
//         let query_pipeline = interpreter.execute2()?;
//
//         let async_runtime = subquery_ctx.get_storage_runtime();
//         let mut query_executor = PipelinePullingExecutor::try_create(async_runtime, query_pipeline)?;
//
//         while let Some(data) = query_executor.pull_data() {
//             // TODO: set data result set
//         }
//
//         query_executor.finish()?;
//         // TODO:
//     }
// }
//
// impl Transform for TransformCreateSets {
//     const NAME: &'static str = "TransformCreateSets";
//
//     fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
//         // for query_expression in self.sub_queries_expr {
//         //     match query_expression {
//         //         Expression::Subquery { query_plan, .. } => {
//         //             let plan = query_plan.as_ref().clone();
//         //             // let builder = PipelineBuilder::create(subquery_ctx);
//         //             // let pipeline = builder.build(&plan)?;
//         //             // let shared_future = Self::receive_subquery_res(plan.schema(), pipeline);
//         //             // self.sub_queries.push(shared_future);
//         //         }
//         //         Expression::ScalarSubquery { query_plan, .. } => {
//         //             let plan = query_plan.as_ref().clone();
//         //             // let builder = PipelineBuilder::create(subquery_ctx);
//         //             // let pipeline = builder.build(&plan)?;
//         //             // let shared_future = Self::receive_scalar_subquery_res(pipeline);
//         //             // self.sub_queries.push(shared_future);
//         //         }
//         //         _ => {
//         //             return Result::Err(ErrorCode::LogicalError(
//         //                 "Expression must be Subquery or ScalarSubquery",
//         //             ));
//         //         }
//         //     };
//         // }
//         unimplemented!()
//         // TODO:
//     }
// }
//
// #[async_trait::async_trait]
// impl Processor for TransformCreateSets {
//     fn name(&self) -> &'static str {
//         "TransformDummyCreateSets"
//     }
//
//     fn event(&mut self) -> Result<Event> {
//         todo!()
//     }
// }
