// Copyright 2021 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionFactory;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::Expression;
use common_planners::ExpressionRewriter;
use common_planners::Expressions;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::PlanRewriter;

use crate::optimizers::Optimizer;
use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;

pub struct ConstantFoldingOptimizer {}

struct ConstantFoldingImpl {
    before_group_by_schema: Option<DataSchemaRef>,
}

impl ConstantFoldingImpl {
    fn constants_arguments(args: &[Expression]) -> bool {
        !args
            .iter()
            .any(|expr| !matches!(expr, Expression::Literal { .. }))
    }

    fn rewrite_function<F>(op: &str, args: Expressions, name: String, f: F) -> Result<Expression>
    where F: Fn(&str, Expressions) -> Expression {
        let factory = FunctionFactory::instance();
        let function_features = factory.get_features(op)?;

        if function_features.is_deterministic && Self::constants_arguments(&args) {
            let op = op.to_string();
            return ConstantFoldingImpl::execute_expression(
                Expression::ScalarFunction { op, args },
                name,
            );
        }

        Ok(f(op, args))
    }

    fn expr_executor(schema: &DataSchemaRef, expr: Expression) -> Result<ExpressionExecutor> {
        let output_fields = vec![expr.to_data_field(schema)?];
        let output_schema = DataSchemaRefExt::create(output_fields);
        ExpressionExecutor::try_create(
            "Constant folding optimizer.",
            schema.clone(),
            output_schema,
            vec![expr],
            false,
        )
    }

    fn execute_expression(expression: Expression, origin_name: String) -> Result<Expression> {
        let input_fields = vec![DataField::new("_dummy", u8::to_data_type())];
        let input_schema = Arc::new(DataSchema::new(input_fields));

        let data_type = expression.to_data_type(&input_schema)?;
        let expression_executor = Self::expr_executor(&input_schema, expression)?;
        let const_col = ConstColumn::new(Series::from_data(vec![1u8]), 1);
        let dummy_columns = vec![Arc::new(const_col) as ColumnRef];
        let data_block = DataBlock::create(input_schema, dummy_columns);
        let executed_data_block = expression_executor.execute(&data_block)?;

        ConstantFoldingImpl::convert_to_expression(origin_name, executed_data_block, data_type)
    }

    fn convert_to_expression(
        column_name: String,
        data_block: DataBlock,
        data_type: DataTypePtr,
    ) -> Result<Expression> {
        debug_assert!(data_block.num_rows() == 1);
        debug_assert!(data_block.num_columns() == 1);

        let column_name = Some(column_name);
        let value = data_block.column(0).get_checked(0)?;
        Ok(Expression::Literal {
            value,
            column_name,
            data_type,
        })
    }
}

impl PlanRewriter for ConstantFoldingImpl {
    fn rewrite_expr(&mut self, schema: &DataSchemaRef, origin: &Expression) -> Result<Expression> {
        /* TODO: constant folding for subquery and scalar subquery
         * For example:
         *   before optimize: SELECT (SELECT 1 + 2)
         *   after optimize: SELECT 3
         */
        struct ConstantExpressionRewriter(*mut ConstantFoldingImpl, DataSchemaRef);

        impl ExpressionRewriter for ConstantExpressionRewriter {
            fn mutate_scalar_function(
                &mut self,
                name: &str,
                args: Vec<Expression>,
                origin_expr: &Expression,
            ) -> Result<Expression> {
                let origin_name = origin_expr.column_name();
                ConstantFoldingImpl::rewrite_function(
                    name,
                    args,
                    origin_name,
                    Expression::create_scalar_function,
                )
            }

            fn mutate_unary_expression(
                &mut self,
                op: &str,
                expr: Expression,
                origin_expr: &Expression,
            ) -> Result<Expression> {
                let origin_name = origin_expr.column_name();
                ConstantFoldingImpl::rewrite_function(
                    op,
                    vec![expr],
                    origin_name,
                    Expression::create_unary_expression,
                )
            }

            fn mutate_binary(
                &mut self,
                op: &str,
                left: Expression,
                right: Expression,
                origin_expr: &Expression,
            ) -> Result<Expression> {
                let origin_name = origin_expr.column_name();
                ConstantFoldingImpl::rewrite_function(
                    op,
                    vec![left, right],
                    origin_name,
                    Expression::create_binary_expression,
                )
            }

            fn mutate_cast(
                &mut self,
                typ: &DataTypePtr,
                expr: Expression,
                origin_expr: &Expression,
                is_nullable: bool,
            ) -> Result<Expression> {
                if matches!(&expr, Expression::Literal { .. }) {
                    let optimize_expr = Expression::Cast {
                        expr: Box::new(expr),
                        data_type: typ.clone(),
                        is_nullable,
                    };

                    return ConstantFoldingImpl::execute_expression(
                        optimize_expr,
                        origin_expr.column_name(),
                    );
                }

                Ok(Expression::Cast {
                    expr: Box::new(expr),
                    data_type: typ.clone(),
                    is_nullable,
                })
            }
        }

        ConstantExpressionRewriter(self, schema.clone()).mutate(origin)
    }

    fn rewrite_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<PlanNode> {
        let new_input = self.rewrite_plan_node(&plan.input)?;
        match self.before_group_by_schema {
            Some(_) => Err(ErrorCode::LogicalError(
                "Logical error: before group by schema must be None",
            )),
            None => {
                self.before_group_by_schema = Some(new_input.schema());
                let new_aggr_expr = self.rewrite_exprs(&new_input.schema(), &plan.aggr_expr)?;
                let new_group_expr = self.rewrite_exprs(&new_input.schema(), &plan.group_expr)?;
                PlanBuilder::from(&new_input)
                    .aggregate_partial(&new_aggr_expr, &new_group_expr)?
                    .build()
            }
        }
    }

    fn rewrite_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<PlanNode> {
        let new_input = self.rewrite_plan_node(&plan.input)?;

        match self.before_group_by_schema.take() {
            None => Err(ErrorCode::LogicalError(
                "Logical error: before group by schema must be Some",
            )),
            Some(schema_before_group_by) => {
                let new_aggr_expr = self.rewrite_exprs(&new_input.schema(), &plan.aggr_expr)?;
                let new_group_expr = self.rewrite_exprs(&new_input.schema(), &plan.group_expr)?;
                PlanBuilder::from(&new_input)
                    .aggregate_final(schema_before_group_by, &new_aggr_expr, &new_group_expr)?
                    .build()
            }
        }
    }
}

impl ConstantFoldingImpl {
    pub fn new() -> ConstantFoldingImpl {
        ConstantFoldingImpl {
            before_group_by_schema: None,
        }
    }
}

impl Optimizer for ConstantFoldingOptimizer {
    fn name(&self) -> &str {
        "ConstantFolding"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut visitor = ConstantFoldingImpl::new();
        visitor.rewrite_plan_node(plan)
    }
}

impl ConstantFoldingOptimizer {
    pub fn create(_ctx: Arc<QueryContext>) -> Self {
        ConstantFoldingOptimizer {}
    }
}
