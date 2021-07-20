// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionFactory;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::Expression;
use common_planners::Expressions;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::PlanRewriter;

use crate::optimizers::Optimizer;
use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::FuseQueryContextRef;

pub struct ConstantFoldingOptimizer {}

struct ConstantFoldingImpl {
    before_group_by_schema: Option<DataSchemaRef>,
}

impl ConstantFoldingImpl {
    fn rewrite_alias(alias: &str, expr: Expression) -> Result<Expression> {
        Ok(Expression::Alias(alias.to_string(), Box::new(expr)))
    }

    fn constants_arguments(args: &[Expression]) -> bool {
        !args
            .iter()
            .any(|expr| !matches!(expr, Expression::Literal { .. }))
    }

    fn rewrite_function<F>(op: &str, args: Expressions, name: String, f: F) -> Result<Expression>
    where F: Fn(&str, Expressions) -> Expression {
        let function = FunctionFactory::get(op)?;

        if function.is_deterministic() && ConstantFoldingImpl::constants_arguments(&args) {
            let op = op.to_string();
            return ConstantFoldingImpl::execute_expression(
                Expression::ScalarFunction { op, args },
                name,
            );
        }

        Ok(f(op, args))
    }

    fn create_scalar_function(op: &str, args: Expressions) -> Expression {
        let op = op.to_string();
        Expression::ScalarFunction { op, args }
    }

    fn create_unary_expression(op: &str, mut args: Expressions) -> Expression {
        let op = op.to_string();
        let expr = Box::new(args.remove(0));
        Expression::UnaryExpression { op, expr }
    }

    fn create_binary_expression(op: &str, mut args: Expressions) -> Expression {
        let op = op.to_string();
        let left = Box::new(args.remove(0));
        let right = Box::new(args.remove(0));
        Expression::BinaryExpression { op, left, right }
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
        let input_fields = vec![DataField::new("_dummy", DataType::UInt8, false)];
        let input_schema = Arc::new(DataSchema::new(input_fields));

        let expression_executor = Self::expr_executor(&input_schema, expression)?;
        let dummy_columns = vec![DataColumn::Constant(DataValue::UInt8(Some(1)), 1)];
        let data_block = DataBlock::create(input_schema, dummy_columns);
        let executed_data_block = expression_executor.execute(&data_block)?;

        ConstantFoldingImpl::convert_to_expression(origin_name, executed_data_block)
    }

    fn convert_to_expression(column_name: String, data_block: DataBlock) -> Result<Expression> {
        assert_eq!(data_block.num_rows(), 1);
        assert_eq!(data_block.num_columns(), 1);

        let column_name = Some(column_name);
        let value = data_block.column(0).to_values()?.remove(0);
        Ok(Expression::Literal { value, column_name })
    }
}

impl PlanRewriter for ConstantFoldingImpl {
    fn rewrite_expr(&mut self, schema: &DataSchemaRef, origin: &Expression) -> Result<Expression> {
        /* TODO: constant folding for subquery and scalar subquery
         * For example:
         *   before optimize: SELECT (SELECT 1 + 2)
         *   after optimize: SELECT 3
         */
        match origin {
            Expression::Alias(alias, expr) => {
                Self::rewrite_alias(alias, self.rewrite_expr(schema, expr)?)
            }
            Expression::ScalarFunction { op, args } => {
                let new_args = args
                    .iter()
                    .map(|expr| Self::rewrite_expr(self, schema, expr))
                    .collect::<Result<Vec<_>>>()?;

                let origin_name = origin.column_name();
                Self::rewrite_function(op, new_args, origin_name, Self::create_scalar_function)
            }
            Expression::UnaryExpression { op, expr } => {
                let origin_name = origin.column_name();
                let new_expr = vec![self.rewrite_expr(schema, expr)?];
                Self::rewrite_function(op, new_expr, origin_name, Self::create_unary_expression)
            }
            Expression::BinaryExpression { op, left, right } => {
                let new_left = self.rewrite_expr(schema, left)?;
                let new_right = self.rewrite_expr(schema, right)?;

                let origin_name = origin.column_name();
                let new_exprs = vec![new_left, new_right];
                Self::rewrite_function(op, new_exprs, origin_name, Self::create_binary_expression)
            }
            Expression::Cast { expr, data_type } => {
                let new_expr = self.rewrite_expr(schema, expr)?;

                if matches!(&new_expr, Expression::Literal { .. }) {
                    let optimize_expr = Expression::Cast {
                        expr: Box::new(new_expr),
                        data_type: data_type.clone(),
                    };

                    return Self::execute_expression(optimize_expr, origin.column_name());
                }

                Ok(Expression::Cast {
                    expr: Box::new(new_expr),
                    data_type: data_type.clone(),
                })
            }
            Expression::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                let new_expr = self.rewrite_expr(schema, expr)?;
                Ok(ConstantFoldingImpl::create_sort(asc, nulls_first, new_expr))
            }
            Expression::AggregateFunction { op, distinct, args } => {
                let args = args
                    .iter()
                    .map(|expr| Self::rewrite_expr(self, schema, expr))
                    .collect::<Result<Vec<_>>>()?;

                let op = op.clone();
                let distinct = *distinct;
                Ok(Expression::AggregateFunction { op, distinct, args })
            }
            _ => Ok(origin.clone()),
        }
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

#[async_trait::async_trait]
impl Optimizer for ConstantFoldingOptimizer {
    fn name(&self) -> &str {
        "ConstantFolding"
    }

    async fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut visitor = ConstantFoldingImpl::new();
        visitor.rewrite_plan_node(plan)
    }
}

impl ConstantFoldingOptimizer {
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        ConstantFoldingOptimizer {}
    }
}

impl ConstantFoldingImpl {
    fn create_sort(asc: &bool, nulls_first: &bool, new_expr: Expression) -> Expression {
        Expression::Sort {
            expr: Box::new(new_expr),
            asc: *asc,
            nulls_first: *nulls_first,
        }
    }
}
