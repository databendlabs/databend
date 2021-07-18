// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::{AggregatorFinalPlan, Expressions, SchemaChanges};
use common_planners::AggregatorPartialPlan;
use common_planners::Expression;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::PlanRewriter;

use crate::optimizers::Optimizer;
use crate::sessions::FuseQueryContextRef;
use common_functions::scalars::FunctionFactory;
use crate::pipelines::transforms::ExpressionExecutor;
use common_datablocks::DataBlock;

pub struct ConstantFoldingOptimizer {}

struct ConstantFoldingImpl {
    before_group_by_schema: Option<DataSchemaRef>,
}

impl ConstantFoldingImpl {
    fn rewrite_alias(alias: &str, expr: Expression) -> Result<Expression> {
        Ok(Expression::Alias(alias.to_string(), Box::new(expr)))
    }

    fn constants_arguments(args: &[Expression]) -> bool {
        !args.iter().any(|expr| !matches!(expr, Expression::Literal(_)))
    }

    fn rewrite_function(op: &str, args: Expressions) -> Result<Option<Expression>> {
        let function = FunctionFactory::get(op)?;
        match function.is_deterministic() {
            true => Self::rewrite_deterministic_function(op, args),
            false => Ok(None)
        }
    }

    fn rewrite_deterministic_function(op: &str, args: Expressions) -> Result<Option<Expression>> {
        match ConstantFoldingImpl::constants_arguments(&args) {
            false => Ok(None),
            true => ConstantFoldingImpl::execute_expression(Expression::ScalarFunction {
                op: op.to_string(),
                args: args,
            }),
        }
    }

    fn expr_executor(schema: &DataSchemaRef, expr: Expression) -> Result<ExpressionExecutor> {
        let output_fields = vec![expr.to_data_field(&schema)?];
        let output_schema = DataSchemaRefExt::create(output_fields);
        ExpressionExecutor::try_create(
            "Constant folding optimizer.",
            schema.clone(),
            output_schema,
            vec![expr],
            false,
        )
    }

    fn execute_expression(expression: Expression) -> Result<Option<Expression>> {
        let input_fields = vec![DataField::new("_dummy", DataType::UInt8, false)];
        let input_schema = Arc::new(DataSchema::new(input_fields));

        let expression_executor = Self::expr_executor(&input_schema, expression)?;
        let dummy_columns = vec![DataColumn::Constant(DataValue::UInt8(Some(1)), 1)];
        let data_block = DataBlock::create(input_schema.clone(), dummy_columns);
        let executed_data_block = expression_executor.execute(&data_block)?;

        assert_eq!(executed_data_block.num_rows(), 1);
        assert_eq!(executed_data_block.num_columns(), 1);
        Ok(Some(Expression::Literal(executed_data_block.column(0).to_values()?[0].clone())))
    }
}

impl PlanRewriter for ConstantFoldingImpl {
    fn rewrite_expr(&mut self, changes: &SchemaChanges, expr: &Expression) -> Result<Expression> {
        match expr {
            Expression::Alias(alias, expr) => {
                Self::rewrite_alias(alias, self.rewrite_expr(changes, expr)?)
            },
            Expression::ScalarFunction { op, args } => {
                let new_args = args
                    .iter()
                    .map(|expr| Self::rewrite_expr(self, changes, expr))
                    .collect::<Result<Vec<_>>>()?;

                match Self::rewrite_function(op, new_args.clone())? {
                    Some(new_expr) => Ok(new_expr),
                    None => Ok(Expression::ScalarFunction {
                        op: op.clone(),
                        args: new_args,
                    }),
                }
            }
            Expression::UnaryExpression { op, expr: inner_expr } => {
                let new_expr = self.rewrite_expr(changes, inner_expr)?;
                match Self::rewrite_function(op, vec![new_expr.clone()])? {
                    Some(new_expr) => Ok(new_expr),
                    None => Ok(Expression::UnaryExpression {
                        op: op.clone(),
                        expr: Box::new(new_expr),
                    }),
                }
            }
            Expression::BinaryExpression { op, left, right } => {
                let new_left = self.rewrite_expr(changes, left)?;
                let new_right = self.rewrite_expr(changes, right)?;
                match Self::rewrite_function(op, vec![new_left.clone(), new_right.clone()])? {
                    Some(new_expr) => Ok(new_expr),
                    None => Ok(Expression::BinaryExpression {
                        op: op.clone(),
                        left: Box::new(new_left),
                        right: Box::new(new_right),
                    }),
                }
            },
            Expression::Cast { expr, data_type } => {
                let new_expr = self.rewrite_expr(changes, expr)?;
                match &new_expr {
                    Expression::Literal(_) => Ok(Self::execute_expression(
                        Expression::Cast {
                            expr: Box::new(new_expr),
                            data_type: data_type.clone(),
                        }
                    )?.unwrap()),
                    _ => Ok(new_expr)
                }
            }
            Expression::Column(column_name) => {
                let field_pos = changes.before_input_schema.index_of(column_name)?;
                let new_field = changes.after_input_schema.field(field_pos);
                Ok(Expression::Column(new_field.name().to_string()))
            }
            _ => Ok(expr.clone()),
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
                PlanBuilder::from(&new_input)
                    .aggregate_partial(&plan.aggr_expr, &plan.group_expr)?
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
            Some(schema_before_group_by) => PlanBuilder::from(&new_input)
                .aggregate_final(schema_before_group_by, &plan.aggr_expr, &plan.group_expr)?
                .build(),
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
    pub fn create(_ctx: FuseQueryContextRef) -> Self {
        ConstantFoldingOptimizer {}
    }
}
