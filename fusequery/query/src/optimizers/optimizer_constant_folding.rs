// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::{AggregatorFinalPlan, Expressions};
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

    fn rewrite_function(name: &str, args: Expressions) -> Result<Expression> {
        println!("rewrite function");
        let function = FunctionFactory::get(name)?;
        match function.is_deterministic() {
            true => Self::rewrite_deterministic_function(name, args),
            false => Ok(Expression::ScalarFunction {
                op: name.to_string(),
                args: args.to_vec(),
            })
        }
    }

    fn rewrite_deterministic_function(name: &str, args: Expressions) -> Result<Expression> {
        println!("rewrite deterministic function");
        match ConstantFoldingImpl::constants_arguments(&args) {
            true => Self::rewrite_const_deterministic_function(name, args),
            false => Ok(Expression::ScalarFunction { op: name.to_string(), args })
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

    fn rewrite_const_deterministic_function(name: &str, args: Expressions) -> Result<Expression> {
        println!("rewrite const deterministic function");
        let input_fields = vec![DataField::new("_dummy", DataType::UInt8, false)];
        let input_schema = Arc::new(DataSchema::new(input_fields));

        let expression = Expression::ScalarFunction { op: name.to_string(), args };
        let expression_executor = ConstantFoldingImpl::expr_executor(&input_schema, expression)?;
        let dummy_columns = vec![DataColumn::Constant(DataValue::UInt8(Some(1)), 1)];
        let data_block = DataBlock::create(input_schema, dummy_columns);
        let executed_data_block = expression_executor.execute(&data_block)?;

        assert_eq!(executed_data_block.num_rows(), 1);
        assert_eq!(executed_data_block.num_columns(), 1);
        Ok(Expression::Literal(executed_data_block.column(0).to_values()?[0].clone()))
    }
}

impl PlanRewriter for ConstantFoldingImpl {
    fn rewrite_expr(&mut self, schema: &DataSchemaRef, expr: &Expression) -> Result<Expression> {
        println!("rewrite_expr");
        match expr {
            Expression::Alias(alias, expr) => {
                Self::rewrite_alias(alias, self.rewrite_expr(schema, expr)?)
            },
            Expression::ScalarFunction { op, args } => {
                Ok(Expression::Alias(expr.column_name(), Box::new(Self::rewrite_function(op, args
                    .iter()
                    .map(|expr| Self::rewrite_expr(self, schema, expr))
                    .collect::<Result<Vec<_>>>()?)?
                )))
            }
            Expression::UnaryExpression { op, expr } => {
                Ok(Expression::Alias(expr.column_name(), Box::new(
                    Self::rewrite_function(op, vec![self.rewrite_expr(schema, expr)?])?
                )))
            }
            Expression::BinaryExpression { op, left, right } => {
                Ok(Expression::Alias(expr.column_name(), Box::new(Self::rewrite_function(op, vec![
                    self.rewrite_expr(schema, left)?,
                    self.rewrite_expr(schema, right)?
                ])?)))
            },

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
