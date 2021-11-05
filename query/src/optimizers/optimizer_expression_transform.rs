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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionFactory;
use common_planners::*;

use crate::optimizers::Optimizer;
use crate::sessions::DatabendQueryContextRef;

pub struct ExprTransformOptimizer {}

struct ExprTransformImpl {
    before_group_by_schema: Option<DataSchemaRef>,
    one_time_filter: Option<bool>,
    one_time_limit: Option<bool>,
    one_time_having: Option<bool>,
}

impl ExprTransformImpl {
    fn inverse_expr<F>(
        op: &str,
        args: Expressions,
        origin: &Expression,
        is_negated: bool,
        f: F,
    ) -> Result<Expression>
    where
        F: Fn(&str, Expressions) -> Expression,
    {
        if !is_negated {
            return Ok(origin.clone());
        }

        let factory = FunctionFactory::instance();
        let function_features = factory.get_features(op)?;

        let expr = function_features.negative_function_name.as_ref().map_or(
            Expression::create_unary_expression("NOT", vec![origin.clone()]),
            |v| f(v, args),
        );
        Ok(expr)
    }

    // Apply NOT transformation to the expression and return a new one.
    fn truth_transformer(origin: &Expression, is_negated: bool) -> Result<Expression> {
        match origin {
            // TODO: support in and not in.
            Expression::BinaryExpression { op, left, right } => match op.to_lowercase().as_str() {
                "and" => {
                    let new_left = Self::truth_transformer(left, is_negated)?;
                    let new_right = Self::truth_transformer(right, is_negated)?;
                    if is_negated {
                        Ok(new_left.or(new_right))
                    } else {
                        Ok(new_left.and(new_right))
                    }
                }
                "or" => {
                    let new_left = Self::truth_transformer(left, is_negated)?;
                    let new_right = Self::truth_transformer(right, is_negated)?;
                    if is_negated {
                        Ok(new_left.and(new_right))
                    } else {
                        Ok(new_left.or(new_right))
                    }
                }
                other => Self::inverse_expr(
                    other,
                    vec![left.as_ref().clone(), right.as_ref().clone()],
                    origin,
                    is_negated,
                    Expression::create_binary_expression,
                ),
            },
            Expression::ScalarFunction { op, args } => Self::inverse_expr(
                op.to_lowercase().as_str(),
                args.clone(),
                origin,
                is_negated,
                Expression::create_scalar_function,
            ),
            Expression::UnaryExpression { op, expr } if op.to_lowercase().eq("not") => {
                Self::truth_transformer(expr, !is_negated)
            }
            _ => {
                if !is_negated {
                    Ok(origin.clone())
                } else {
                    Ok(Expression::create_unary_expression("NOT", vec![
                        origin.clone()
                    ]))
                }
            }
        }
    }

    fn make_condition(op: &str, origin: &Expression) -> Result<Expression> {
        let factory = FunctionFactory::instance();
        let function_features = factory.get_features(op)?;
        if function_features.is_bool_func {
            Ok(origin.clone())
        } else {
            Ok(origin.not_eq(lit(0)))
        }
    }

    // Ensure that all expressions involved in conditions are boolean functions.
    // Specifically, change <non-bool-expr> to (0 <> <non-bool-expr>).
    fn boolean_transformer(origin: &Expression) -> Result<Expression> {
        match origin {
            Expression::Literal { .. } => Ok(origin.clone()),
            Expression::BinaryExpression { op, left, right } => match op.to_lowercase().as_str() {
                "and" => {
                    let new_left = Self::boolean_transformer(left)?;
                    let new_right = Self::boolean_transformer(right)?;
                    Ok(new_left.and(new_right))
                }
                "or" => {
                    let new_left = Self::boolean_transformer(left)?;
                    let new_right = Self::boolean_transformer(right)?;
                    Ok(new_left.or(new_right))
                }
                other => Self::make_condition(other, origin),
            },
            Expression::UnaryExpression { op, expr } => match op.to_lowercase().as_str() {
                "not" => {
                    let new_expr = Self::boolean_transformer(expr)?;
                    Ok(not(new_expr))
                }
                other => Self::make_condition(other, origin),
            },
            Expression::ScalarFunction { op, .. } => Self::make_condition(op.as_str(), origin),
            _ => Ok(origin.not_eq(lit(0))),
        }
    }
}

impl PlanRewriter for ExprTransformImpl {
    fn rewrite_expr(&mut self, _schema: &DataSchemaRef, expr: &Expression) -> Result<Expression> {
        Self::truth_transformer(expr, false)
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

    fn rewrite_limit(&mut self, plan: &LimitPlan) -> Result<PlanNode> {
        let new_input = if let Some(0) = plan.n {
            // case of limit zero.
            self.one_time_limit = Some(false);
            let plan = self.rewrite_plan_node(plan.input.as_ref())?;
            self.one_time_limit = None;
            plan
        } else {
            self.rewrite_plan_node(plan.input.as_ref())?
        };

        PlanBuilder::from(&new_input)
            .limit_offset(plan.n, plan.offset)?
            .build()
    }

    fn rewrite_filter(&mut self, plan: &FilterPlan) -> Result<PlanNode> {
        if plan.is_literal_false() {
            self.one_time_filter = Some(false);
            let new_input = self.rewrite_plan_node(plan.input.as_ref())?;
            self.one_time_filter = None;
            return PlanBuilder::from(&new_input)
                .filter(plan.predicate.clone())?
                .build();
        }

        let new_input = self.rewrite_plan_node(plan.input.as_ref())?;
        let new_predicate = Self::boolean_transformer(&plan.predicate)?;
        let new_predicate = Self::truth_transformer(&new_predicate, false)?;
        PlanBuilder::from(&new_input).filter(new_predicate)?.build()
    }

    fn rewrite_having(&mut self, plan: &HavingPlan) -> Result<PlanNode> {
        if plan.is_literal_false() {
            self.one_time_having = Some(false);
            let new_input = self.rewrite_plan_node(plan.input.as_ref())?;
            self.one_time_having = None;
            return PlanBuilder::from(&new_input)
                .having(plan.predicate.clone())?
                .build();
        }

        let new_input = self.rewrite_plan_node(plan.input.as_ref())?;
        let new_predicate = Self::boolean_transformer(&plan.predicate)?;
        let new_predicate = Self::truth_transformer(&new_predicate, false)?;
        PlanBuilder::from(&new_input).having(new_predicate)?.build()
    }

    fn rewrite_read_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<PlanNode> {
        if self.should_skip_scan() {
            // if the filter is literal false, like 'where 1+2=4' (constant folding optimizer will overwrite it to literal false),
            // or the limit is zero, like 'limit 0',
            // of the having is literal false like 'having 1=2'
            // then we overwrites the ReadDataSourcePlan to an empty one.
            let node = PlanNode::ReadSource(ReadDataSourcePlan {
                table_info: plan.table_info.clone(),
                scan_fields: plan.scan_fields.clone(),
                parts: vec![], // set parts to empty vector, read_table should return None immediately
                statistics: Statistics {
                    read_rows: 0,
                    read_bytes: 0,
                    is_exact: true,
                },
                description: format!("(Read from {} table)", plan.table_info.desc),
                tbl_args: plan.tbl_args.clone(),
                push_downs: plan.push_downs.clone(),
            });
            return Ok(node);
        }
        Ok(PlanNode::ReadSource(plan.clone()))
    }
}

impl ExprTransformImpl {
    pub fn new() -> ExprTransformImpl {
        ExprTransformImpl {
            before_group_by_schema: None,
            one_time_filter: None,
            one_time_limit: None,
            one_time_having: None,
        }
    }

    pub fn should_skip_scan(&self) -> bool {
        self.one_time_filter == Some(false)
            || self.one_time_limit == Some(false)
            || self.one_time_having == Some(false)
    }
}

impl Optimizer for ExprTransformOptimizer {
    fn name(&self) -> &str {
        "ExprTransform"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut visitor = ExprTransformImpl::new();
        visitor.rewrite_plan_node(plan)
    }
}

impl ExprTransformOptimizer {
    pub fn create(_ctx: DatabendQueryContextRef) -> Self {
        ExprTransformOptimizer {}
    }
}
