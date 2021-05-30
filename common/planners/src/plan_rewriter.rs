// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::AggregatorFinalPlan;
use crate::AggregatorPartialPlan;
use crate::CreateDatabasePlan;
use crate::CreateTablePlan;
use crate::DropDatabasePlan;
use crate::DropTablePlan;
use crate::EmptyPlan;
use crate::ExplainPlan;
use crate::Expression;
use crate::ExpressionPlan;
use crate::FilterPlan;
use crate::HavingPlan;
use crate::JoinPlan;
use crate::InsertIntoPlan;
use crate::LimitPlan;
use crate::PlanNode;
use crate::ProjectionPlan;
use crate::ReadDataSourcePlan;
use crate::ScanPlan;
use crate::SelectPlan;
use crate::SettingPlan;
use crate::SortPlan;
use crate::StagePlan;
use crate::UseDatabasePlan;

/// `PlanRewriter` is a visitor that can help to rewrite `PlanNode`
/// By default, a `PlanRewriter` will traverse the plan tree in pre-order and return rewritten plan tree.
/// Every `rewrite_xxx` method should return a new `PlanNode`(in default implementation it will return a clone of given plan node)
/// so its parent can replace original children with rewritten children.
/// # Example
/// `PlanRewriter` is useful when you want to rewrite a part of a plan tree.
/// For example, if we'd like to rewrite all the `Filter` nodes in a plan tree and keep rest nodes as they are, we can implement a rewriter like:
/// ```ignore
/// struct FilterRewriter {};
/// impl<'plan> PlanRewriter<'plan> for FilterRewriter {
///     fn rewrite_filter(&mut self, plan: &PlanNode) -> Result<PlanNode> {
///         // Do what you want to do and return the new Filter node
///     }
/// }
///
/// let plan = build_some_plan();
/// let mut rewriter = FilterRewriter {};
/// let new_plan = rewriter.rewrite_plan_node(&plan)?; // new_plan is the rewritten plan
/// ```
pub trait PlanRewriter<'plan> {
    fn rewrite_plan_node(&mut self, plan: &'plan PlanNode) -> Result<PlanNode> {
        match plan {
            PlanNode::AggregatorPartial(plan) => self.rewrite_aggregate_partial(plan),
            PlanNode::AggregatorFinal(plan) => self.rewrite_aggregate_final(plan),
            PlanNode::Empty(plan) => self.rewrite_empty(plan),
            PlanNode::Projection(plan) => self.rewrite_projection(plan),
            PlanNode::Filter(plan) => self.rewrite_filter(plan),
            PlanNode::Sort(plan) => self.rewrite_sort(plan),
            PlanNode::Limit(plan) => self.rewrite_limit(plan),
            PlanNode::Scan(plan) => self.rewrite_scan(plan),
            PlanNode::ReadSource(plan) => self.rewrite_read_data_source(plan),
            PlanNode::Select(plan) => self.rewrite_select(plan),
            PlanNode::Explain(plan) => self.rewrite_explain(plan),
            PlanNode::CreateTable(plan) => self.rewrite_create_table(plan),
            PlanNode::CreateDatabase(plan) => self.rewrite_create_database(plan),
            PlanNode::UseDatabase(plan) => self.rewrite_use_database(plan),
            PlanNode::SetVariable(plan) => self.rewrite_set_variable(plan),
            PlanNode::Stage(plan) => self.rewrite_stage(plan),
            PlanNode::Having(plan) => self.rewrite_having(plan),
            PlanNode::Expression(plan) => self.rewrite_expression(plan),
            PlanNode::DropTable(plan) => self.rewrite_drop_table(plan),
            PlanNode::DropDatabase(plan) => self.rewrite_drop_database(plan),
            PlanNode::Join(plan) => self.rewrite_join(plan)
            PlanNode::InsertInto(plan) => self.rewrite_insert_into(plan)
        }
    }

    fn rewrite_aggregate_partial(
        &mut self,
        plan: &'plan AggregatorPartialPlan
    ) -> Result<PlanNode> {
        Ok(PlanNode::AggregatorPartial(AggregatorPartialPlan {
            schema: plan.schema.clone(),
            aggr_expr: plan.aggr_expr.clone(),
            group_expr: plan.group_expr.clone(),
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?)
        }))
    }

    fn rewrite_aggregate_final(&mut self, plan: &'plan AggregatorFinalPlan) -> Result<PlanNode> {
        Ok(PlanNode::AggregatorFinal(AggregatorFinalPlan {
            schema: plan.schema.clone(),
            aggr_expr: plan.aggr_expr.clone(),
            group_expr: plan.group_expr.clone(),
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?)
        }))
    }

    fn rewrite_empty(&mut self, plan: &'plan EmptyPlan) -> Result<PlanNode> {
        Ok(PlanNode::Empty(plan.clone()))
    }

    fn rewrite_stage(&mut self, plan: &'plan StagePlan) -> Result<PlanNode> {
        Ok(PlanNode::Stage(StagePlan {
            uuid: plan.uuid.clone(),
            id: plan.id,
            state: plan.state.clone(),
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?)
        }))
    }

    fn rewrite_projection(&mut self, plan: &'plan ProjectionPlan) -> Result<PlanNode> {
        Ok(PlanNode::Projection(ProjectionPlan {
            schema: plan.schema.clone(),
            expr: plan.expr.clone(),
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?)
        }))
    }

    fn rewrite_expression(&mut self, plan: &'plan ExpressionPlan) -> Result<PlanNode> {
        Ok(PlanNode::Expression(ExpressionPlan {
            schema: plan.schema.clone(),
            desc: plan.desc.clone(),
            exprs: plan.exprs.clone(),
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?)
        }))
    }

    fn rewrite_filter(&mut self, plan: &'plan FilterPlan) -> Result<PlanNode> {
        Ok(PlanNode::Filter(FilterPlan {
            predicate: plan.predicate.clone(),
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?)
        }))
    }

    fn rewrite_having(&mut self, plan: &'plan HavingPlan) -> Result<PlanNode> {
        Ok(PlanNode::Having(HavingPlan {
            predicate: plan.predicate.clone(),
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?)
        }))
    }

    fn rewrite_sort(&mut self, plan: &'plan SortPlan) -> Result<PlanNode> {
        Ok(PlanNode::Sort(SortPlan {
            order_by: plan.order_by.clone(),
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?)
        }))
    }

    fn rewrite_limit(&mut self, plan: &'plan LimitPlan) -> Result<PlanNode> {
        Ok(PlanNode::Limit(LimitPlan {
            n: plan.n,
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?)
        }))
    }

    fn rewrite_scan(&mut self, plan: &'plan ScanPlan) -> Result<PlanNode> {
        Ok(PlanNode::Scan(plan.clone()))
    }

    fn rewrite_read_data_source(&mut self, plan: &'plan ReadDataSourcePlan) -> Result<PlanNode> {
        Ok(PlanNode::ReadSource(plan.clone()))
    }

    fn rewrite_select(&mut self, plan: &'plan SelectPlan) -> Result<PlanNode> {
        Ok(PlanNode::Select(SelectPlan {
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?)
        }))
    }

    fn rewrite_explain(&mut self, plan: &'plan ExplainPlan) -> Result<PlanNode> {
        Ok(PlanNode::Explain(ExplainPlan {
            typ: plan.typ,
            input: Arc::new(self.rewrite_plan_node(plan.input.as_ref())?)
        }))
    }

    fn rewrite_create_table(&mut self, plan: &'plan CreateTablePlan) -> Result<PlanNode> {
        Ok(PlanNode::CreateTable(plan.clone()))
    }

    fn rewrite_create_database(&mut self, plan: &'plan CreateDatabasePlan) -> Result<PlanNode> {
        Ok(PlanNode::CreateDatabase(plan.clone()))
    }

    fn rewrite_use_database(&mut self, plan: &'plan UseDatabasePlan) -> Result<PlanNode> {
        Ok(PlanNode::UseDatabase(plan.clone()))
    }

    fn rewrite_set_variable(&mut self, plan: &'plan SettingPlan) -> Result<PlanNode> {
        Ok(PlanNode::SetVariable(plan.clone()))
    }

    fn rewrite_drop_table(&mut self, plan: &'plan DropTablePlan) -> Result<PlanNode> {
        Ok(PlanNode::DropTable(plan.clone()))
    }

    fn rewrite_drop_database(&mut self, plan: &'plan DropDatabasePlan) -> Result<PlanNode> {
        Ok(PlanNode::DropDatabase(plan.clone()))
    }

    fn rewrite_join(&mut self, plan: &'plan JoinPlan) -> Result<PlanNode> {
        Ok(PlanNode::Join(plan.clone()))
    }

    fn rewrite_insert_into(&mut self, plan: &'plan InsertIntoPlan) -> Result<PlanNode> {
        Ok(PlanNode::InsertInto(plan.clone()))
    }
}

pub struct RewriteHelper {}

struct QueryAliasData {
    aliases: HashMap<String, Expression>,
    inside_aliases: HashSet<String>,
    // deepest alias current step in
    current_alias: String
}

impl RewriteHelper {
    /// Recursively extract the aliases in projection exprs
    ///
    /// SELECT (x+1) as y, y*y FROM ..
    /// ->
    /// SELECT (x+1) as y, (x+1)*(x+1) FROM ..
    pub fn rewrite_projection_aliases(exprs: &[Expression]) -> Result<Vec<Expression>> {
        let mut mp = HashMap::new();
        RewriteHelper::alias_exprs_to_map(&exprs, &mut mp)?;

        let mut data = QueryAliasData {
            aliases: mp,
            inside_aliases: HashSet::new(),
            current_alias: "".into()
        };

        exprs
            .iter()
            .map(|expr| RewriteHelper::expr_rewrite_alias(expr, &mut data))
            .collect()
    }

    fn alias_exprs_to_map(
        exprs: &[Expression],
        mp: &mut HashMap<String, Expression>
    ) -> Result<()> {
        for expr in exprs.iter() {
            if let Expression::Alias(alias, alias_expr) = expr {
                if let Some(expr_result) = mp.get(alias) {
                    let hash_result = format!("{:?}", expr_result);
                    let hash_expr = format!("{:?}", expr);

                    if hash_result != hash_expr {
                        return Result::Err(ErrorCodes::SyntaxException(format!(
                            "Planner Error: Different expressions with the same alias {}",
                            alias
                        )));
                    }
                }
                mp.insert(alias.clone(), *alias_expr.clone());
            }
        }
        Ok(())
    }

    fn expr_rewrite_alias(expr: &Expression, data: &mut QueryAliasData) -> Result<Expression> {
        match expr {
            Expression::Column(field) => {
                // x + 1 --> x
                if *field == data.current_alias {
                    return Ok(expr.clone());
                }

                // x + 1 --> y, y + 1 --> x
                if data.inside_aliases.contains(field) {
                    return Result::Err(ErrorCodes::SyntaxException(format!(
                        "Planner Error: Cyclic aliases: {}",
                        field
                    )));
                }

                let tmp = data.aliases.get(field).cloned();
                if let Some(e) = tmp {
                    let previous_alias = data.current_alias.clone();

                    data.current_alias = field.clone();
                    data.inside_aliases.insert(field.clone());
                    let c = RewriteHelper::expr_rewrite_alias(&e, data)?;
                    data.inside_aliases.remove(field);
                    data.current_alias = previous_alias;

                    return Ok(c);
                }
                Ok(expr.clone())
            }

            Expression::BinaryExpression { op, left, right } => {
                let left = RewriteHelper::expr_rewrite_alias(left, data)?;
                let right = RewriteHelper::expr_rewrite_alias(right, data)?;

                Ok(Expression::BinaryExpression {
                    op: op.clone(),
                    left: Box::new(left),
                    right: Box::new(right)
                })
            }

            Expression::UnaryExpression { op, expr } => {
                let expr_new = RewriteHelper::expr_rewrite_alias(expr, data)?;

                Ok(Expression::UnaryExpression {
                    op: op.clone(),
                    expr: Box::new(expr_new)
                })
            }

            Expression::ScalarFunction { op, args } => {
                let new_args: Result<Vec<Expression>> = args
                    .iter()
                    .map(|v| RewriteHelper::expr_rewrite_alias(v, data))
                    .collect();

                match new_args {
                    Ok(v) => Ok(Expression::ScalarFunction {
                        op: op.clone(),
                        args: v
                    }),
                    Err(v) => Err(v)
                }
            }

            Expression::AggregateFunction { op, args } => {
                let new_args: Result<Vec<Expression>> = args
                    .iter()
                    .map(|v| RewriteHelper::expr_rewrite_alias(v, data))
                    .collect();

                match new_args {
                    Ok(v) => Ok(Expression::AggregateFunction {
                        op: op.clone(),
                        args: v
                    }),
                    Err(v) => Err(v)
                }
            }

            Expression::Alias(alias, plan) => {
                if data.inside_aliases.contains(alias) {
                    return Result::Err(ErrorCodes::SyntaxException(format!(
                        "Planner Error: Cyclic aliases: {}",
                        alias
                    )));
                }

                let previous_alias = data.current_alias.clone();
                data.current_alias = alias.clone();
                data.inside_aliases.insert(alias.clone());
                let new_expr = RewriteHelper::expr_rewrite_alias(plan, data)?;
                data.inside_aliases.remove(alias);
                data.current_alias = previous_alias;

                Ok(Expression::Alias(alias.clone(), Box::new(new_expr)))
            }
            Expression::Cast { expr, data_type } => {
                let new_expr = RewriteHelper::expr_rewrite_alias(expr, data)?;
                Ok(Expression::Cast {
                    expr: Box::new(new_expr),
                    data_type: data_type.clone()
                })
            }
            Expression::Wildcard | Expression::Literal(_) | Expression::Sort { .. } => {
                Ok(expr.clone())
            }
        }
    }

    /// replaces expression columns by its name on the projection.
    /// SELECT a as b ... where b>1
    /// ->
    /// SELECT a as b ... where a>1
    pub fn rewrite_alias_expr(
        projection_map: &HashMap<String, Expression>,
        expr: &Expression
    ) -> Result<Expression> {
        let expressions = Self::expression_plan_children(expr)?;

        let expressions = expressions
            .iter()
            .map(|e| Self::rewrite_alias_expr(projection_map, e))
            .collect::<Result<Vec<_>>>()?;

        if let Expression::Column(name) = expr {
            if let Some(expr) = projection_map.get(name) {
                return Ok(expr.clone());
            }
        }
        Ok(Self::rebuild_from_exprs(&expr, &expressions))
    }

    /// replaces expressions columns by its name on the projection.
    pub fn rewrite_alias_exprs(
        projection_map: &HashMap<String, Expression>,
        exprs: &[Expression]
    ) -> Result<Vec<Expression>> {
        exprs
            .iter()
            .map(|e| Self::rewrite_alias_expr(projection_map, e))
            .collect::<Result<Vec<_>>>()
    }

    /// Collect all unique projection fields to a map.
    pub fn projection_to_map(plan: &PlanNode) -> Result<HashMap<String, Expression>> {
        let mut map = HashMap::new();
        Self::projections_to_map(plan, &mut map)?;
        Ok(map)
    }

    /// Get the expression children.
    pub fn expression_plan_children(expr: &Expression) -> Result<Vec<Expression>> {
        Ok(match expr {
            Expression::Alias(_, expr) => vec![expr.as_ref().clone()],
            Expression::Column(_) => vec![],
            Expression::Literal(_) => vec![],
            Expression::UnaryExpression { expr, .. } => {
                vec![expr.as_ref().clone()]
            }
            Expression::BinaryExpression { left, right, .. } => {
                vec![left.as_ref().clone(), right.as_ref().clone()]
            }
            Expression::ScalarFunction { args, .. } => args.clone(),
            Expression::AggregateFunction { args, .. } => args.clone(),
            Expression::Wildcard => vec![],
            Expression::Sort { expr, .. } => vec![expr.as_ref().clone()],
            Expression::Cast { expr, .. } => vec![expr.as_ref().clone()]
        })
    }

    /// Get the leaves of an expression.
    pub fn expression_plan_columns(expr: &Expression) -> Result<Vec<Expression>> {
        Ok(match expr {
            Expression::Alias(_, expr) => Self::expression_plan_columns(expr)?,
            Expression::Column(_) => vec![expr.clone()],
            Expression::Literal(_) => vec![],
            Expression::UnaryExpression { expr, .. } => Self::expression_plan_columns(expr)?,
            Expression::BinaryExpression { left, right, .. } => {
                let mut l = Self::expression_plan_columns(left)?;
                let mut r = Self::expression_plan_columns(right)?;
                l.append(&mut r);
                l
            }
            Expression::ScalarFunction { args, .. } => {
                let mut v = vec![];
                for arg in args {
                    let mut col = Self::expression_plan_columns(arg)?;
                    v.append(&mut col);
                }
                v
            }
            Expression::AggregateFunction { args, .. } => {
                let mut v = vec![];
                for arg in args {
                    let mut col = Self::expression_plan_columns(arg)?;
                    v.append(&mut col);
                }
                v
            }
            Expression::Wildcard => vec![],
            Expression::Sort { expr, .. } => Self::expression_plan_columns(expr)?,
            Expression::Cast { expr, .. } => Self::expression_plan_columns(expr)?
        })
    }

    /// Collect all unique projection fields to a map.
    fn projections_to_map(plan: &PlanNode, map: &mut HashMap<String, Expression>) -> Result<()> {
        match plan {
            PlanNode::Projection(v) => {
                v.schema.fields().iter().enumerate().for_each(|(i, field)| {
                    let expr = match &v.expr[i] {
                        Expression::Alias(_alias, plan) => plan.as_ref().clone(),
                        other => other.clone()
                    };
                    map.insert(field.name().clone(), expr);
                })
            }
            // Aggregator aggr_expr is the projection
            PlanNode::AggregatorPartial(v) => {
                for expr in &v.aggr_expr {
                    let column_name = expr.column_name();
                    map.insert(column_name, expr.clone());
                }
            }
            // Aggregator aggr_expr is the projection
            PlanNode::AggregatorFinal(v) => {
                for expr in &v.aggr_expr {
                    let column_name = expr.column_name();
                    map.insert(column_name, expr.clone());
                }
            }
            other => {
                for child in other.inputs() {
                    Self::projections_to_map(child.as_ref(), map)?;
                }
            }
        }
        Ok(())
    }

    fn rebuild_from_exprs(expr: &Expression, expressions: &[Expression]) -> Expression {
        match expr {
            Expression::Alias(alias, _) => {
                Expression::Alias(alias.clone(), Box::from(expressions[0].clone()))
            }
            Expression::Column(_) => expr.clone(),
            Expression::Literal(_) => expr.clone(),
            Expression::BinaryExpression { op, .. } => Expression::BinaryExpression {
                left: Box::new(expressions[0].clone()),
                op: op.clone(),
                right: Box::new(expressions[1].clone())
            },
            Expression::ScalarFunction { op, .. } => Expression::ScalarFunction {
                op: op.clone(),
                args: expressions.to_vec()
            },
            Expression::AggregateFunction { op, .. } => Expression::AggregateFunction {
                op: op.clone(),
                args: expressions.to_vec()
            },
            other => other.clone()
        }
    }

    /// Check if aggr is in group-by's list
    /// Case1: group is a column, the name needs to match with aggr
    /// Case2: aggr is an alias, unfold aggr
    /// Case3: group and aggr are exactly the same expression
    pub fn check_aggr_in_group_expr(
        aggr: &Expression,
        group_by_names: &HashSet<String>,
        input_schema: &DataSchemaRef
    ) -> Result<bool> {
        match aggr {
            Expression::Alias(alias, plan) => {
                if group_by_names.contains(alias) {
                    return Ok(true);
                } else {
                    return Self::check_aggr_in_group_expr(plan, group_by_names, input_schema);
                }
            }
            _ => {
                let aggr_str = format!("{:?}", aggr);
                if group_by_names.contains(&aggr_str) {
                    return Ok(true);
                } else {
                    let columns = Self::expression_plan_columns(aggr)?;
                    for col in columns {
                        let cn = col.column_name();
                        if !group_by_names.contains(&cn) {
                            return Ok(false);
                        }
                    }
                }
            }
        };
        Ok(true)
    }

    pub fn exprs_to_fields(
        exprs: &[Expression],
        input_schema: &DataSchemaRef
    ) -> Result<Vec<DataField>> {
        exprs
            .iter()
            .map(|expr| expr.to_data_field(input_schema))
            .collect::<Result<_>>()
    }

    pub fn exprs_to_names(exprs: &[Expression], names: &mut HashSet<String>) -> Result<()> {
        for expr in exprs {
            let name = format!("{:?}", expr);
            names.insert(name.clone());
        }
        Ok(())
    }
}
