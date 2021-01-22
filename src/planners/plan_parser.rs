// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::contexts::FuseQueryContextRef;
use crate::datavalues::{DataSchema, DataValue};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::{
    DFExplainPlan, DFParser, DFStatement, ExplainPlan, ExpressionPlan, PlanBuilder, PlanNode,
    Planner, SelectPlan, SettingPlan,
};
use sqlparser::ast::{FunctionArg, Statement, TableFactor};

impl Planner {
    pub fn build_from_sql(
        &self,
        ctx: FuseQueryContextRef,
        query: &str,
    ) -> FuseQueryResult<PlanNode> {
        let statements = DFParser::parse_sql(query)?;
        if statements.len() != 1 {
            return Err(FuseQueryError::Internal(
                "Only support single query".to_string(),
            ));
        }
        self.statement_to_plan(ctx, &statements[0])
    }

    pub fn statement_to_plan(
        &self,
        ctx: FuseQueryContextRef,
        statement: &DFStatement,
    ) -> FuseQueryResult<PlanNode> {
        match statement {
            DFStatement::Statement(v) => self.sql_statement_to_plan(ctx, &v),
            DFStatement::Explain(v) => self.sql_explain_to_plan(ctx, &v),
            _ => Err(FuseQueryError::Internal(
                "Only Statement are implemented".to_string(),
            )),
        }
    }

    /// Builds plan from AST statement.
    pub fn sql_statement_to_plan(
        &self,
        ctx: FuseQueryContextRef,
        statement: &sqlparser::ast::Statement,
    ) -> FuseQueryResult<PlanNode> {
        match statement {
            Statement::Query(query) => self.query_to_plan(ctx, query),
            Statement::SetVariable {
                variable, value, ..
            } => self.set_variable_to_plan(ctx, variable, value),
            _ => Err(FuseQueryError::Internal(format!(
                "Unsupported statement {:?}",
                statement
            ))),
        }
    }

    /// Generate a logic plan from an EXPLAIN
    pub fn sql_explain_to_plan(
        &self,
        ctx: FuseQueryContextRef,
        explain: &DFExplainPlan,
    ) -> FuseQueryResult<PlanNode> {
        let plan = self.sql_statement_to_plan(ctx, &explain.statement)?;
        Ok(PlanNode::Explain(ExplainPlan {
            typ: explain.typ,
            plan: Box::new(plan),
        }))
    }

    /// Generate a logic plan from an SQL query
    pub fn query_to_plan(
        &self,
        ctx: FuseQueryContextRef,
        query: &sqlparser::ast::Query,
    ) -> FuseQueryResult<PlanNode> {
        match &query.body {
            sqlparser::ast::SetExpr::Select(s) => {
                self.select_to_plan(ctx, s.as_ref(), &query.limit)
            }
            _ => Err(FuseQueryError::Internal(format!(
                "Query {} not implemented yet",
                query.body
            ))),
        }
    }

    /// Generate a logic plan from an SQL select
    fn select_to_plan(
        &self,
        ctx: FuseQueryContextRef,
        select: &sqlparser::ast::Select,
        limit: &Option<sqlparser::ast::Expr>,
    ) -> FuseQueryResult<PlanNode> {
        if select.having.is_some() {
            return Err(FuseQueryError::Internal(
                "HAVING is not implemented yet".to_string(),
            ));
        }

        // from.
        let plan = self.plan_tables_with_joins(ctx, &select.from)?;

        // filter (also known as selection) first
        let plan = self.filter(&plan, &select.selection)?;

        // projection.
        let projection_expr: Vec<ExpressionPlan> = select
            .projection
            .iter()
            .map(|e| self.sql_select_to_rex(&e, &plan.schema()))
            .collect::<FuseQueryResult<Vec<ExpressionPlan>>>()?;

        let aggr_expr: Vec<ExpressionPlan> = projection_expr
            .iter()
            .filter(|x| x.is_aggregate())
            .cloned()
            .collect();

        let plan = if !select.group_by.is_empty() || !aggr_expr.is_empty() {
            self.aggregate(&plan, projection_expr, aggr_expr, &select.group_by)?
        } else {
            self.project(&plan, projection_expr)?
        };

        // limit.
        let plan = self.limit(&plan, limit)?;

        Ok(PlanNode::Select(SelectPlan {
            plan: Box::new(plan),
        }))
    }

    /// Generate a relational expression from a select SQL expression
    fn sql_select_to_rex(
        &self,
        sql: &sqlparser::ast::SelectItem,
        schema: &DataSchema,
    ) -> FuseQueryResult<ExpressionPlan> {
        match sql {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => self.sql_to_rex(expr, schema),
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => Ok(ExpressionPlan::Alias(
                alias.value.clone(),
                Box::new(self.sql_to_rex(&expr, schema)?),
            )),
            sqlparser::ast::SelectItem::Wildcard => Ok(ExpressionPlan::Wildcard),
            _ => Err(FuseQueryError::Internal(format!(
                "SelectItem: {:?} are not supported",
                sql
            ))),
        }
    }

    fn plan_tables_with_joins(
        &self,
        ctx: FuseQueryContextRef,
        from: &[sqlparser::ast::TableWithJoins],
    ) -> FuseQueryResult<PlanNode> {
        match from.len() {
            0 => Ok(PlanBuilder::empty().build()?),
            1 => self.plan_table_with_joins(ctx, &from[0]),
            _ => Err(FuseQueryError::Internal(
                "Cannot support JOIN clause".to_string(),
            )),
        }
    }

    fn plan_table_with_joins(
        &self,
        ctx: FuseQueryContextRef,
        t: &sqlparser::ast::TableWithJoins,
    ) -> FuseQueryResult<PlanNode> {
        self.create_relation(ctx, &t.relation)
    }

    fn create_relation(
        &self,
        ctx: FuseQueryContextRef,
        relation: &sqlparser::ast::TableFactor,
    ) -> FuseQueryResult<PlanNode> {
        match relation {
            sqlparser::ast::TableFactor::Table { name, args, .. } => {
                let mut db_name = ctx.get_default_db()?;
                let mut table_name = name.to_string();
                if name.0.len() == 2 {
                    db_name = name.0[0].to_string();
                    table_name = name.0[1].to_string();
                }
                let table = ctx.get_table(&db_name, table_name.as_str())?;
                let schema = table.schema()?;

                let mut table_args = None;
                if !args.is_empty() {
                    match &args[0] {
                        FunctionArg::Named { arg, .. } => {
                            table_args = Some(self.sql_to_rex(&arg, &schema)?);
                        }
                        FunctionArg::Unnamed(arg) => {
                            table_args = Some(self.sql_to_rex(&arg, &schema)?);
                        }
                    }
                }

                let scan =
                    PlanBuilder::scan(&db_name, &table_name, schema.as_ref(), None, table_args)?
                        .build()?;
                let datasource_plan = table.read_plan(ctx, scan)?;
                Ok(PlanNode::ReadSource(datasource_plan))
            }
            sqlparser::ast::TableFactor::Derived { subquery, .. } => {
                self.query_to_plan(ctx, subquery)
            }
            sqlparser::ast::TableFactor::NestedJoin(table_with_joins) => {
                self.plan_table_with_joins(ctx, table_with_joins)
            }
            TableFactor::TableFunction { .. } => Err(FuseQueryError::Internal(
                "Unsupported table function".to_string(),
            )),
        }
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(
        &self,
        sql: &sqlparser::ast::Expr,
        schema: &DataSchema,
    ) -> FuseQueryResult<ExpressionPlan> {
        match sql {
            sqlparser::ast::Expr::Identifier(ref v) => Ok(ExpressionPlan::Field(v.clone().value)),
            sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n)) => match n.parse::<i64>()
            {
                Ok(n) => {
                    if n >= 0 {
                        Ok(ExpressionPlan::Constant(DataValue::UInt64(Some(n as u64))))
                    } else {
                        Ok(ExpressionPlan::Constant(DataValue::Int64(Some(n))))
                    }
                }
                Err(_) => Ok(ExpressionPlan::Constant(DataValue::Float64(Some(
                    n.parse::<f64>()?,
                )))),
            },
            sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => {
                Ok(ExpressionPlan::Constant(DataValue::String(Some(s.clone()))))
            }
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                Ok(ExpressionPlan::BinaryExpression {
                    op: format!("{}", op),
                    left: Box::new(self.sql_to_rex(left, schema)?),
                    right: Box::new(self.sql_to_rex(right, schema)?),
                })
            }
            sqlparser::ast::Expr::Nested(e) => self.sql_to_rex(e, schema),
            sqlparser::ast::Expr::Function(e) => {
                let mut args = Vec::with_capacity(e.args.len());
                for arg in &e.args {
                    match &arg {
                        FunctionArg::Named { arg, .. } => {
                            args.push(self.sql_to_rex(arg, schema)?);
                        }
                        FunctionArg::Unnamed(arg) => {
                            args.push(self.sql_to_rex(arg, schema)?);
                        }
                    }
                }
                Ok(ExpressionPlan::Function {
                    op: e.name.to_string(),
                    args,
                })
            }
            sqlparser::ast::Expr::Wildcard => Ok(ExpressionPlan::Wildcard),
            _ => Err(FuseQueryError::Plan(format!(
                "Unsupported ExpressionPlan: {}",
                sql
            ))),
        }
    }

    pub fn set_variable_to_plan(
        &self,
        _ctx: FuseQueryContextRef,
        variable: &sqlparser::ast::Ident,
        value: &sqlparser::ast::SetVariableValue,
    ) -> FuseQueryResult<PlanNode> {
        let variable = variable.value.clone();
        let value = match value {
            sqlparser::ast::SetVariableValue::Ident(v) => v.value.clone(),
            sqlparser::ast::SetVariableValue::Literal(v) => v.to_string(),
        };
        Ok(PlanNode::SetVariable(SettingPlan { variable, value }))
    }

    /// Apply a filter to the plan
    fn filter(
        &self,
        plan: &PlanNode,
        predicate: &Option<sqlparser::ast::Expr>,
    ) -> FuseQueryResult<PlanNode> {
        match *predicate {
            Some(ref predicate_expr) => PlanBuilder::from(&plan)
                .filter(self.sql_to_rex(predicate_expr, &plan.schema())?)?
                .build(),
            _ => Ok(plan.clone()),
        }
    }

    /// Wrap a plan in a projection
    fn project(&self, input: &PlanNode, expr: Vec<ExpressionPlan>) -> FuseQueryResult<PlanNode> {
        PlanBuilder::from(input).project(expr)?.build()
    }

    /// Wrap a plan in an aggregate
    fn aggregate(
        &self,
        input: &PlanNode,
        projection_expr: Vec<ExpressionPlan>,
        aggr_expr: Vec<ExpressionPlan>,
        group_by: &[sqlparser::ast::Expr],
    ) -> FuseQueryResult<PlanNode> {
        let group_expr: Vec<ExpressionPlan> = group_by
            .iter()
            .map(|e| self.sql_to_rex(&e, &input.schema()))
            .collect::<FuseQueryResult<Vec<ExpressionPlan>>>()?;

        let group_by_count = group_expr.len();
        let aggr_count = aggr_expr.len();

        if group_by_count + aggr_count != projection_expr.len() {
            return Err(FuseQueryError::Plan(
                "Projection references non-aggregate values".to_owned(),
            ));
        }

        PlanBuilder::from(&input)
            .aggregate(group_expr, aggr_expr)?
            .build()
    }

    /// Wrap a plan in a limit
    fn limit(
        &self,
        input: &PlanNode,
        limit: &Option<sqlparser::ast::Expr>,
    ) -> FuseQueryResult<PlanNode> {
        match *limit {
            Some(ref limit_expr) => {
                let n = match self.sql_to_rex(&limit_expr, &input.schema())? {
                    ExpressionPlan::Constant(DataValue::UInt64(Some(n))) => Ok(n as usize),
                    _ => Err(FuseQueryError::Plan(
                        "Unexpected expression for LIMIT clause".to_string(),
                    )),
                }?;
                PlanBuilder::from(&input).limit(n)?.build()
            }
            _ => Ok(input.clone()),
        }
    }
}
