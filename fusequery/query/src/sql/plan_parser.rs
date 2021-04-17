// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataValue;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::ExplainPlan;
use common_planners::ExpressionPlan;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_planners::SettingPlan;
use common_planners::StageState;
use common_planners::VarValue;
use sqlparser::ast::FunctionArg;
use sqlparser::ast::OrderByExpr;
use sqlparser::ast::Statement;
use sqlparser::ast::TableFactor;

use crate::datasources::ITable;
use crate::sessions::FuseQueryContextRef;
use crate::sql::make_data_type;
use crate::sql::make_sql_interval_to_literal;
use crate::sql::sql_statement::DfCreateTable;
use crate::sql::DfCreateDatabase;
use crate::sql::DfExplain;
use crate::sql::DfParser;
use crate::sql::DfStatement;

pub struct PlanParser {
    ctx: FuseQueryContextRef,
}

impl PlanParser {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        Self { ctx }
    }

    pub fn build_from_sql(&self, query: &str) -> Result<PlanNode> {
        let statements = DfParser::parse_sql(query)?;
        if statements.len() != 1 {
            bail!("Only support single query");
        }
        self.statement_to_plan(&statements[0])
    }

    pub fn statement_to_plan(&self, statement: &DfStatement) -> Result<PlanNode> {
        match statement {
            DfStatement::Statement(v) => self.sql_statement_to_plan(&v),
            DfStatement::Explain(v) => self.sql_explain_to_plan(&v),
            DfStatement::CreateDatabase(v) => self.sql_create_database_to_plan(&v),
            DfStatement::CreateTable(v) => self.sql_create_table_to_plan(&v),

            // TODO: support like and other filters in show queries
            DfStatement::ShowTables(_) => self.build_from_sql(
                format!(
                    "SELECT name FROM system.tables where database = '{}'",
                    self.ctx.get_default_db()?
                )
                .as_str(),
            ),
            DfStatement::ShowSettings(_) => self.build_from_sql("SELECT name FROM system.settings"),
        }
    }

    /// Builds plan from AST statement.
    pub fn sql_statement_to_plan(&self, statement: &sqlparser::ast::Statement) -> Result<PlanNode> {
        match statement {
            Statement::Query(query) => self.query_to_plan(query),
            Statement::SetVariable {
                variable, value, ..
            } => self.set_variable_to_plan(variable, value),
            _ => bail!("Unsupported statement {:?}", statement),
        }
    }

    /// Generate a logic plan from an EXPLAIN
    pub fn sql_explain_to_plan(&self, explain: &DfExplain) -> Result<PlanNode> {
        let plan = self.sql_statement_to_plan(&explain.statement)?;
        Ok(PlanNode::Explain(ExplainPlan {
            typ: explain.typ,
            input: Arc::new(plan),
        }))
    }

    pub fn sql_create_database_to_plan(&self, create: &DfCreateDatabase) -> Result<PlanNode> {
        if create.name.0.is_empty() {
            bail!("Create database name is empty");
        }
        let db = create.name.0[0].value.clone();

        let mut options = HashMap::new();
        for p in create.options.iter() {
            options.insert(p.name.value.to_lowercase(), p.value.to_string());
        }

        Ok(PlanNode::CreateDatabase(CreateDatabasePlan {
            if_not_exists: create.if_not_exists,
            db,
            engine: create.engine,
            options,
        }))
    }

    pub fn sql_create_table_to_plan(&self, create: &DfCreateTable) -> Result<PlanNode> {
        let mut db = self.ctx.get_default_db()?;
        if create.name.0.is_empty() {
            bail!("Create table name is empty");
        }
        let mut table = create.name.0[0].value.clone();
        if create.name.0.len() > 1 {
            db = table;
            table = create.name.0[1].value.clone();
        }

        let mut fields = vec![];
        for col in create.columns.iter() {
            fields.push(DataField::new(
                &col.name.value,
                make_data_type(&col.data_type)?,
                false,
            ));
        }

        let mut options = HashMap::new();
        for p in create.options.iter() {
            options.insert(p.name.value.to_lowercase(), p.value.to_string());
        }

        // Schema with metadata(due to serde must set metadata).
        let schema = Arc::new(DataSchema::new_with_metadata(
            fields,
            [("Key".to_string(), "Value".to_string())]
                .iter()
                .cloned()
                .collect(),
        ));
        Ok(PlanNode::CreateTable(CreateTablePlan {
            if_not_exists: create.if_not_exists,
            db,
            table,
            schema,
            engine: create.engine,
            options,
        }))
    }

    /// Generate a logic plan from an SQL query
    pub fn query_to_plan(&self, query: &sqlparser::ast::Query) -> Result<PlanNode> {
        match &query.body {
            sqlparser::ast::SetExpr::Select(s) => {
                self.select_to_plan(s.as_ref(), &query.limit, &query.order_by)
            }
            _ => bail!("Query {} not implemented yet", query.body),
        }
    }

    /// Generate a logic plan from an SQL select
    fn select_to_plan(
        &self,
        select: &sqlparser::ast::Select,
        limit: &Option<sqlparser::ast::Expr>,
        order_by: &[OrderByExpr],
    ) -> Result<PlanNode> {
        if select.having.is_some() {
            bail!("HAVING is not implemented yet");
        }

        // from.
        let plan = self.plan_tables_with_joins(&select.from)?;

        // filter (also known as selection) first
        let plan = self.filter(&plan, &select.selection)?;

        // projection.
        let projection_expr: Vec<ExpressionPlan> = select
            .projection
            .iter()
            .map(|e| self.sql_select_to_rex(&e, &plan.schema()))
            .collect::<Result<Vec<ExpressionPlan>>>()?;

        // Aggregator check.
        let mut has_aggregator = false;
        for expr in &projection_expr {
            if expr.has_aggregator()? {
                has_aggregator = true;
                break;
            }
        }

        let plan = if !select.group_by.is_empty() || has_aggregator {
            self.aggregate(&plan, projection_expr, &select.group_by)?
        } else {
            self.project(&plan, projection_expr)?
        };

        // order by
        let plan = self.sort(&plan, order_by)?;

        // limit.
        let plan = self.limit(&plan, limit)?;

        Ok(PlanNode::Select(SelectPlan {
            input: Arc::new(plan),
        }))
    }

    /// Generate a relational expression from a select SQL expression
    fn sql_select_to_rex(
        &self,
        sql: &sqlparser::ast::SelectItem,
        schema: &DataSchema,
    ) -> Result<ExpressionPlan> {
        match sql {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => self.sql_to_rex(expr, schema),
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => Ok(ExpressionPlan::Alias(
                alias.value.clone(),
                Box::new(self.sql_to_rex(&expr, schema)?),
            )),
            sqlparser::ast::SelectItem::Wildcard => Ok(ExpressionPlan::Wildcard),
            _ => bail!("SelectItem: {:?} are not supported", sql),
        }
    }

    fn plan_tables_with_joins(&self, from: &[sqlparser::ast::TableWithJoins]) -> Result<PlanNode> {
        match from.len() {
            0 => self.plan_with_dummy_source(),
            1 => self.plan_table_with_joins(&from[0]),
            _ => bail!("Cannot support JOIN clause"),
        }
    }

    fn plan_with_dummy_source(&self) -> Result<PlanNode> {
        let db_name = "system";
        let table_name = "one";
        let table = self.ctx.get_table(db_name, table_name)?;
        let schema = table.schema()?;

        let scan =
            PlanBuilder::scan(db_name, table_name, schema.as_ref(), None, None, None)?.build()?;
        let datasource_plan = table.read_plan(self.ctx.clone(), scan)?;
        Ok(PlanNode::ReadSource(datasource_plan))
    }

    fn plan_table_with_joins(&self, t: &sqlparser::ast::TableWithJoins) -> Result<PlanNode> {
        self.create_relation(&t.relation)
    }

    fn create_relation(&self, relation: &sqlparser::ast::TableFactor) -> Result<PlanNode> {
        match relation {
            sqlparser::ast::TableFactor::Table { name, args, .. } => {
                let mut db_name = self.ctx.get_default_db()?;
                let mut table_name = name.to_string();
                if name.0.len() == 2 {
                    db_name = name.0[0].to_string();
                    table_name = name.0[1].to_string();
                }
                let mut table_args = None;
                let table: Arc<dyn ITable>;

                // only table functions has table args
                if !args.is_empty() {
                    if name.0.len() >= 2 {
                        bail!("Currently table can't have arguments")
                    }

                    let empty_schema = Arc::new(DataSchema::empty());
                    match &args[0] {
                        FunctionArg::Named { arg, .. } => {
                            table_args = Some(self.sql_to_rex(&arg, empty_schema.as_ref())?);
                        }
                        FunctionArg::Unnamed(arg) => {
                            table_args = Some(self.sql_to_rex(&arg, empty_schema.as_ref())?);
                        }
                    }
                    let table_function = self.ctx.get_table_function(&table_name)?;
                    table_name = table_function.name().to_string();
                    db_name = table_function.db().to_string();
                    table = table_function.as_table();
                } else {
                    table = self.ctx.get_table(&db_name, table_name.as_str())?;
                }

                let schema = table.schema()?;
                let scan = PlanBuilder::scan(
                    &db_name,
                    &table_name,
                    schema.as_ref(),
                    None,
                    table_args,
                    None,
                )?
                .build()?;
                let datasource_plan = table.read_plan(self.ctx.clone(), scan)?;
                Ok(PlanNode::ReadSource(datasource_plan))
            }
            sqlparser::ast::TableFactor::Derived { subquery, .. } => self.query_to_plan(subquery),
            sqlparser::ast::TableFactor::NestedJoin(table_with_joins) => {
                self.plan_table_with_joins(table_with_joins)
            }
            TableFactor::TableFunction { .. } => bail!("Unsupported table function"),
        }
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(
        &self,
        expr: &sqlparser::ast::Expr,
        schema: &DataSchema,
    ) -> Result<ExpressionPlan> {
        match expr {
            sqlparser::ast::Expr::Identifier(ref v) => Ok(ExpressionPlan::Column(v.clone().value)),
            sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
                Ok(ExpressionPlan::Literal(DataValue::try_from_literal(n)?))
            }
            sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => {
                Ok(ExpressionPlan::Literal(DataValue::Utf8(Some(s.clone()))))
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
            sqlparser::ast::Expr::TypedString { data_type, value } => Ok(ExpressionPlan::Cast {
                expr: Box::new(ExpressionPlan::Literal(DataValue::Utf8(Some(
                    value.clone(),
                )))),
                data_type: make_data_type(data_type)?,
            }),
            sqlparser::ast::Expr::Cast { expr, data_type } => Ok(ExpressionPlan::Cast {
                expr: Box::from(self.sql_to_rex(expr, schema)?),
                data_type: make_data_type(data_type)?,
            }),
            sqlparser::ast::Expr::Value(sqlparser::ast::Value::Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            }) => make_sql_interval_to_literal(
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            ),
            other => bail!("Unsupported expression: {}, type: {:?}", expr, other),
        }
    }

    pub fn set_variable_to_plan(
        &self,
        variable: &sqlparser::ast::Ident,
        values: &[sqlparser::ast::SetVariableValue],
    ) -> Result<PlanNode> {
        let mut vars = vec![];
        for value in values {
            let variable = variable.value.clone();
            let value = match value {
                sqlparser::ast::SetVariableValue::Ident(v) => v.value.clone(),
                sqlparser::ast::SetVariableValue::Literal(v) => v.to_string(),
            };
            vars.push(VarValue { variable, value });
        }
        Ok(PlanNode::SetVariable(SettingPlan { vars }))
    }

    /// Apply a filter to the plan
    fn filter(
        &self,
        plan: &PlanNode,
        predicate: &Option<sqlparser::ast::Expr>,
    ) -> Result<PlanNode> {
        match *predicate {
            Some(ref predicate_expr) => Ok(PlanBuilder::from(&plan)
                .filter(self.sql_to_rex(predicate_expr, &plan.schema())?)?
                .build()?),
            _ => Ok(plan.clone()),
        }
    }

    /// Wrap a plan in a projection
    fn project(&self, input: &PlanNode, expr: Vec<ExpressionPlan>) -> Result<PlanNode> {
        PlanBuilder::from(input).project(expr)?.build()
    }

    /// Wrap a plan for an aggregate
    fn aggregate(
        &self,
        input: &PlanNode,
        aggr_expr: Vec<ExpressionPlan>,
        group_by: &[sqlparser::ast::Expr],
    ) -> Result<PlanNode> {
        let group_expr: Vec<ExpressionPlan> = group_by
            .iter()
            .map(|e| self.sql_to_rex(&e, &input.schema()))
            .collect::<Result<Vec<ExpressionPlan>>>()?;

        // S0: Apply a partial aggregator plan.
        // S1: Apply a fragment plan for distributed planners split.
        // S2: Apply a final aggregator plan.
        PlanBuilder::from(&input)
            .aggregate_partial(aggr_expr.clone(), group_expr.clone())?
            .stage(self.ctx.get_id()?, StageState::AggregatorMerge)?
            .aggregate_final(aggr_expr, group_expr)?
            .build()
    }

    fn sort(&self, input: &PlanNode, order_by: &[OrderByExpr]) -> Result<PlanNode> {
        if order_by.is_empty() {
            return Ok(input.clone());
        }

        let order_by_exprs: Vec<ExpressionPlan> = order_by
            .iter()
            .map(|e| -> Result<ExpressionPlan> {
                Ok(ExpressionPlan::Sort {
                    expr: Box::new(self.sql_to_rex(&e.expr, &input.schema())?),
                    asc: e.asc.unwrap_or(true),
                    nulls_first: e.nulls_first.unwrap_or(true),
                })
            })
            .collect::<Result<Vec<ExpressionPlan>>>()?;

        PlanBuilder::from(&input).sort(&order_by_exprs)?.build()
    }

    /// Wrap a plan in a limit
    fn limit(&self, input: &PlanNode, limit: &Option<sqlparser::ast::Expr>) -> Result<PlanNode> {
        match *limit {
            Some(ref limit_expr) => {
                let n = match self.sql_to_rex(&limit_expr, &input.schema())? {
                    ExpressionPlan::Literal(DataValue::UInt64(Some(n))) => Ok(n as usize),
                    _ => Err(anyhow!("Unexpected expression for LIMIT clause")),
                }?;
                Ok(PlanBuilder::from(&input).limit(n)?.build()?)
            }
            _ => Ok(input.clone()),
        }
    }
}
