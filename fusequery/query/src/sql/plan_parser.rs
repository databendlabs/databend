// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use common_aggregate_functions::AggregateFunctionFactory;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::StringArray;
use common_arrow::arrow::datatypes::Field;
use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_planners::ExplainPlan;
use common_planners::Expression;
use common_planners::InsertIntoPlan;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_planners::SettingPlan;
use common_planners::StageState;
use common_planners::UseDatabasePlan;
use common_planners::VarValue;
use sqlparser::ast::Expr;
use sqlparser::ast::FunctionArg;
// use sqlparser::ast::JoinConstraint;
// use sqlparser::ast::JoinOperator;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::OrderByExpr;
use sqlparser::ast::Query;
use sqlparser::ast::Statement;

use super::expr_common::rebase_expr_from_input;
use crate::datasources::ITable;
use crate::functions::ContextFunction;
use crate::sessions::FuseQueryContextRef;
use crate::sql::expr_common::expand_aggregate_arg_exprs;
use crate::sql::expr_common::expand_wildcard;
use crate::sql::expr_common::expr_as_column_expr;
use crate::sql::expr_common::extract_aliases;
use crate::sql::expr_common::find_aggregate_exprs;
use crate::sql::expr_common::find_columns_not_satisfy_exprs;
use crate::sql::expr_common::rebase_expr;
use crate::sql::expr_common::resolve_aliases_to_exprs;
use crate::sql::expr_common::sort_to_inner_expr;
use crate::sql::expr_common::unwrap_alias_exprs;
use crate::sql::sql_statement::DfCreateTable;
use crate::sql::sql_statement::DfDropDatabase;
use crate::sql::sql_statement::DfUseDatabase;
use crate::sql::DfCreateDatabase;
use crate::sql::DfDropTable;
use crate::sql::DfExplain;
use crate::sql::DfParser;
use crate::sql::DfStatement;
use crate::sql::SQLCommon;

pub struct PlanParser {
    ctx: FuseQueryContextRef
}

impl PlanParser {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        Self { ctx }
    }

    pub fn build_from_sql(&self, query: &str) -> Result<PlanNode> {
        DfParser::parse_sql(query).and_then(|statement| {
            statement
                .first()
                .map(|statement| self.statement_to_plan(&statement))
                .unwrap_or_else(|| {
                    Result::Err(ErrorCodes::SyntaxException("Only support single query"))
                })
        })
    }

    pub fn statement_to_plan(&self, statement: &DfStatement) -> Result<PlanNode> {
        match statement {
            DfStatement::Statement(v) => self.sql_statement_to_plan(&v),
            DfStatement::Explain(v) => self.sql_explain_to_plan(&v),
            DfStatement::ShowDatabases(_) => {
                self.build_from_sql("SELECT name FROM system.databases ORDER BY name")
            }
            DfStatement::CreateDatabase(v) => self.sql_create_database_to_plan(&v),
            DfStatement::DropDatabase(v) => self.sql_drop_database_to_plan(&v),
            DfStatement::CreateTable(v) => self.sql_create_table_to_plan(&v),
            DfStatement::DropTable(v) => self.sql_drop_table_to_plan(&v),
            DfStatement::UseDatabase(v) => self.sql_use_database_to_plan(&v),

            // TODO: support like and other filters in show queries
            DfStatement::ShowTables(_) => self.build_from_sql(
                format!(
                    "SELECT name FROM system.tables where database = '{}' ORDER BY database, name",
                    self.ctx.get_current_database()
                )
                .as_str()
            ),
            DfStatement::ShowSettings(_) => self.build_from_sql("SELECT name FROM system.settings")
        }
    }

    /// Builds plan from AST statement.
    pub fn sql_statement_to_plan(&self, statement: &sqlparser::ast::Statement) -> Result<PlanNode> {
        match statement {
            Statement::Query(query) => self.query_to_plan(query),
            Statement::SetVariable {
                variable, value, ..
            } => self.set_variable_to_plan(variable, value),

            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => self.insert_to_plan(table_name, columns, source),

            _ => Result::Err(ErrorCodes::SyntaxException(format!(
                "Unsupported statement {:?}",
                statement
            )))
        }
    }

    /// Generate a logic plan from an EXPLAIN
    pub fn sql_explain_to_plan(&self, explain: &DfExplain) -> Result<PlanNode> {
        let plan = self.sql_statement_to_plan(&explain.statement)?;
        Ok(PlanNode::Explain(ExplainPlan {
            typ: explain.typ,
            input: Arc::new(plan)
        }))
    }

    /// DfCreateDatabase to plan.
    pub fn sql_create_database_to_plan(&self, create: &DfCreateDatabase) -> Result<PlanNode> {
        if create.name.0.is_empty() {
            return Result::Err(ErrorCodes::SyntaxException("Create database name is empty"));
        }
        let name = create.name.0[0].value.clone();

        let mut options = HashMap::new();
        for p in create.options.iter() {
            options.insert(p.name.value.to_lowercase(), p.value.to_string());
        }

        Ok(PlanNode::CreateDatabase(CreateDatabasePlan {
            if_not_exists: create.if_not_exists,
            db: name,
            engine: create.engine,
            options
        }))
    }

    /// DfDropDatabase to plan.
    pub fn sql_drop_database_to_plan(&self, drop: &DfDropDatabase) -> Result<PlanNode> {
        if drop.name.0.is_empty() {
            return Result::Err(ErrorCodes::SyntaxException("Drop database name is empty"));
        }
        let name = drop.name.0[0].value.clone();

        Ok(PlanNode::DropDatabase(DropDatabasePlan {
            if_exists: drop.if_exists,
            db: name
        }))
    }

    pub fn sql_use_database_to_plan(&self, use_db: &DfUseDatabase) -> Result<PlanNode> {
        let db = use_db.name.0[0].value.clone();
        Ok(PlanNode::UseDatabase(UseDatabasePlan { db }))
    }

    pub fn sql_create_table_to_plan(&self, create: &DfCreateTable) -> Result<PlanNode> {
        let mut db = self.ctx.get_current_database();
        if create.name.0.is_empty() {
            return Result::Err(ErrorCodes::SyntaxException("Create table name is empty"));
        }
        let mut table = create.name.0[0].value.clone();
        if create.name.0.len() > 1 {
            db = table;
            table = create.name.0[1].value.clone();
        }

        let fields = create
            .columns
            .iter()
            .map(|column| {
                SQLCommon::make_data_type(&column.data_type)
                    .map(|data_type| DataField::new(&column.name.value, data_type, false))
            })
            .collect::<Result<Vec<Field>>>()?;

        let mut options = HashMap::new();
        for p in create.options.iter() {
            options.insert(
                p.name.value.to_lowercase(),
                p.value
                    .to_string()
                    .trim_matches(|s| s == '\'' || s == '"')
                    .to_string()
            );
        }

        let schema = DataSchemaRefExt::create(fields);
        Ok(PlanNode::CreateTable(CreateTablePlan {
            if_not_exists: create.if_not_exists,
            db,
            table,
            schema,
            engine: create.engine,
            options
        }))
    }

    /// DfDropTable to plan.
    pub fn sql_drop_table_to_plan(&self, drop: &DfDropTable) -> Result<PlanNode> {
        let mut db = self.ctx.get_current_database();
        if drop.name.0.is_empty() {
            return Result::Err(ErrorCodes::SyntaxException("Drop table name is empty"));
        }
        let mut table = drop.name.0[0].value.clone();
        if drop.name.0.len() > 1 {
            db = table;
            table = drop.name.0[1].value.clone();
        }
        Ok(PlanNode::DropTable(DropTablePlan {
            if_exists: drop.if_exists,
            db,
            table
        }))
    }

    fn insert_to_plan(
        &self,
        table_name: &ObjectName,
        columns: &[Ident],
        source: &Query
    ) -> Result<PlanNode> {
        if let sqlparser::ast::SetExpr::Values(ref vs) = source.body {
            //            let col_num = columns.len();
            let db_name = self.ctx.get_current_database();
            let tbl_name = table_name
                .0
                .get(0)
                .ok_or_else(|| ErrorCodes::SyntaxException("empty table name now allowed"))?
                .value
                .clone();

            let values = &vs.0;
            if values.is_empty() {
                return Err(ErrorCodes::EmptyData(
                    "empty values for insertion is not allowed"
                ));
            }

            let all_value = values
                .iter()
                .all(|row| row.iter().all(|item| matches!(item, Expr::Value(_))));
            if !all_value {
                return Err(ErrorCodes::UnImplement(
                    "not support value expressions other than literal value yet"
                ));
            }
            // Buffers some chunks if possible
            let chunks = values.chunks(100);
            let fields = columns
                .iter()
                .map(|ident| Field::new(&ident.value, DataType::Utf8, true))
                .collect::<Vec<_>>();
            let schema = DataSchemaRefExt::create(fields);

            let blocks: Vec<DataBlock> = chunks
                .map(|chunk| {
                    let transposed: Vec<Vec<String>> = (0..chunk[0].len())
                        .map(|i| {
                            chunk
                                .iter()
                                .map(|inner| match &inner[i] {
                                    Expr::Value(v) => v.to_string(),
                                    _ => "N/A".to_string()
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect();

                    let cols = transposed
                        .iter()
                        .map(|col| {
                            Arc::new(StringArray::from(
                                col.iter().map(|s| s as &str).collect::<Vec<&str>>()
                            )) as ArrayRef
                        })
                        .collect::<Vec<_>>();

                    DataBlock::create_by_array(schema.clone(), cols)
                })
                .collect();
            log::info!("data block is {:?}", blocks);
            let input_stream = futures::stream::iter(blocks);
            let plan_node = InsertIntoPlan {
                db_name,
                tbl_name,
                schema,
                // this is crazy, please do not keep it, I am just test driving apis
                input_stream: Arc::new(Mutex::new(Some(Box::pin(input_stream))))
            };
            Ok(PlanNode::InsertInto(plan_node))
        } else {
            Err(ErrorCodes::UnImplement(
                "only supports simple value tuples as source of insertion"
            ))
        }
    }

    /// Generate a logic plan from an SQL query
    pub fn query_to_plan(&self, query: &sqlparser::ast::Query) -> Result<PlanNode> {
        match &query.body {
            sqlparser::ast::SetExpr::Select(s) => {
                self.select_to_plan(s.as_ref(), &query.limit, &query.order_by)
            }
            _ => Result::Err(ErrorCodes::UnImplement(format!(
                "Query {} not implemented yet",
                query.body
            )))
        }
    }

    /// Generate a logic plan from an SQL select
    /// For example:
    /// "select sum(number+1)+2, number%3 as id from numbers(10) where number>1 group by id having id>1 order by id desc limit 3"
    fn select_to_plan(
        &self,
        select: &sqlparser::ast::Select,
        limit: &Option<sqlparser::ast::Expr>,
        order_by: &[OrderByExpr]
    ) -> Result<PlanNode> {
        // Filter expression
        // In example: Filter=(number > 1)
        let plan = self
            .plan_tables_with_joins(&select.from)
            .and_then(|input| self.filter(&input, &select.selection))?;

        // Projection expression
        // In example: Projection=[(sum((number + 1)) + 2), (number % 3) as id]
        let projection_exprs = select
            .projection
            .iter()
            .map(|e| self.sql_select_to_rex(&e, &plan.schema()))
            .collect::<Result<Vec<Expression>>>()?
            .iter()
            .flat_map(|expr| expand_wildcard(&expr, &plan.schema()))
            .collect::<Vec<Expression>>();

        // Aliases replacement for group by, having, sorting
        // In example: Aliases=[("id", (number % 3))]
        let aliases = extract_aliases(&projection_exprs);

        // Group By expression after against aliases
        // In example: GroupBy=[(number % 3)]
        let group_by_exprs = select
            .group_by
            .iter()
            .map(|e| {
                self.sql_to_rex(e, &plan.schema())
                    .and_then(|expr| resolve_aliases_to_exprs(&expr, &aliases))
            })
            .collect::<Result<Vec<_>>>()?;

        // Having Expression after against aliases
        // In example: Having=((number % 3) > 1)
        let having_expr_opt = select
            .having
            .as_ref()
            .map::<Result<Expression>, _>(|having_expr| {
                let having_expr = self.sql_to_rex(having_expr, &plan.schema())?;
                let having_expr = resolve_aliases_to_exprs(&having_expr, &aliases)?;

                Ok(having_expr)
            })
            .transpose()?;

        // OrderBy expression after against aliases
        // In example: Sort=(number % 3)
        let order_by_exprs = order_by
            .iter()
            .map(|e| -> Result<Expression> {
                Ok(Expression::Sort {
                    expr: Box::new(
                        self.sql_to_rex(&e.expr, &plan.schema())
                            .and_then(|expr| resolve_aliases_to_exprs(&expr, &aliases))?
                    ),
                    asc: e.asc.unwrap_or(true),
                    nulls_first: e.nulls_first.unwrap_or(true)
                })
            })
            .collect::<Result<Vec<Expression>>>()?;

        // The outer expressions we will search through for
        // aggregates. Aggregates may be sourced from the SELECT, order by, having ...
        let mut expression_exprs = projection_exprs.clone();
        // from order by
        expression_exprs.extend_from_slice(&order_by_exprs);
        let expression_with_sort = expression_exprs.clone();
        // ... or from the HAVING.
        if let Some(having_expr) = &having_expr_opt {
            expression_exprs.push(having_expr.clone());
        }

        // All of the aggregate expressions (deduplicated).
        // In example: aggr=[[sum((number + 1))]]
        let aggr_exprs = find_aggregate_exprs(&expression_exprs);

        let has_aggr = aggr_exprs.len() + group_by_exprs.len() > 0;
        let (plan, having_expr_post_aggr_opt) = if has_aggr {
            let aggr_projection_exprs = group_by_exprs
                .iter()
                .chain(aggr_exprs.iter())
                .cloned()
                .collect::<Vec<_>>();

            let before_aggr_exprs = expand_aggregate_arg_exprs(&aggr_projection_exprs);

            // Build aggregate inner expression plan and then aggregate&groupby plan.
            // In example:
            // inner expression=[(number + 1), (number % 3)]
            let plan = self
                .expression(&plan, &before_aggr_exprs, "Before GroupBy")
                .and_then(|input| self.aggregate(&input, &aggr_exprs, &group_by_exprs))?;

            // After aggregation, these are all of the columns that will be
            // available to next phases of planning.
            let column_exprs_post_aggr = aggr_projection_exprs
                .iter()
                .map(|expr| expr_as_column_expr(expr))
                .collect::<Result<Vec<_>>>()?;

            // Rewrite the SELECT expression to use the columns produced by the aggregation.
            // In example:[col("number + 1"), col("number % 3")]
            let select_exprs_post_aggr = expression_exprs
                .iter()
                .map(|expr| rebase_expr(expr, &aggr_projection_exprs))
                .collect::<Result<Vec<_>>>()?;

            if let Ok(Some(expr)) =
                find_columns_not_satisfy_exprs(&column_exprs_post_aggr, &select_exprs_post_aggr)
            {
                return Err(ErrorCodes::IllegalAggregateExp(format!(
                    "Column `{:?}` is not under aggregate function and not in GROUP BY: While processing {:?}",
                    expr, select_exprs_post_aggr
                )));
            }

            // Rewrite the HAVING expression to use the columns produced by the
            // aggregation.
            let having_expr_post_aggr_opt = if let Some(having_expr) = &having_expr_opt {
                let having_expr_post_aggr = rebase_expr(having_expr, &aggr_projection_exprs)?;
                if let Ok(Some(expr)) = find_columns_not_satisfy_exprs(&column_exprs_post_aggr, &[
                    having_expr_post_aggr.clone()
                ]) {
                    return Err(ErrorCodes::IllegalAggregateExp(format!(
                        "Column `{:?}` is not under aggregate function and not in GROUP BY: While processing {:?}",
                        expr, having_expr_post_aggr
                    )));
                }
                Some(having_expr_post_aggr)
            } else {
                None
            };

            (plan, having_expr_post_aggr_opt)
        } else {
            (plan, having_expr_opt)
        };

        let stage_phase = if order_by_exprs.is_empty() {
            "Before Projection"
        } else {
            "Before OrderBy"
        };

        let plan = self.expression(&plan, &expression_with_sort, stage_phase)?;

        // Having.
        let plan = self.having(&plan, having_expr_post_aggr_opt)?;
        // Order by
        let plan = self.sort(&plan, &order_by_exprs)?;
        // Projection
        let plan = self.project(&plan, &projection_exprs)?;
        // Limit.
        let plan = self.limit(&plan, limit)?;

        Ok(PlanNode::Select(SelectPlan {
            input: Arc::new(plan)
        }))
    }

    /// Generate a relational expression from a select SQL expression
    fn sql_select_to_rex(
        &self,
        sql: &sqlparser::ast::SelectItem,
        schema: &DataSchema
    ) -> Result<Expression> {
        match sql {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => self.sql_to_rex(expr, schema),
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => Ok(Expression::Alias(
                alias.value.clone(),
                Box::new(self.sql_to_rex(&expr, schema)?)
            )),
            sqlparser::ast::SelectItem::Wildcard => Ok(Expression::Wildcard),
            _ => Result::Err(ErrorCodes::UnImplement(format!(
                "SelectItem: {:?} are not supported",
                sql
            )))
        }
    }

    fn plan_tables_with_joins(&self, from: &[sqlparser::ast::TableWithJoins]) -> Result<PlanNode> {
        match from.len() {
            0 => self.plan_with_dummy_source(),
            1 => self.plan_table_with_joins(&from[0]),
            _ => self.plan_joins(from)
        }
    }

    fn plan_joins(&self, table_references: &[sqlparser::ast::TableWithJoins]) -> Result<PlanNode> {
        let mut joins = Vec::<PlanNode>::new();
        for table_reference in table_references {
            joins.push(self.plan_join(table_reference)?);
        }
        let mut builder = PlanBuilder::from(&joins[0]);
        for i in 1..joins.len() {
            builder = builder.join(&[], &joins[i])?;
        }
        builder.build()
    }

    fn plan_join(&self, table_reference: &sqlparser::ast::TableWithJoins) -> Result<PlanNode> {
        let mut joined_table = self.create_relation(&table_reference.relation)?;
        // This is a hack that we just simply merge the schemas of joined tables into one
        // so we can build join condition with it.
        let mut schema = (*joined_table.schema()).clone();
        for join in table_reference.joins.iter() {
            let right_table = self.create_relation(&join.relation)?;
            schema = DataSchema::try_merge(vec![schema, (*right_table.schema()).clone()])?;
            // Note that schema of current right table and merged left tables is enough to build condition expression.
            // let condition = match &join.join_operator {
            //     JoinOperator::Inner(constraint) => match constraint {
            //         JoinConstraint::On(condition) => self.sql_to_rex(condition, &schema),
            //         _ => Result::Err(ErrorCodes::SyntexException("Unsupported USING clause"))
            //     },
            //     _ => Result::Err(ErrorCodes::SyntexException("Unsupported JOIN type"))
            // }?;

            joined_table = PlanBuilder::from(&joined_table)
                .join(&[], &right_table)?
                .build()?;
        }

        Ok(joined_table)
    }

    fn plan_with_dummy_source(&self) -> Result<PlanNode> {
        let db_name = "system";
        let table_name = "one";

        self.ctx.get_table(db_name, table_name).and_then(|table| {
            table
                .schema()
                .and_then(|ref schema| {
                    PlanBuilder::scan(db_name, table_name, schema, None, None, None)
                })
                .and_then(|builder| builder.build())
                .and_then(|dummy_scan_plan| match dummy_scan_plan {
                    PlanNode::Scan(ref dummy_scan_plan) => table
                        .read_plan(self.ctx.clone(), dummy_scan_plan)
                        .map(PlanNode::ReadSource),
                    _unreachable_plan => panic!("Logical error: cannot downcast to scan plan")
                })
        })
    }

    fn plan_table_with_joins(&self, t: &sqlparser::ast::TableWithJoins) -> Result<PlanNode> {
        self.create_relation(&t.relation)
    }

    fn create_relation(&self, relation: &sqlparser::ast::TableFactor) -> Result<PlanNode> {
        use sqlparser::ast::TableFactor::*;

        match relation {
            Table { name, args, .. } => {
                let mut db_name = self.ctx.get_current_database();
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
                        return Result::Err(ErrorCodes::BadArguments(
                            "Currently table can't have arguments"
                        ));
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

                let scan = {
                    table.schema().and_then(|schema| {
                        PlanBuilder::scan(
                            &db_name,
                            &table_name,
                            schema.as_ref(),
                            None,
                            table_args,
                            None
                        )
                        .and_then(|builder| builder.build())
                    })
                };

                scan.and_then(|scan| match scan {
                    PlanNode::Scan(ref scan) => table
                        .read_plan(self.ctx.clone(), scan)
                        .map(PlanNode::ReadSource),
                    _unreachable_plan => panic!("Logical error: Cannot downcast to scan plan")
                })
            }
            Derived { subquery, .. } => self.query_to_plan(subquery),
            NestedJoin(table_with_joins) => self.plan_table_with_joins(table_with_joins),
            TableFunction { .. } => {
                Result::Err(ErrorCodes::UnImplement("Unsupported table function"))
            }
        }
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(
        &self,
        expr: &sqlparser::ast::Expr,
        schema: &DataSchema
    ) -> Result<Expression> {
        fn value_to_rex(value: &sqlparser::ast::Value) -> Result<Expression> {
            match value {
                sqlparser::ast::Value::Number(ref n, _) => {
                    DataValue::try_from_literal(n).map(Expression::Literal)
                }
                sqlparser::ast::Value::SingleQuotedString(ref value) => {
                    Ok(Expression::Literal(DataValue::Utf8(Some(value.clone()))))
                }
                sqlparser::ast::Value::Interval {
                    value,
                    leading_field,
                    leading_precision,
                    last_field,
                    fractional_seconds_precision
                } => SQLCommon::make_sql_interval_to_literal(
                    value,
                    leading_field,
                    leading_precision,
                    last_field,
                    fractional_seconds_precision
                ),
                sqlparser::ast::Value::Boolean(b) => {
                    Ok(Expression::Literal(DataValue::Boolean(Some(*b))))
                }
                other => Result::Err(ErrorCodes::SyntaxException(format!(
                    "Unsupported value expression: {}, type: {:?}",
                    value, other
                )))
            }
        }

        match expr {
            sqlparser::ast::Expr::Value(value) => value_to_rex(value),
            sqlparser::ast::Expr::Identifier(ref v) => Ok(Expression::Column(v.clone().value)),
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                Ok(Expression::BinaryExpression {
                    op: format!("{}", op),
                    left: Box::new(self.sql_to_rex(left, schema)?),
                    right: Box::new(self.sql_to_rex(right, schema)?)
                })
            }
            sqlparser::ast::Expr::UnaryOp { op, expr } => Ok(Expression::UnaryExpression {
                op: format!("{}", op),
                expr: Box::new(self.sql_to_rex(expr, schema)?)
            }),
            sqlparser::ast::Expr::Nested(e) => self.sql_to_rex(e, schema),
            sqlparser::ast::Expr::Function(e) => {
                let mut args = Vec::with_capacity(e.args.len());

                // 1. Get the args from context by function name. such as SELECT database()
                // common::ScalarFunctions::udf::database arg is ctx.get_default()
                let ctx_args = ContextFunction::build_args_from_ctx(
                    e.name.to_string().as_str(),
                    self.ctx.clone()
                )?;
                if !ctx_args.is_empty() {
                    args.extend_from_slice(ctx_args.as_slice());
                }

                // 2. Get args from the ast::Expr:Function
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

                let op = e.name.to_string();
                if AggregateFunctionFactory::get(&op).is_ok() {
                    return Ok(Expression::AggregateFunction { op, args });
                }

                Ok(Expression::ScalarFunction { op, args })
            }
            sqlparser::ast::Expr::Wildcard => Ok(Expression::Wildcard),
            sqlparser::ast::Expr::TypedString { data_type, value } => {
                SQLCommon::make_data_type(data_type).map(|data_type| Expression::Cast {
                    expr: Box::new(Expression::Literal(DataValue::Utf8(Some(value.clone())))),
                    data_type
                })
            }
            sqlparser::ast::Expr::Cast { expr, data_type } => self
                .sql_to_rex(expr, schema)
                .map(Box::from)
                .and_then(|expr| {
                    SQLCommon::make_data_type(data_type)
                        .map(|data_type| Expression::Cast { expr, data_type })
                }),
            sqlparser::ast::Expr::Substring {
                expr,
                substring_from,
                substring_for
            } => {
                let mut args = Vec::with_capacity(3);
                args.push(self.sql_to_rex(expr, schema)?);
                if let Some(from) = substring_from {
                    args.push(self.sql_to_rex(from, schema)?);
                } else {
                    args.push(Expression::Literal(DataValue::Int64(Some(1))));
                }

                if let Some(len) = substring_for {
                    args.push(self.sql_to_rex(len, schema)?);
                }

                Ok(Expression::ScalarFunction {
                    op: "substring".to_string(),
                    args
                })
            }
            other => Result::Err(ErrorCodes::SyntaxException(format!(
                "Unsupported expression: {}, type: {:?}",
                expr, other
            )))
        }
    }

    pub fn set_variable_to_plan(
        &self,
        variable: &sqlparser::ast::Ident,
        values: &[sqlparser::ast::SetVariableValue]
    ) -> Result<PlanNode> {
        let mut vars = vec![];
        for value in values {
            let variable = variable.value.clone();
            let value = match value {
                sqlparser::ast::SetVariableValue::Ident(v) => v.value.clone(),
                sqlparser::ast::SetVariableValue::Literal(v) => v.to_string()
            };
            vars.push(VarValue { variable, value });
        }
        Ok(PlanNode::SetVariable(SettingPlan { vars }))
    }

    /// Apply a filter to the plan
    fn filter(
        &self,
        plan: &PlanNode,
        predicate: &Option<sqlparser::ast::Expr>
    ) -> Result<PlanNode> {
        match *predicate {
            Some(ref predicate_expr) => {
                self.sql_to_rex(predicate_expr, &plan.schema())
                    .and_then(|filter_expr| {
                        PlanBuilder::from(&plan)
                            .filter(filter_expr)
                            .and_then(|builder| builder.build())
                    })
            }
            _ => Ok(plan.clone())
        }
    }

    /// Apply a having to the plan
    fn having(&self, plan: &PlanNode, expr: Option<Expression>) -> Result<PlanNode> {
        if let Some(expr) = expr {
            let expr = rebase_expr_from_input(&expr, &plan.schema())?;
            return PlanBuilder::from(&plan)
                .having(expr)
                .and_then(|builder| builder.build());
        }
        Ok(plan.clone())
    }

    /// Wrap a plan in a projection
    fn project(&self, input: &PlanNode, exprs: &[Expression]) -> Result<PlanNode> {
        let exprs = exprs
            .iter()
            .map(|expr| rebase_expr_from_input(expr, &input.schema()))
            .collect::<Result<Vec<_>>>()?;

        PlanBuilder::from(&input)
            .project(&exprs)
            .and_then(|builder| builder.build())
    }

    /// Wrap a plan for an aggregate
    fn aggregate(
        &self,
        input: &PlanNode,
        aggr_exprs: &[Expression],
        group_by_exprs: &[Expression]
    ) -> Result<PlanNode> {
        let aggr_exprs = aggr_exprs
            .iter()
            .map(|expr| rebase_expr_from_input(expr, &input.schema()))
            .collect::<Result<Vec<_>>>()?;

        let group_by_exprs = group_by_exprs
            .iter()
            .map(|expr| rebase_expr_from_input(expr, &input.schema()))
            .collect::<Result<Vec<_>>>()?;

        // S0: Apply a partial aggregator plan.
        // S1: Apply a fragment plan for distributed planners split.
        // S2: Apply a final aggregator plan.
        PlanBuilder::from(&input)
            .aggregate_partial(&aggr_exprs, &group_by_exprs)
            .and_then(|builder| builder.stage(self.ctx.get_id()?, StageState::AggregatorMerge))
            .and_then(|builder| {
                builder.aggregate_final(input.schema(), &aggr_exprs, &group_by_exprs)
            })
            .and_then(|builder| builder.build())
    }

    fn sort(&self, input: &PlanNode, order_by_exprs: &[Expression]) -> Result<PlanNode> {
        if order_by_exprs.is_empty() {
            return Ok(input.clone());
        }

        let order_by_exprs = order_by_exprs
            .iter()
            .map(|expr| rebase_expr_from_input(expr, &input.schema()))
            .collect::<Result<Vec<_>>>()?;

        PlanBuilder::from(&input)
            .sort(&order_by_exprs)
            .and_then(|builder| builder.build())
    }

    /// Wrap a plan in a limit
    fn limit(&self, input: &PlanNode, limit: &Option<sqlparser::ast::Expr>) -> Result<PlanNode> {
        match *limit {
            Some(ref limit_expr) => {
                let n = self
                    .sql_to_rex(&limit_expr, &input.schema())
                    .and_then(|limit_expr| match limit_expr {
                        Expression::Literal(DataValue::UInt64(Some(n))) => Ok(n as usize),
                        _ => Err(ErrorCodes::SyntaxException(
                            "Unexpected expression for LIMIT clause"
                        ))
                    })?;

                PlanBuilder::from(&input)
                    .limit(n)
                    .and_then(|builder| builder.build())
            }
            _ => Ok(input.clone())
        }
    }

    /// Apply a expression against exprs.
    fn expression(&self, input: &PlanNode, exprs: &[Expression], desc: &str) -> Result<PlanNode> {
        let mut dedup_exprs = vec![];
        for expr in exprs {
            let rebased_expr = unwrap_alias_exprs(expr)
                .and_then(|e| rebase_expr_from_input(&e, &input.schema()))?;
            let rebased_expr = sort_to_inner_expr(&rebased_expr);

            if !dedup_exprs.contains(&rebased_expr) {
                dedup_exprs.push(rebased_expr);
            }
        }

        // if all expression is column expression expression, we skip this expression
        if dedup_exprs.iter().all(|expr| {
            if let Expression::Column(_) = expr {
                return true;
            }
            false
        }) {
            return Ok(input.clone());
        }

        if dedup_exprs.is_empty() {
            return Ok(input.clone());
        }

        PlanBuilder::from(&input)
            .expression(&dedup_exprs, desc)
            .and_then(|builder| builder.build())
    }
}
