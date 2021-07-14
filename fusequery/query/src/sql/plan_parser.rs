// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_planners::expand_aggregate_arg_exprs;
use common_planners::expand_wildcard;
use common_planners::expr_as_column_expr;
use common_planners::extract_aliases;
use common_planners::find_aggregate_exprs;
use common_planners::find_columns_not_satisfy_exprs;
use common_planners::rebase_expr;
use common_planners::rebase_expr_from_input;
use common_planners::resolve_aliases_to_exprs;
use common_planners::sort_to_inner_expr;
use common_planners::unwrap_alias_exprs;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DescribeTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_planners::ExplainPlan;
use common_planners::Expression;
use common_planners::InListExpr;
use common_planners::InsertIntoPlan;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_planners::SettingPlan;
use common_planners::ShowCreateTablePlan;
use common_planners::UseDatabasePlan;
use common_planners::VarValue;
use common_tracing::tracing;
use sqlparser::ast::Expr;
use sqlparser::ast::FunctionArg;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::OrderByExpr;
use sqlparser::ast::Query;
use sqlparser::ast::Statement;
use sqlparser::ast::TableFactor;

use crate::datasources::Table;
use crate::functions::ContextFunction;
use crate::sessions::FuseQueryContextRef;
use crate::sql::sql_statement::DfCreateTable;
use crate::sql::sql_statement::DfDropDatabase;
use crate::sql::sql_statement::DfUseDatabase;
use crate::sql::DfCreateDatabase;
use crate::sql::DfDescribeTable;
use crate::sql::DfDropTable;
use crate::sql::DfExplain;
use crate::sql::DfHint;
use crate::sql::DfParser;
use crate::sql::DfShowCreateTable;
use crate::sql::DfStatement;
use crate::sql::SQLCommon;

pub struct PlanParser {
    ctx: FuseQueryContextRef,
}

impl PlanParser {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        Self { ctx }
    }

    pub fn build_from_sql(&self, query: &str) -> Result<PlanNode> {
        tracing::debug!(query);
        DfParser::parse_sql(query).and_then(|(stmts, _)| {
            stmts
                .first()
                .map(|statement| self.statement_to_plan(statement))
                .unwrap_or_else(|| {
                    Result::Err(ErrorCode::SyntaxException("Only support single query"))
                })
        })
    }

    pub fn build_with_hint_from_sql(&self, query: &str) -> (Result<PlanNode>, Vec<DfHint>) {
        tracing::debug!(query);
        let stmt_hints = DfParser::parse_sql(query);
        match stmt_hints {
            Ok((stmts, hints)) => match stmts.first() {
                Some(stmt) => (self.statement_to_plan(stmt), hints),
                None => (
                    Result::Err(ErrorCode::SyntaxException("Only support single query")),
                    vec![],
                ),
            },
            Err(e) => (Err(e), vec![]),
        }
    }

    pub fn statement_to_plan(&self, statement: &DfStatement) -> Result<PlanNode> {
        match statement {
            DfStatement::Statement(v) => self.sql_statement_to_plan(v),
            DfStatement::Explain(v) => self.sql_explain_to_plan(v),
            DfStatement::ShowDatabases(_) => {
                self.build_from_sql("SELECT name FROM system.databases ORDER BY name")
            }
            DfStatement::CreateDatabase(v) => self.sql_create_database_to_plan(v),
            DfStatement::DropDatabase(v) => self.sql_drop_database_to_plan(v),
            DfStatement::CreateTable(v) => self.sql_create_table_to_plan(v),
            DfStatement::DescribeTable(v) => self.sql_describe_table_to_plan(v),
            DfStatement::DropTable(v) => self.sql_drop_table_to_plan(v),
            DfStatement::UseDatabase(v) => self.sql_use_database_to_plan(v),
            DfStatement::ShowCreateTable(v) => self.sql_show_create_table_to_plan(v),

            // TODO: support like and other filters in show queries
            DfStatement::ShowTables(_) => self.build_from_sql(
                format!(
                    "SELECT name FROM system.tables where database = '{}' ORDER BY database, name",
                    self.ctx.get_current_database()
                )
                .as_str(),
            ),
            DfStatement::ShowSettings(_) => self.build_from_sql("SELECT name FROM system.settings"),
        }
    }

    /// Builds plan from AST statement.
    #[tracing::instrument(level = "info", skip(self, statement))]
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

            _ => Result::Err(ErrorCode::SyntaxException(format!(
                "Unsupported statement {:?}",
                statement
            ))),
        }
    }

    /// Generate a logic plan from an EXPLAIN
    #[tracing::instrument(level = "info", skip(self, explain))]
    pub fn sql_explain_to_plan(&self, explain: &DfExplain) -> Result<PlanNode> {
        let plan = self.sql_statement_to_plan(&explain.statement)?;
        Ok(PlanNode::Explain(ExplainPlan {
            typ: explain.typ,
            input: Arc::new(plan),
        }))
    }

    /// DfCreateDatabase to plan.
    #[tracing::instrument(level = "info", skip(self, create), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub fn sql_create_database_to_plan(&self, create: &DfCreateDatabase) -> Result<PlanNode> {
        if create.name.0.is_empty() {
            return Result::Err(ErrorCode::SyntaxException("Create database name is empty"));
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
            options,
        }))
    }

    /// DfDropDatabase to plan.
    #[tracing::instrument(level = "info", skip(self, drop), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub fn sql_drop_database_to_plan(&self, drop: &DfDropDatabase) -> Result<PlanNode> {
        if drop.name.0.is_empty() {
            return Result::Err(ErrorCode::SyntaxException("Drop database name is empty"));
        }
        let name = drop.name.0[0].value.clone();

        Ok(PlanNode::DropDatabase(DropDatabasePlan {
            if_exists: drop.if_exists,
            db: name,
        }))
    }

    #[tracing::instrument(level = "info", skip(self, use_db), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub fn sql_use_database_to_plan(&self, use_db: &DfUseDatabase) -> Result<PlanNode> {
        let db = use_db.name.0[0].value.clone();
        Ok(PlanNode::UseDatabase(UseDatabasePlan { db }))
    }

    #[tracing::instrument(level = "info", skip(self, create), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub fn sql_create_table_to_plan(&self, create: &DfCreateTable) -> Result<PlanNode> {
        let mut db = self.ctx.get_current_database();
        if create.name.0.is_empty() {
            return Result::Err(ErrorCode::SyntaxException("Create table name is empty"));
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
            .collect::<Result<Vec<DataField>>>()?;

        let mut options = HashMap::new();
        for p in create.options.iter() {
            options.insert(
                p.name.value.to_lowercase(),
                p.value
                    .to_string()
                    .trim_matches(|s| s == '\'' || s == '"')
                    .to_string(),
            );
        }

        let schema = DataSchemaRefExt::create(fields);
        Ok(PlanNode::CreateTable(CreateTablePlan {
            if_not_exists: create.if_not_exists,
            db,
            table,
            schema,
            engine: create.engine,
            options,
        }))
    }

    #[tracing::instrument(level = "info", skip(self, show_create), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub fn sql_show_create_table_to_plan(
        &self,
        show_create: &DfShowCreateTable,
    ) -> Result<PlanNode> {
        let mut db = self.ctx.get_current_database();
        if show_create.name.0.is_empty() {
            return Result::Err(ErrorCode::SyntaxException(
                "Show create table name is empty",
            ));
        }
        let mut table = show_create.name.0[0].value.clone();
        if show_create.name.0.len() > 1 {
            db = table;
            table = show_create.name.0[1].value.clone();
        }

        let fields = vec![
            DataField::new("Table", DataType::Utf8, false),
            DataField::new("Create Table", DataType::Utf8, false),
        ];

        let schema = DataSchemaRefExt::create(fields);
        Ok(PlanNode::ShowCreateTable(ShowCreateTablePlan {
            db,
            table,
            schema,
        }))
    }

    /// DfDescribeTable to plan.
    #[tracing::instrument(level = "info", skip(self, describe), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub fn sql_describe_table_to_plan(&self, describe: &DfDescribeTable) -> Result<PlanNode> {
        let mut db = self.ctx.get_current_database();
        if describe.name.0.is_empty() {
            return Result::Err(ErrorCode::SyntaxException("Describe table name is empty"));
        }
        let mut table = describe.name.0[0].value.clone();
        if describe.name.0.len() > 1 {
            db = table;
            table = describe.name.0[1].value.clone();
        }

        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Field", DataType::Utf8, false),
            DataField::new("Type", DataType::Utf8, false),
            DataField::new("Null", DataType::Utf8, false),
        ]);

        Ok(PlanNode::DescribeTable(DescribeTablePlan {
            db,
            table,
            schema,
        }))
    }

    /// DfDropTable to plan.
    #[tracing::instrument(level = "info", skip(self, drop), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub fn sql_drop_table_to_plan(&self, drop: &DfDropTable) -> Result<PlanNode> {
        let mut db = self.ctx.get_current_database();
        if drop.name.0.is_empty() {
            return Result::Err(ErrorCode::SyntaxException("Drop table name is empty"));
        }
        let mut table = drop.name.0[0].value.clone();
        if drop.name.0.len() > 1 {
            db = table;
            table = drop.name.0[1].value.clone();
        }
        Ok(PlanNode::DropTable(DropTablePlan {
            if_exists: drop.if_exists,
            db,
            table,
        }))
    }

    #[tracing::instrument(level = "info", skip(self, table_name, columns, source), fields(ctx.id = self.ctx.get_id().as_str()))]
    fn insert_to_plan(
        &self,
        table_name: &ObjectName,
        columns: &[Ident],
        source: &Query,
    ) -> Result<PlanNode> {
        if let sqlparser::ast::SetExpr::Values(ref vs) = source.body {
            //            let col_num = columns.len();
            let db_name = self.ctx.get_current_database();
            let tbl_name = table_name
                .0
                .get(0)
                .ok_or_else(|| ErrorCode::SyntaxException("empty table name now allowed"))?
                .value
                .clone();

            let values = &vs.0;
            if values.is_empty() {
                return Err(ErrorCode::EmptyData(
                    "empty values for insertion is not allowed",
                ));
            }

            let all_value = values
                .iter()
                .all(|row| row.iter().all(|item| matches!(item, Expr::Value(_))));
            if !all_value {
                return Err(ErrorCode::UnImplement(
                    "not support value expressions other than literal value yet",
                ));
            }
            // Buffers some chunks if possible
            let chunks = values.chunks(100);
            let fields = columns
                .iter()
                .map(|ident| DataField::new(&ident.value, DataType::Utf8, true))
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
                                    _ => "N/A".to_string(),
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect();

                    let cols = transposed
                        .iter()
                        .map(|col| {
                            Series::new(col.iter().map(|s| s as &str).collect::<Vec<&str>>())
                        })
                        .collect::<Vec<_>>();

                    DataBlock::create_by_array(schema.clone(), cols)
                })
                .collect();
            let input_stream = futures::stream::iter(blocks);
            let plan_node = InsertIntoPlan {
                db_name,
                tbl_name,
                schema,
                // this is crazy, please do not keep it, I am just test driving apis
                input_stream: Arc::new(Mutex::new(Some(Box::pin(input_stream)))),
            };
            Ok(PlanNode::InsertInto(plan_node))
        } else {
            Err(ErrorCode::UnImplement(
                "only supports simple value tuples as source of insertion",
            ))
        }
    }

    /// Generate a logic plan from an SQL query
    pub fn query_to_plan(&self, query: &sqlparser::ast::Query) -> Result<PlanNode> {
        if query.with.is_some() {
            return Result::Err(ErrorCode::UnImplement("CTE is not yet implement"));
        }

        match &query.body {
            sqlparser::ast::SetExpr::Select(s) => {
                self.select_to_plan(s.as_ref(), &query.limit, &query.offset, &query.order_by)
            }
            _ => Result::Err(ErrorCode::UnImplement(format!(
                "Query {} is not yet implemented",
                query.body
            ))),
        }
    }

    /// Generate a logic plan from an SQL select
    /// For example:
    /// "select sum(number+1)+2, number%3 as id from numbers(10) where number>1 group by id having id>1 order by id desc limit 3"
    #[tracing::instrument(level = "info", skip(self, select, limit, order_by))]
    fn select_to_plan(
        &self,
        select: &sqlparser::ast::Select,
        limit: &Option<sqlparser::ast::Expr>,
        offset: &Option<sqlparser::ast::Offset>,
        order_by: &[OrderByExpr],
    ) -> Result<PlanNode> {
        // Filter expression
        // In example: Filter=(number > 1)
        let plan = self
            .plan_tables_with_joins(&select.from)
            .and_then(|input| self.filter(&input, &select.selection, Some(select)))?;

        // Projection expression
        // In example: Projection=[(sum((number + 1)) + 2), (number % 3) as id]
        let projection_exprs = select
            .projection
            .iter()
            .map(|e| self.sql_select_to_rex(e, &plan.schema(), Some(select)))
            .collect::<Result<Vec<Expression>>>()?
            .iter()
            .flat_map(|expr| expand_wildcard(expr, &plan.schema()))
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
                self.sql_to_rex(e, &plan.schema(), Some(select))
                    .and_then(|expr| resolve_aliases_to_exprs(&expr, &aliases))
            })
            .collect::<Result<Vec<_>>>()?;

        // Having Expression after against aliases
        // In example: Having=((number % 3) > 1)
        let having_expr_opt = select
            .having
            .as_ref()
            .map::<Result<Expression>, _>(|having_expr| {
                let having_expr = self.sql_to_rex(having_expr, &plan.schema(), Some(select))?;
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
                        self.sql_to_rex(&e.expr, &plan.schema(), Some(select))
                            .and_then(|expr| resolve_aliases_to_exprs(&expr, &aliases))?,
                    ),
                    asc: e.asc.unwrap_or(true),
                    nulls_first: e.nulls_first.unwrap_or(true),
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
                return Err(ErrorCode::IllegalAggregateExp(format!(
                    "Column `{:?}` is not under aggregate function and not in GROUP BY: While processing {:?}",
                    expr, select_exprs_post_aggr
                )));
            }

            // Rewrite the HAVING expression to use the columns produced by the
            // aggregation.
            let having_expr_post_aggr_opt = if let Some(having_expr) = &having_expr_opt {
                let having_expr_post_aggr = rebase_expr(having_expr, &aggr_projection_exprs)?;
                if let Ok(Some(expr)) = find_columns_not_satisfy_exprs(&column_exprs_post_aggr, &[
                    having_expr_post_aggr.clone(),
                ]) {
                    return Err(ErrorCode::IllegalAggregateExp(format!(
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
        let plan = self.limit(&plan, limit, offset, Some(select))?;

        Ok(PlanNode::Select(SelectPlan {
            input: Arc::new(plan),
        }))
    }

    /// Generate a relational expression from a select SQL expression
    fn sql_select_to_rex(
        &self,
        sql: &sqlparser::ast::SelectItem,
        schema: &DataSchema,
        select: Option<&sqlparser::ast::Select>,
    ) -> Result<Expression> {
        match sql {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => self.sql_to_rex(expr, schema, select),
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => Ok(Expression::Alias(
                alias.value.clone(),
                Box::new(self.sql_to_rex(expr, schema, select)?),
            )),
            sqlparser::ast::SelectItem::Wildcard => Ok(Expression::Wildcard),
            _ => Result::Err(ErrorCode::UnImplement(format!(
                "SelectItem: {:?} are not supported",
                sql
            ))),
        }
    }

    fn plan_tables_with_joins(&self, from: &[sqlparser::ast::TableWithJoins]) -> Result<PlanNode> {
        match from.len() {
            0 => self.plan_with_dummy_source(),
            1 => self.plan_table_with_joins(&from[0]),
            _ => Result::Err(ErrorCode::SyntaxException("Cannot support JOIN clause")),
        }
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
                        .read_plan(
                            self.ctx.clone(),
                            dummy_scan_plan,
                            self.ctx.get_max_threads()? as usize,
                        )
                        .map(PlanNode::ReadSource),
                    _unreachable_plan => panic!("Logical error: cannot downcast to scan plan"),
                })
        })
    }

    fn plan_table_with_joins(&self, t: &sqlparser::ast::TableWithJoins) -> Result<PlanNode> {
        self.create_relation(&t.relation)
    }

    fn create_relation(&self, relation: &sqlparser::ast::TableFactor) -> Result<PlanNode> {
        match relation {
            TableFactor::Table { name, args, .. } => {
                let mut db_name = self.ctx.get_current_database();
                let mut table_name = name.to_string();
                if name.0.len() == 2 {
                    db_name = name.0[0].to_string();
                    table_name = name.0[1].to_string();
                }
                let mut table_args = None;
                let table: Arc<dyn Table>;

                // only table functions has table args
                if !args.is_empty() {
                    if name.0.len() >= 2 {
                        return Result::Err(ErrorCode::BadArguments(
                            "Currently table can't have arguments",
                        ));
                    }

                    let empty_schema = Arc::new(DataSchema::empty());
                    match &args[0] {
                        FunctionArg::Named { arg, .. } => {
                            table_args = Some(self.sql_to_rex(arg, empty_schema.as_ref(), None)?);
                        }
                        FunctionArg::Unnamed(arg) => {
                            table_args = Some(self.sql_to_rex(arg, empty_schema.as_ref(), None)?);
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
                            None,
                        )
                        .and_then(|builder| builder.build())
                    })
                };

                // TODO: Move ReadSourcePlan to SelectInterpreter
                scan.and_then(|scan| match scan {
                    PlanNode::Scan(ref scan) => table
                        .read_plan(self.ctx.clone(), scan, self.ctx.get_max_threads()? as usize)
                        .map(PlanNode::ReadSource),
                    _unreachable_plan => panic!("Logical error: Cannot downcast to scan plan"),
                })
            }
            TableFactor::Derived { subquery, .. } => self.query_to_plan(subquery),
            TableFactor::NestedJoin(table_with_joins) => {
                self.plan_table_with_joins(table_with_joins)
            }
            TableFactor::TableFunction { .. } => {
                Result::Err(ErrorCode::UnImplement("Unsupported table function"))
            }
        }
    }
    fn process_compound_ident(
        &self,
        ids: &[Ident],
        select: Option<&sqlparser::ast::Select>,
    ) -> Result<Expression> {
        let mut var_names = vec![];
        for id in ids {
            var_names.push(id.value.clone());
        }
        if &var_names[0][0..1] == "@" || var_names.len() != 2 || select == None {
            return Err(ErrorCode::UnImplement(format!(
                "Unsupported compound identifier '{:?}'",
                var_names,
            )));
        }

        let table_name = &var_names[0];
        let from = &select.unwrap().from;
        let obj_table_name = ObjectName(vec![Ident::new(table_name)]);

        match from.len() {
            0 => Err(ErrorCode::SyntaxException(
                "Missing table in the select clause",
            )),
            1 => match &from[0].relation {
                TableFactor::Table {
                    name,
                    alias,
                    args: _,
                    with_hints: _,
                } => {
                    if *name == obj_table_name {
                        return Ok(Expression::Column(var_names.pop().unwrap()));
                    }
                    match alias {
                        Some(a) => {
                            if a.name == ids[0] {
                                Ok(Expression::Column(var_names.pop().unwrap()))
                            } else {
                                Err(ErrorCode::UnknownTable(format!(
                                    "Unknown Table '{:?}'",
                                    &table_name,
                                )))
                            }
                        }
                        None => Err(ErrorCode::UnknownTable(format!(
                            "Unknown Table '{:?}'",
                            &table_name,
                        ))),
                    }
                }
                TableFactor::Derived {
                    lateral: _,
                    subquery: _,
                    alias,
                } => match alias {
                    Some(a) => {
                        if a.name == ids[0] {
                            Ok(Expression::Column(var_names.pop().unwrap()))
                        } else {
                            Err(ErrorCode::UnknownTable(format!(
                                "Unknown Table '{:?}'",
                                &table_name,
                            )))
                        }
                    }
                    None => Err(ErrorCode::UnknownTable(format!(
                        "Unknown Table '{:?}'",
                        &table_name,
                    ))),
                },
                _ => Err(ErrorCode::SyntaxException("Cannot support Nested Join now")),
            },
            _ => Err(ErrorCode::SyntaxException("Cannot support JOIN clause")),
        }
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(
        &self,
        expr: &sqlparser::ast::Expr,
        schema: &DataSchema,
        select: Option<&sqlparser::ast::Select>,
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
                    fractional_seconds_precision,
                } => SQLCommon::make_sql_interval_to_literal(
                    value,
                    leading_field,
                    leading_precision,
                    last_field,
                    fractional_seconds_precision,
                ),
                sqlparser::ast::Value::Boolean(b) => {
                    Ok(Expression::Literal(DataValue::Boolean(Some(*b))))
                }
                other => Result::Err(ErrorCode::SyntaxException(format!(
                    "Unsupported value expression: {}, type: {:?}",
                    value, other
                ))),
            }
        }

        match expr {
            sqlparser::ast::Expr::Value(value) => value_to_rex(value),
            sqlparser::ast::Expr::Identifier(ref v) => Ok(Expression::Column(v.clone().value)),
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                Ok(Expression::BinaryExpression {
                    op: format!("{}", op),
                    left: Box::new(self.sql_to_rex(left, schema, select)?),
                    right: Box::new(self.sql_to_rex(right, schema, select)?),
                })
            }
            sqlparser::ast::Expr::UnaryOp { op, expr } => Ok(Expression::UnaryExpression {
                op: format!("{}", op),
                expr: Box::new(self.sql_to_rex(expr, schema, select)?),
            }),
            sqlparser::ast::Expr::Exists(q) => {
                Ok(Expression::Exists(Arc::new(self.query_to_plan(q)?)))
            }
            sqlparser::ast::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut list_expr = vec![];
                for item in list {
                    list_expr.push(self.sql_to_rex(item, schema, select)?);
                }
                Ok(Expression::InList(InListExpr::new(
                    Box::new(self.sql_to_rex(expr, schema, select)?),
                    list_expr,
                    *negated,
                )))
            }
            sqlparser::ast::Expr::Nested(e) => self.sql_to_rex(e, schema, select),
            sqlparser::ast::Expr::CompoundIdentifier(ids) => {
                self.process_compound_ident(ids.as_slice(), select)
            }
            sqlparser::ast::Expr::Function(e) => {
                let mut args = Vec::with_capacity(e.args.len());

                // 1. Get the args from context by function name. such as SELECT database()
                // common::ScalarFunctions::udf::database arg is ctx.get_default()
                let ctx_args = ContextFunction::build_args_from_ctx(
                    e.name.to_string().as_str(),
                    self.ctx.clone(),
                )?;
                if !ctx_args.is_empty() {
                    args.extend_from_slice(ctx_args.as_slice());
                }

                // 2. Get args from the ast::Expr:Function
                for arg in &e.args {
                    match &arg {
                        FunctionArg::Named { arg, .. } => {
                            args.push(self.sql_to_rex(arg, schema, select)?);
                        }
                        FunctionArg::Unnamed(arg) => {
                            args.push(self.sql_to_rex(arg, schema, select)?);
                        }
                    }
                }

                let op = e.name.to_string();
                if AggregateFunctionFactory::check(&op) {
                    let args = match op.to_lowercase().as_str() {
                        "count" => args
                            .iter()
                            .map(|c| match c {
                                Expression::Wildcard => common_planners::lit(0i64),
                                _ => c.clone(),
                            })
                            .collect(),
                        _ => args,
                    };
                    return Ok(Expression::AggregateFunction {
                        op,
                        distinct: e.distinct,
                        args,
                    });
                }

                Ok(Expression::ScalarFunction { op, args })
            }
            sqlparser::ast::Expr::Wildcard => Ok(Expression::Wildcard),
            sqlparser::ast::Expr::TypedString { data_type, value } => {
                SQLCommon::make_data_type(data_type).map(|data_type| Expression::Cast {
                    expr: Box::new(Expression::Literal(DataValue::Utf8(Some(value.clone())))),
                    data_type,
                })
            }
            sqlparser::ast::Expr::Cast { expr, data_type } => self
                .sql_to_rex(expr, schema, select)
                .map(Box::from)
                .and_then(|expr| {
                    SQLCommon::make_data_type(data_type)
                        .map(|data_type| Expression::Cast { expr, data_type })
                }),
            sqlparser::ast::Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                let mut args = Vec::with_capacity(3);
                args.push(self.sql_to_rex(expr, schema, select)?);
                if let Some(from) = substring_from {
                    args.push(self.sql_to_rex(from, schema, select)?);
                } else {
                    args.push(Expression::Literal(DataValue::Int64(Some(1))));
                }

                if let Some(len) = substring_for {
                    args.push(self.sql_to_rex(len, schema, select)?);
                }

                Ok(Expression::ScalarFunction {
                    op: "substring".to_string(),
                    args,
                })
            }
            sqlparser::ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expression = self.sql_to_rex(expr, schema, select)?;
                let low_expression = self.sql_to_rex(low, schema, select)?;
                let high_expression = self.sql_to_rex(high, schema, select)?;
                match *negated {
                    false => Ok(expression
                        .gt_eq(low_expression)
                        .and(expression.lt_eq(high_expression))),
                    true => Ok(expression
                        .lt(low_expression)
                        .or(expression.gt(high_expression))),
                }
            }
            other => Result::Err(ErrorCode::SyntaxException(format!(
                "Unsupported expression: {}, type: {:?}",
                expr, other
            ))),
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
        select: Option<&sqlparser::ast::Select>,
    ) -> Result<PlanNode> {
        match *predicate {
            Some(ref predicate_expr) => self
                .sql_to_rex(predicate_expr, &plan.schema(), select)
                .and_then(|filter_expr| {
                    PlanBuilder::from(plan)
                        .filter(filter_expr)
                        .and_then(|builder| builder.build())
                }),
            _ => Ok(plan.clone()),
        }
    }

    /// Apply a having to the plan
    fn having(&self, plan: &PlanNode, expr: Option<Expression>) -> Result<PlanNode> {
        if let Some(expr) = expr {
            let expr = rebase_expr_from_input(&expr, &plan.schema())?;
            return PlanBuilder::from(plan)
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

        PlanBuilder::from(input)
            .project(&exprs)
            .and_then(|builder| builder.build())
    }

    /// Wrap a plan for an aggregate
    fn aggregate(
        &self,
        input: &PlanNode,
        aggr_exprs: &[Expression],
        group_by_exprs: &[Expression],
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
        PlanBuilder::from(input)
            .aggregate_partial(&aggr_exprs, &group_by_exprs)
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

        PlanBuilder::from(input)
            .sort(&order_by_exprs)
            .and_then(|builder| builder.build())
    }

    /// Wrap a plan in a limit
    fn limit(
        &self,
        input: &PlanNode,
        limit: &Option<sqlparser::ast::Expr>,
        offset: &Option<sqlparser::ast::Offset>,
        select: Option<&sqlparser::ast::Select>,
    ) -> Result<PlanNode> {
        match (limit, offset) {
            (None, None) => Ok(input.clone()),
            (limit, offset) => {
                let n = limit
                    .as_ref()
                    .map(|limit_expr| {
                        self.sql_to_rex(limit_expr, &input.schema(), select)
                            .and_then(|limit_expr| match limit_expr {
                                Expression::Literal(v) => Ok(v.as_u64()? as usize),
                                _ => Err(ErrorCode::SyntaxException(format!(
                                    "Unexpected expression for LIMIT clause: {:?}",
                                    limit_expr
                                ))),
                            })
                    })
                    .transpose()?;

                let offset = offset
                    .as_ref()
                    .map(|offset| {
                        let offset_expr = &offset.value;
                        self.sql_to_rex(offset_expr, &input.schema(), select)
                            .and_then(|offset_expr| match offset_expr {
                                Expression::Literal(v) => Ok(v.as_u64()? as usize),
                                _ => Err(ErrorCode::SyntaxException(format!(
                                    "Unexpected expression for OFFSET clause: {:?}",
                                    offset_expr,
                                ))),
                            })
                    })
                    .transpose()?
                    .unwrap_or(0);

                PlanBuilder::from(input)
                    .limit_offset(n, offset)
                    .and_then(|builder| builder.build())
            }
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

        PlanBuilder::from(input)
            .expression(&dedup_exprs, desc)
            .and_then(|builder| builder.build())
    }
}
