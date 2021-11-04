// Copyright 2020 Datafuse Labs.
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

use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_infallible::Mutex;
use common_meta_types::TableMeta;
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
use common_planners::CreateUserPlan;
use common_planners::DescribeTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_planners::ExplainPlan;
use common_planners::Expression;
use common_planners::Extras;
use common_planners::InsertIntoPlan;
use common_planners::KillPlan;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::SelectPlan;
use common_planners::SettingPlan;
use common_planners::ShowCreateTablePlan;
use common_planners::TruncateTablePlan;
use common_planners::UseDatabasePlan;
use common_planners::VarValue;
use common_streams::Source;
use common_streams::ValueSource;
use common_tracing::tracing;
use nom::FindSubstring;
use sqlparser::ast::FunctionArg;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::OrderByExpr;
use sqlparser::ast::Query;
use sqlparser::ast::Statement;
use sqlparser::ast::TableFactor;
use sqlparser::ast::UnaryOperator;

use crate::catalogs::Catalog;
use crate::catalogs::ToReadDataSourcePlan;
use crate::functions::ContextFunction;
use crate::sessions::DatabendQueryContextRef;
use crate::sql::sql_statement::DfCreateTable;
use crate::sql::sql_statement::DfDropDatabase;
use crate::sql::sql_statement::DfUseDatabase;
use crate::sql::DfCreateDatabase;
use crate::sql::DfCreateUser;
use crate::sql::DfDescribeTable;
use crate::sql::DfDropTable;
use crate::sql::DfExplain;
use crate::sql::DfHint;
use crate::sql::DfKillStatement;
use crate::sql::DfParser;
use crate::sql::DfShowCreateTable;
use crate::sql::DfShowDatabases;
use crate::sql::DfShowTables;
use crate::sql::DfStatement;
use crate::sql::DfTruncateTable;
use crate::sql::SQLCommon;

pub struct PlanParser {
    ctx: DatabendQueryContextRef,
}

impl PlanParser {
    pub fn create(ctx: DatabendQueryContextRef) -> Self {
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
            DfStatement::ShowDatabases(v) => self.sql_show_databases_to_plan(v),
            DfStatement::CreateDatabase(v) => self.sql_create_database_to_plan(v),
            DfStatement::DropDatabase(v) => self.sql_drop_database_to_plan(v),
            DfStatement::CreateTable(v) => self.sql_create_table_to_plan(v),
            DfStatement::DescribeTable(v) => self.sql_describe_table_to_plan(v),
            DfStatement::DropTable(v) => self.sql_drop_table_to_plan(v),
            DfStatement::TruncateTable(v) => self.sql_truncate_table_to_plan(v),
            DfStatement::UseDatabase(v) => self.sql_use_database_to_plan(v),
            DfStatement::ShowCreateTable(v) => self.sql_show_create_table_to_plan(v),
            DfStatement::ShowTables(df) => {
                let show_sql = match df {
                    DfShowTables::All => {
                        format!(
                            "SELECT name FROM system.tables where database = '{}' ORDER BY database, name",
                            self.ctx.get_current_database()
                        )
                    }
                    DfShowTables::Like(i) => {
                        format!(
                            "SELECT name FROM system.tables where database = '{}' AND name LIKE {} ORDER BY database, name",
                            self.ctx.get_current_database(), i,
                        )
                    }
                    DfShowTables::Where(e) => {
                        format!(
                            "SELECT name FROM system.tables where database = '{}' AND ({}) ORDER BY database, name",
                            self.ctx.get_current_database(), e,
                        )
                    }
                    DfShowTables::FromOrIn(name) => {
                        format!(
                            "SELECT name FROM system.tables where database = '{}' ORDER BY database, name",
                            name.0[0].value.clone()
                        )
                    }
                };
                self.build_from_sql(show_sql.as_str())
            }
            DfStatement::ShowSettings(_) => {
                self.build_from_sql("SELECT name, value FROM system.settings ORDER BY name")
            }
            DfStatement::ShowProcessList(_) => {
                self.build_from_sql("SELECT * FROM system.processes")
            }
            DfStatement::ShowMetrics(_) => self.build_from_sql("SELECT * FROM system.metrics"),
            DfStatement::KillQuery(v) => self.sql_kill_query_to_plan(v),
            DfStatement::KillConn(v) => self.sql_kill_connection_to_plan(v),
            DfStatement::CreateUser(v) => self.sql_create_user_to_plan(v),
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
            } => {
                let format_sql = format!("{}", statement);
                self.insert_to_plan(table_name, columns, source, &format_sql)
            }

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
            options,
        }))
    }

    /// DfShowDatabase to plan
    #[tracing::instrument(level = "info", skip(self, show), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub fn sql_show_databases_to_plan(&self, show: &DfShowDatabases) -> Result<PlanNode> {
        let where_clause = match &show.where_opt {
            Some(expr) => format!("WHERE {}", expr),
            None => String::from(""),
        };

        self.build_from_sql(
            format!(
                "SELECT name AS Database FROM system.databases {} ORDER BY name",
                where_clause
            )
            .as_str(),
        )
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

    #[tracing::instrument(level = "info", skip(self, kill), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub fn sql_kill_query_to_plan(&self, kill: &DfKillStatement) -> Result<PlanNode> {
        let id = kill.object_id.value.clone();
        Ok(PlanNode::Kill(KillPlan {
            id,
            kill_connection: false,
        }))
    }

    #[tracing::instrument(level = "info", skip(self, kill), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub fn sql_kill_connection_to_plan(&self, kill: &DfKillStatement) -> Result<PlanNode> {
        let id = kill.object_id.value.clone();
        Ok(PlanNode::Kill(KillPlan {
            id,
            kill_connection: true,
        }))
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
            table_meta: TableMeta {
                schema,
                engine: create.engine.clone(),
                options,
            },
        }))
    }

    #[tracing::instrument(level = "info", skip(self, create), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub fn sql_create_user_to_plan(&self, create: &DfCreateUser) -> Result<PlanNode> {
        Ok(PlanNode::CreateUser(CreateUserPlan {
            name: create.name.clone(),
            password: Vec::from(create.password.clone()),
            host_name: create.host_name.clone(),
            auth_type: create.auth_type.clone(),
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
            DataField::new("Table", DataType::String, false),
            DataField::new("Create Table", DataType::String, false),
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
            DataField::new("Field", DataType::String, false),
            DataField::new("Type", DataType::String, false),
            DataField::new("Null", DataType::String, false),
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

    // DfTruncateTable to plan.
    #[tracing::instrument(level = "info", skip(self, truncate), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub fn sql_truncate_table_to_plan(&self, truncate: &DfTruncateTable) -> Result<PlanNode> {
        let mut db = self.ctx.get_current_database();
        if truncate.name.0.is_empty() {
            return Result::Err(ErrorCode::SyntaxException(
                "TruncateTable table name is empty",
            ));
        }
        let mut table = truncate.name.0[0].value.clone();
        if truncate.name.0.len() > 1 {
            db = table;
            table = truncate.name.0[1].value.clone();
        }

        Ok(PlanNode::TruncateTable(TruncateTablePlan { db, table }))
    }

    #[tracing::instrument(level = "info", skip(self, table_name, columns, source), fields(ctx.id = self.ctx.get_id().as_str()))]
    fn insert_to_plan(
        &self,
        table_name: &ObjectName,
        columns: &[Ident],
        source: &Option<Box<Query>>,
        format_sql: &str,
    ) -> Result<PlanNode> {
        let mut db_name = self.ctx.get_current_database();
        let mut tbl_name = table_name.0[0].value.clone();

        if table_name.0.len() > 1 {
            db_name = tbl_name;
            tbl_name = table_name.0[1].value.clone();
        }

        let table = self.ctx.get_table(&db_name, &tbl_name)?;
        let mut schema = table.schema();
        let tbl_id = table.get_id();

        if !columns.is_empty() {
            let fields = columns
                .iter()
                .map(|ident| schema.field_with_name(&ident.value).map(|v| v.clone()))
                .collect::<Result<Vec<_>>>()?;

            schema = DataSchemaRefExt::create(fields);
        }

        let mut input_stream = futures::stream::iter::<Vec<DataBlock>>(vec![]);

        if let Some(source) = source {
            if let sqlparser::ast::SetExpr::Values(_vs) = &source.body {
                tracing::debug!("{:?}", format_sql);
                let index = format_sql.find_substring(" VALUES ").unwrap();
                let values = &format_sql[index + " VALUES ".len()..];

                let block_size = self.ctx.get_settings().get_max_block_size()? as usize;
                let mut source = ValueSource::new(values.as_bytes(), schema.clone(), block_size);
                let mut blocks = vec![];
                loop {
                    let block = source.read()?;
                    match block {
                        Some(b) => blocks.push(b),
                        None => break,
                    }
                }
                input_stream = futures::stream::iter(blocks);
            }
        }

        let plan_node = InsertIntoPlan {
            db_name,
            tbl_name,
            tbl_id,
            schema,
            input_stream: Arc::new(Mutex::new(Some(Box::pin(input_stream)))),
        };
        Ok(PlanNode::InsertInto(plan_node))
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
            // Such as SELECT * FROM t1, t2;
            // It's not `JOIN` clause.
            _ => Result::Err(ErrorCode::SyntaxException("Cannot SELECT multiple tables")),
        }
    }

    fn plan_with_dummy_source(&self) -> Result<PlanNode> {
        let db_name = "system";
        let table_name = "one";

        let table = self.ctx.get_table(db_name, table_name)?;

        // TODO(xp): is it possible to use get_cluster_table_io_context() here?
        let io_ctx = self.ctx.get_single_node_table_io_context()?;
        let io_ctx = Arc::new(io_ctx);

        let source_plan = table.read_plan(
            io_ctx,
            Some(Extras::default()),
            Some(self.ctx.get_settings().get_max_threads()? as usize),
        )?;

        let dummy_read_plan = PlanNode::ReadSource(source_plan);
        Ok(dummy_read_plan)
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
                let table;

                // only table functions has table args
                if !args.is_empty() {
                    if name.0.len() >= 2 {
                        return Result::Err(ErrorCode::BadArguments(
                            "Currently table can't have arguments",
                        ));
                    }

                    let empty_schema = Arc::new(DataSchema::empty());
                    let table_args = args.iter().try_fold(
                        Vec::with_capacity(args.len()),
                        |mut acc, f_arg| {
                            let item = match f_arg {
                                FunctionArg::Named { arg, .. } => {
                                    self.sql_to_rex(arg, empty_schema.as_ref(), None)?
                                }
                                FunctionArg::Unnamed(arg) => {
                                    self.sql_to_rex(arg, empty_schema.as_ref(), None)?
                                }
                            };
                            acc.push(item);
                            Ok::<_, ErrorCode>(acc)
                        },
                    )?;

                    let table_func = self
                        .ctx
                        .get_catalog()
                        .get_table_function(&table_name, Some(table_args))?;
                    table = table_func.as_table();
                } else {
                    table = self.ctx.get_table(&db_name, &table_name)?;
                }

                // TODO(xp): is it possible to use get_cluster_table_io_context() here?
                let io_ctx = self.ctx.get_single_node_table_io_context()?;
                let io_ctx = Arc::new(io_ctx);

                let partitions = self.ctx.get_settings().get_max_threads()? as usize;

                // TODO: Move ReadSourcePlan to SelectInterpreter
                let source_plan = table.read_plan(
                    io_ctx,
                    Some(Extras::default()),
                    // TODO(xp): remove partitions, partitioning hint has been included in io_ctx.max_threads and io_ctx.query_nodes
                    Some(partitions),
                )?;

                let dummy_read_plan = PlanNode::ReadSource(source_plan);
                Ok(dummy_read_plan)
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

    fn interval_to_day_time(days: i32, ms: i32) -> Result<Expression> {
        let data_type = DataType::Interval(IntervalUnit::DayTime);
        let milliseconds_per_day = 24 * 3600 * 1000;
        let total_ms = days as i64 * milliseconds_per_day + ms as i64;

        Ok(Expression::Literal {
            value: DataValue::Int64(Some(total_ms)),
            column_name: Some(total_ms.to_string()),
            data_type,
        })
    }

    fn interval_to_year_month(months: i32) -> Result<Expression> {
        let data_type = DataType::Interval(IntervalUnit::YearMonth);

        Ok(Expression::Literal {
            value: DataValue::Int64(Some(months as i64)),
            column_name: Some(months.to_string()),
            data_type,
        })
    }

    fn interval_to_rex(
        value: &str,
        interval_kind: sqlparser::ast::DateTimeField,
    ) -> Result<Expression> {
        let num = value.parse::<i32>()?; // we only accept i32 for number in "interval [num] [year|month|day|hour|minute|second]"
        match interval_kind {
            sqlparser::ast::DateTimeField::Year => Self::interval_to_year_month(num * 12),
            sqlparser::ast::DateTimeField::Month => Self::interval_to_year_month(num),
            sqlparser::ast::DateTimeField::Day => Self::interval_to_day_time(num, 0),
            sqlparser::ast::DateTimeField::Hour => Self::interval_to_day_time(0, num * 3600 * 1000),
            sqlparser::ast::DateTimeField::Minute => Self::interval_to_day_time(0, num * 60 * 1000),
            sqlparser::ast::DateTimeField::Second => Self::interval_to_day_time(0, num * 1000),
        }
    }

    fn value_to_rex(value: &sqlparser::ast::Value) -> Result<Expression> {
        match value {
            sqlparser::ast::Value::Number(ref n, _) => {
                DataValue::try_from_literal(n).map(Expression::create_literal)
            }
            sqlparser::ast::Value::SingleQuotedString(ref value) => Ok(Expression::create_literal(
                DataValue::String(Some(value.clone().into_bytes())),
            )),
            sqlparser::ast::Value::Boolean(b) => {
                Ok(Expression::create_literal(DataValue::Boolean(Some(*b))))
            }
            sqlparser::ast::Value::Interval {
                value: value_expr,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            } => {
                // We don't support full interval expression like 'Interval ..To.. '. Currently only partial interval expression like "interval [num] [unit]" is supported.
                if leading_precision.is_some()
                    || last_field.is_some()
                    || fractional_seconds_precision.is_some()
                {
                    return Result::Err(ErrorCode::SyntaxException(format!(
                        "Unsupported interval expression: {}.",
                        value
                    )));
                }

                // When the input is like "interval '1 hour'", leading_field will be None and value_expr will be '1 hour'.
                // We may want to support this pattern in native paser (sqlparser-rs), to have a parsing result that leading_field is Some(Hour) and value_expr is number '1'.
                if leading_field.is_none() {
                    //TODO: support parsing literal interval like '1 hour'
                    return Result::Err(ErrorCode::SyntaxException(format!(
                        "Unsupported interval expression: {}.",
                        value
                    )));
                }
                Self::interval_to_rex(value_expr, leading_field.clone().unwrap())
            }
            sqlparser::ast::Value::Null => Ok(Expression::create_literal(DataValue::Null)),
            other => Result::Err(ErrorCode::SyntaxException(format!(
                "Unsupported value expression: {}, type: {:?}",
                value, other
            ))),
        }
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(
        &self,
        expr: &sqlparser::ast::Expr,
        schema: &DataSchema,
        select: Option<&sqlparser::ast::Select>,
    ) -> Result<Expression> {
        match expr {
            sqlparser::ast::Expr::Value(value) => Self::value_to_rex(value),
            sqlparser::ast::Expr::Identifier(ref v) => Ok(Expression::Column(v.clone().value)),
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                Ok(Expression::BinaryExpression {
                    op: format!("{}", op),
                    left: Box::new(self.sql_to_rex(left, schema, select)?),
                    right: Box::new(self.sql_to_rex(right, schema, select)?),
                })
            }
            sqlparser::ast::Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Plus => self.sql_to_rex(expr, schema, select),
                _ => Ok(Expression::UnaryExpression {
                    op: format!("{}", op),
                    expr: Box::new(self.sql_to_rex(expr, schema, select)?),
                }),
            },
            sqlparser::ast::Expr::IsNull(expr) => Ok(Expression::ScalarFunction {
                op: "isnull".to_owned(),
                args: vec![self.sql_to_rex(expr, schema, select)?],
            }),
            sqlparser::ast::Expr::IsNotNull(expr) => Ok(Expression::ScalarFunction {
                op: "isnotnull".to_owned(),
                args: vec![self.sql_to_rex(expr, schema, select)?],
            }),
            sqlparser::ast::Expr::Exists(q) => Ok(Expression::ScalarFunction {
                op: "EXISTS".to_lowercase(),
                args: vec![self.subquery_to_rex(q)?],
            }),
            sqlparser::ast::Expr::Subquery(q) => Ok(self.scalar_subquery_to_rex(q)?),
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
                if AggregateFunctionFactory::instance().check(&op) {
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

                    let params = e
                        .params
                        .iter()
                        .map(|v| {
                            let expr = Self::value_to_rex(v);
                            if let Ok(Expression::Literal { value, .. }) = expr {
                                Ok(value)
                            } else {
                                Result::Err(ErrorCode::SyntaxException(format!(
                                    "Unsupported value expression: {:?}, must be datavalue",
                                    expr
                                )))
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;

                    return Ok(Expression::AggregateFunction {
                        op,
                        distinct: e.distinct,
                        params,
                        args,
                    });
                }

                Ok(Expression::ScalarFunction { op, args })
            }
            sqlparser::ast::Expr::Wildcard => Ok(Expression::Wildcard),
            sqlparser::ast::Expr::TypedString { data_type, value } => {
                SQLCommon::make_data_type(data_type).map(|data_type| Expression::Cast {
                    expr: Box::new(Expression::create_literal(DataValue::String(Some(
                        value.clone().into_bytes(),
                    )))),
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
                    args.push(Expression::create_literal(DataValue::Int64(Some(1))));
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

    pub fn subquery_to_rex(&self, subquery: &Query) -> Result<Expression> {
        let subquery = self.query_to_plan(subquery)?;
        let subquery_name = self.ctx.get_subquery_name(&subquery);
        Ok(Expression::Subquery {
            name: subquery_name,
            query_plan: Arc::new(subquery),
        })
    }

    pub fn scalar_subquery_to_rex(&self, subquery: &Query) -> Result<Expression> {
        let subquery = self.query_to_plan(subquery)?;
        let subquery_name = self.ctx.get_subquery_name(&subquery);
        Ok(Expression::ScalarSubquery {
            name: subquery_name,
            query_plan: Arc::new(subquery),
        })
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
                                Expression::Literal { value, .. } => Ok(value.as_u64()? as usize),
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
                                Expression::Literal { value, .. } => Ok(value.as_u64()? as usize),
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
