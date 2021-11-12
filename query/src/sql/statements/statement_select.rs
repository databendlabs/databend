use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use sqlparser::ast::{Expr, FunctionArg, ObjectName, Offset, OrderByExpr, Query, Select, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins};
use sqlparser::parser::ParserError;

use common_arrow::arrow_format::ipc::flatbuffers::bitflags::_core::future::Future;
use common_datavalues::{DataSchema, DataSchemaRef};
use common_exception::{ErrorCode, Result};
use common_planners::{expand_aggregate_arg_exprs, expr_as_column_expr, Expression, Expressions, extract_aliases, find_aggregate_exprs, find_aggregate_exprs_in_expr, PlanNode, ReadDataSourcePlan, rebase_expr, resolve_aliases_to_exprs};

use crate::catalogs::ToReadDataSourcePlan;
use crate::sessions::{DatabendQueryContext, DatabendQueryContextRef};
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use crate::sql::statements::analyzer_expr::{ExpressionAnalyzer};
use crate::sql::statements::analyzer_schema::{AnalyzeQuerySchema, AnalyzeQueryColumnDesc};

#[derive(Debug, Clone, PartialEq)]
pub struct DfQueryStatement {
    pub from: Vec<TableWithJoins>,
    pub projection: Vec<SelectItem>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Offset>,
}

pub enum QueryRelation {
    FromTable(ReadDataSourcePlan),
    Nested(Box<AnalyzeQueryState>),
}

pub struct AnalyzeQueryState {
    ctx: DatabendQueryContextRef,

    pub relation: Option<Box<QueryRelation>>,
    pub filter_predicate: Option<Expression>,
    pub before_group_by_expressions: Vec<Expression>,
    pub group_by_expressions: Vec<Expression>,
    pub aggregate_expressions: Vec<Expression>,
    pub before_having_expressions: Vec<Expression>,
    pub having_predicate: Option<Expression>,
    pub order_by_expressions: Vec<Expression>,
    pub projection_expressions: Vec<Expression>,

    from_schema: AnalyzeQuerySchema,
    before_aggr_schema: AnalyzeQuerySchema,
    after_aggr_schema: AnalyzeQuerySchema,
    finalize_schema: DataSchemaRef,
    projection_aliases: HashMap<String, Expression>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfQueryStatement {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let mut data = AnalyzeQueryState::create(ctx);

        if let Err(cause) = self.analyze_from(&mut data).await {
            return Err(cause.add_message_back("(while in analyze select from)."));
        }

        if let Err(cause) = self.analyze_filter(&mut data).await {
            return Err(cause.add_message_back("(while in analyze select filter)."));
        }

        // We will analyze the projection before GROUP BY, HAVING, and ORDER BY,
        // because they may access the columns in the projection. Such as:
        // SELECT a + 1 AS b, SUM(c) FROM table_a GROUP BY b;
        if let Err(cause) = self.analyze_projection(&mut data).await {
            return Err(cause.add_message_back("(while in analyze select projection)."));
        }

        if let Err(cause) = self.analyze_group_by(&mut data).await {
            return Err(cause.add_message_back("(while in analyze select group by)."));
        }

        if let Err(cause) = self.analyze_having(&mut data).await {
            return Err(cause.add_message_back("(while in analyze select having)."));
        }

        if let Err(cause) = self.analyze_order_by(&mut data).await {
            return Err(cause.add_message_back("(while in analyze select order by)."));
        }

        if let Err(cause) = self.analyze_limit(&mut data).await {
            return Err(cause.add_message_back("(while in analyze select limit)."));
        }

        // TODO: check and finalize

        unimplemented!()
    }
}

impl DfQueryStatement {
    pub async fn analyze_from(&self, data: &mut AnalyzeQueryState) -> Result<()> {
        match self.from.len() {
            0 => data.dummy_source().await,
            1 => Self::analyze_join(&self.from[0], data).await,
            // It's not `JOIN` clause. Such as SELECT * FROM t1, t2;
            _ => Result::Err(ErrorCode::SyntaxException("Cannot SELECT multiple tables")),
        }
    }

    pub async fn analyze_filter(&self, data: &mut AnalyzeQueryState) -> Result<()> {
        if let Some(predicate) = &self.selection {
            // TODO: collect pushdown predicates
            let analyze_schema = data.from_schema.clone();
            let expr_analyzer = data.create_analyzer(analyze_schema, false);
            data.filter_predicate = Some(expr_analyzer.analyze(predicate).await?);
        }

        Ok(())
    }

    /// Expand wildcard and create columns alias map(alias -> expression) for named projection item
    pub async fn analyze_projection(&self, data: &mut AnalyzeQueryState) -> Result<()> {
        let projection_expressions = self.projection_exprs(data).await?;
        // TODO: expand_wildcard
        data.before_aggr_schema = data.from_schema.clone();
        data.projection_aliases = extract_aliases(&projection_expressions);

        let from_schema = data.from_schema.to_data_schema();
        for projection_expression in &projection_expressions {
            if !data.add_aggregate_function(projection_expression)? {
                if !data.before_group_by_expressions.contains(projection_expression) {
                    data.before_group_by_expressions.push(projection_expression.clone());
                }

                if let Expression::Alias(alias, expr) = projection_expression {
                    let field = expr.to_data_field(&from_schema)?;

                    let nullable = field.is_nullable();
                    let data_type = field.data_type().clone();
                    let column_desc = AnalyzeQueryColumnDesc::create(alias, data_type, nullable);
                    data.before_aggr_schema.add_projection(column_desc, true);
                }
            }
        }

        data.projection_expressions = projection_expressions;
        Ok(())
    }

    /// Push group by exprs and aggregate function inputs into before_group_by_expressions
    pub async fn analyze_group_by(&self, data: &mut AnalyzeQueryState) -> Result<()> {
        let analyze_schema = data.before_aggr_schema.clone();
        let expr_analyzer = data.create_analyzer(analyze_schema, false);
        let projection_aliases = &data.projection_aliases;

        for group_by_expr in &self.group_by {
            let analyzed_expr = expr_analyzer.analyze(group_by_expr).await?;
            let analyzed_expr = resolve_aliases_to_exprs(&analyzed_expr, projection_aliases)?;

            if !data.group_by_expressions.contains(&analyzed_expr) {
                // The expr completed in before_group_by_expressions.
                // let group_by_expression = expr_as_column_expr(&analyzed_expr)?;
                data.group_by_expressions.push(analyzed_expr.clone());
            }

            if !data.before_group_by_expressions.contains(&analyzed_expr) {
                data.before_group_by_expressions.push(analyzed_expr.clone());
            }
        }

        Ok(())
    }

    pub async fn analyze_having(&self, data: &mut AnalyzeQueryState) -> Result<()> {
        if let Some(predicate) = &self.having {
            let expr = Self::analyze_expr_with_alias(predicate, data).await?;

            data.add_aggregate_function(&expr)?;
            data.having_predicate = Some(Self::after_group_by_expr(&expr, data)?);
        }
        Ok(())
    }

    pub async fn analyze_order_by(&self, data: &mut AnalyzeQueryState) -> Result<()> {
        for order_by_expr in &self.order_by {
            let expr = Self::analyze_expr_with_alias(&order_by_expr.expr, data).await?;

            data.add_aggregate_function(&expr)?;
            let after_group_by_expr = Self::after_group_by_expr(&expr, data)?;
            let order_by_column_expr = expr_as_column_expr(&after_group_by_expr)?;

            data.before_having_expressions.push(after_group_by_expr);
            data.order_by_expressions.push(Expression::Sort {
                expr: Box::new(order_by_column_expr),
                asc: order_by_expr.asc.unwrap_or(true),
                nulls_first: order_by_expr.asc.unwrap_or(true),
            });
        }

        Ok(())
    }

    pub async fn analyze_limit(&self, data: &mut AnalyzeQueryState) -> Result<()> {
        // TODO: analyze limit
        Ok(())
    }

    fn after_group_by_expr(expr: &Expression, data: &mut AnalyzeQueryState) -> Result<Expression> {
        let aggr_exprs = &data.aggregate_expressions;
        let rebased_expr = rebase_expr(&expr, aggr_exprs)?;
        rebase_expr(&rebased_expr, &data.before_having_expressions)
    }

    async fn analyze_expr_with_alias(expr: &Expr, data: &mut AnalyzeQueryState) -> Result<Expression> {
        let analyze_schema = data.after_aggr_schema.clone();
        let expr_analyzer = data.create_analyzer(analyze_schema, true);
        let analyzed_expr = expr_analyzer.analyze(expr).await?;
        let projection_aliases = &data.projection_aliases;
        resolve_aliases_to_exprs(&analyzed_expr, projection_aliases)
    }

    async fn analyze_join(table: &TableWithJoins, data: &mut AnalyzeQueryState) -> Result<()> {
        let TableWithJoins { relation, joins } = table;

        match joins.is_empty() {
            true => Err(ErrorCode::UnImplement("Cannot SELECT join.")),
            false => Self::analyze_table_factor(relation, data).await,
        }
    }

    async fn analyze_table_factor(relation: &TableFactor, data: &mut AnalyzeQueryState) -> Result<()> {
        match relation {
            TableFactor::Table { name, args, alias, .. } => {
                match args.is_empty() {
                    true => data.named_table(name, alias).await,
                    false if alias.is_none() => data.table_function(name, args).await,
                    false => Err(ErrorCode::SyntaxException("Table function cannot named.")),
                }
            },
            TableFactor::Derived { lateral, subquery, alias } => {
                if *lateral {
                    return Err(ErrorCode::UnImplement("Cannot SELECT LATERAL subquery."));
                }

                data.subquery(subquery, alias).await
            },
            TableFactor::NestedJoin(nested) => Self::analyze_join(&nested, data).await,
            TableFactor::TableFunction { .. } => Err(ErrorCode::UnImplement("Unsupported table function")),
        }
    }

    async fn projection_exprs(&self, data: &mut AnalyzeQueryState) -> Result<Vec<Expression>> {
        // XXX: We do not allow projection to be used in projection, such as:
        // SELECT column_a + 1 AS alias_b, alias_b + 1 AS alias_c FROM table_name_1;
        // This is also not supported in MySQL, but maybe we should to support it?

        let source = data.from_schema.clone();
        let query_context = data.ctx.clone();
        let expr_analyzer = ExpressionAnalyzer::with_source(query_context, source, true);

        let mut output_columns = Vec::with_capacity(self.projection.len());
        for item in &self.projection {
            match item {
                SelectItem::Wildcard => output_columns.push(Expression::Wildcard),
                SelectItem::UnnamedExpr(expr) => {
                    output_columns.push(expr_analyzer.analyze(expr).await?);
                },
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr_alias = alias.value.clone();
                    let expr = Box::new(expr_analyzer.analyze(expr).await?);
                    output_columns.push(Expression::Alias(expr_alias, expr));
                }
                _ => { return Err(ErrorCode::SyntaxException(format!("SelectItem: {:?} are not supported", item))); }
            };
        }

        Ok(output_columns)
    }
}

enum AnalyzeTableWithJoins {}

impl AnalyzeQueryState {
    pub fn create(ctx: DatabendQueryContextRef) -> AnalyzeQueryState {
        AnalyzeQueryState { ctx, ..Default::default() }
    }

    pub fn create_analyzer(&self, schema: AnalyzeQuerySchema, aggr: bool) -> ExpressionAnalyzer {
        let query_context = self.ctx.clone();
        ExpressionAnalyzer::with_source(query_context, schema, aggr)
    }

    pub fn add_aggregate_function(&mut self, expr: &Expression) -> Result<bool> {
        let aggregate_exprs = find_aggregate_exprs_in_expr(&expr);
        let aggregate_exprs_require_expression = expand_aggregate_arg_exprs(&aggregate_exprs);

        for require_expression in aggregate_exprs_require_expression {
            if !self.before_group_by_expressions.contains(&require_expression) {
                self.before_group_by_expressions.push(require_expression)
            }
        }

        for aggregate_expr in aggregate_exprs {
            if !self.aggregate_expressions.contains(&aggregate_expr) {
                self.aggregate_expressions.push(aggregate_expr);
            }
        }

        let mut expr = expr.clone();
        if let Expression::Alias(_, inner) = &expr {
            expr = inner.as_ref().clone();
        }

        if !aggregate_exprs.is_empty() && !self.before_having_expressions.contains(&expr) {
            self.before_having_expressions.push(expr);
        }

        Ok(!aggregate_exprs.is_empty())
    }

    pub async fn dummy_source(&mut self) -> Result<()> {
        // We cannot reference field by name, such as `SELECT system.one.dummy`
        let context = self.ctx.as_ref();
        let dummy_table = context.get_table("system", "one")?;
        // self.tables_schema.push(TableSchema::UnNamed(dummy_table.schema()));
        Ok(())
    }

    pub async fn table_function(&mut self, name: &ObjectName, args: &[FunctionArg]) -> Result<()> {
        if name.0.len() >= 2 {
            return Result::Err(ErrorCode::BadArguments(
                "Currently table can't have arguments",
            ));
        }

        let mut table_args = Vec::with_capacity(args.len());

        for table_arg in args {
            table_args.push(match table_arg {
                FunctionArg::Named { arg, .. } => arg,
                FunctionArg::Unnamed(arg) => arg,
            });
        }

        let context = &self.ctx;
        let table_function = context.get_table_function(&table_name, Some(table_args))?;
        // self.tables_schema.push(TableSchema::UnNamed(table_function.schema()));
        Ok(())
    }

    // TODO(Winter): await query_context.get_table
    pub async fn named_table(&mut self, name: &ObjectName, alias: &Option<TableAlias>) -> Result<()> {
        let query_context = &self.ctx;
        let (database, table) = self.resolve_table(name)?;
        let read_table = query_context.get_table(&database, &table)?;

        match alias {
            None => {
                let schema = read_table.schema();
                // let analyzed_schema = AnalyzedSchema::unnamed_table(database, table, schema);
                // let table_schema = TableSchema::Default { database, table, schema };
                self.tables_schema.push(table_schema);
            }
            Some(table_alias) => {
                let alias = table_alias.name.value.clone();
                // let analyzed_schema = AnalyzedSchema::named_table(alias, schema);
                // self.tables_schema.push(TableSchema::Named(alias, read_table.schema()));
            }
        }

        Ok(())
    }

    pub async fn subquery(&mut self, subquery: &Query, alias: &Option<TableAlias>) -> Result<()> {
        // TODO: remove clone.
        let subquery = subquery.clone();
        let query_statement = DfQueryStatement::try_from(subquery)?;
        let analyzed_subquery = query_statement.analyze(self.ctx.clone()).await?;

        match analyzed_subquery {
            AnalyzedResult::SelectQuery(analyzed_subquery) => match alias {
                None => {
                    let subquery_finalize_schema = analyzed_subquery.finalize_schema.clone();
                    // self.tables_schema.push(TableSchema::UnNamed(subquery_finalize_schema));
                    Ok(())
                }
                Some(TableAlias { name, .. }) => {
                    let subquery_alias = name.value.clone();
                    let subquery_finalize_schema = analyzed_subquery.finalize_schema.clone();
                    // let named_schema = TableSchema::Named(subquery_alias, subquery_finalize_schema);
                    self.tables_schema.push(named_schema);
                    Ok(())
                }
            }
            _ => unreachable!()
        }
    }

    fn resolve_table(&self, name: &ObjectName) -> Result<(String, String)> {
        let query_context = &self.ctx;

        match name.0.len() {
            0 => Err(ErrorCode::SyntaxException("Table name is empty")),
            1 => Ok((query_context.get_current_database(), name.0[0].value.clone())),
            2 => Ok((name.0[0].value.clone(), name.0[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException("Table name must be [`db`].`table`"))
        }
    }
}

impl TryFrom<Query> for DfQueryStatement {
    type Error = ParserError;

    fn try_from(query: Query) -> Result<Self> {
        if query.with.is_some() {
            return Err(ErrorCode::UnImplement("CTE is not yet implement"));
        }

        if query.fetch.is_some() {
            return Err(ErrorCode::UnImplement("FETCH is not yet implement"));
        }

        let body = match &query.body {
            SetExpr::Select(query) => query,
            other => {
                return Err(ErrorCode::UnImplement(format!("Query {} is not yet implemented", other)));
            }
        };

        if body.top.is_some() {
            return Err(ErrorCode::UnImplement("TOP is not yet implement"));
        }

        if !body.cluster_by.is_empty() {
            return Err(ErrorCode::SyntaxException("Cluster by is unsupport"));
        }

        if !body.sort_by.is_empty() {
            return Err(ErrorCode::SyntaxException("Sort by is unsupport"));
        }

        if !body.distribute_by.is_empty() {
            return Err(ErrorCode::SyntaxException("Distribute by is unsupport"));
        }

        Ok(DfQueryStatement {
            from: body.from.clone(),
            projection: body.projection.clone(),
            selection: body.selection.clone(),
            group_by: body.group_by.clone(),
            having: body.having.clone(),
            order_by: query.order_by.clone(),
            limit: query.limit.clone(),
            offset: query.offset.clone(),
        })
    }
}

