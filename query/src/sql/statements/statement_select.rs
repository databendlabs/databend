use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use sqlparser::ast::{Query, SetExpr, Select, TableWithJoins, TableFactor, ObjectName, TableAlias, FunctionArg, Expr, SelectItem, OrderByExpr, Offset};
use crate::sessions::{DatabendQueryContextRef, DatabendQueryContext};
use common_exception::{Result, ErrorCode};
use crate::catalogs::ToReadDataSourcePlan;
use std::sync::Arc;
use common_planners::{PlanNode, ReadDataSourcePlan, Expression, extract_aliases, resolve_aliases_to_exprs, find_aggregate_exprs, find_aggregate_exprs_in_expr, expand_aggregate_arg_exprs, rebase_expr, Expressions, expr_as_column_expr};
use common_datavalues::{DataSchema, DataSchemaRef};
use common_arrow::arrow_format::ipc::flatbuffers::bitflags::_core::future::Future;
use crate::sql::statements::analyzer_expr::{TableSchema, ExpressionAnalyzer};
use std::collections::HashMap;
use std::convert::TryFrom;
use sqlparser::parser::ParserError;
use crate::sql::statements::analyzer_schema::AnalyzedSchema;

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
    Nested(AnalyzeData),
}

pub struct AnalyzeData {
    ctx: DatabendQueryContextRef,

    pub relation: Option<QueryRelation>,
    pub filter_predicate: Option<Expression>,
    pub before_group_by_expressions: Vec<Expression>,
    pub group_by_expressions: Vec<Expression>,
    pub aggregate_expressions: Vec<Expression>,
    pub before_having_expressions: Vec<Expression>,
    pub having_predicate: Option<Expression>,
    pub order_by_expressions: Vec<Expression>,
    pub projection_expressions: Vec<Expression>,

    tables_schema: Vec<TableSchema>,
    from_schema: AnalyzedSchema,
    finalize_schema: DataSchemaRef,
    projection_aliases: HashMap<String, Expression>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfQueryStatement {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let mut data = AnalyzeData::create(ctx);

        if let Err(cause) = self.analyze_from(&mut data).await {
            return Err(cause.add_message_back("(while in analyze select from)."));
        }

        if let Err(cause) = self.analyze_filter(&mut data).await {
            return Err(cause.add_message_back("(while in analyze select filter)."));
        }

        /// We will analyze the projection before GROUP BY, HAVING, and ORDER BY,
        /// because they may access the columns in the projection. Such as:
        /// SELECT a + 1 AS b, SUM(c) FROM table_a GROUP BY b;
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
    pub async fn analyze_from(&self, data: &mut AnalyzeData) -> Result<()> {
        match self.from.len() {
            0 => data.dummy_source().await,
            1 => Self::analyze_join(&self.from[0], data).await,
            // It's not `JOIN` clause. Such as SELECT * FROM t1, t2;
            _ => Result::Err(ErrorCode::SyntaxException("Cannot SELECT multiple tables")),
        }
    }

    pub async fn analyze_filter(&self, data: &mut AnalyzeData) -> Result<()> {
        if let Some(predicate) = &self.selection {
            // TODO: collect pushdown predicates
            let expr_analyzer = data.get_expr_analyze::<false>();
            data.filter_predicate = Some(expr_analyzer.analyze(predicate)?);
        }

        Ok(())
    }

    /// Expand wildcard and create columns alias map(alias -> expression) for named projection item
    pub async fn analyze_projection(&self, data: &mut AnalyzeData) -> Result<()> {
        let items = &self.projection;
        let projection_expressions = Self::projection_exprs(items, data)?;
        // TODO: expand_wildcard
        data.projection_aliases = extract_aliases(&projection_expressions);
        data.projection_expressions = projection_expressions;
        Ok(())
    }

    /// Push group by exprs and aggregate function inputs into before_group_by_expressions
    pub async fn analyze_group_by(&self, data: &mut AnalyzeData) -> Result<()> {
        let expr_analyzer = data.get_expr_analyze::<false>();
        let projection_aliases = &data.projection_aliases;

        for group_by_expr in &self.group_by {
            let analyzed_expr = expr_analyzer.analyze(group_by_expr)?;
            let analyzed_expr = resolve_aliases_to_exprs(&analyzed_expr, projection_aliases)?;

            if !data.group_by_expressions.contains(&analyzed_expr) {
                // The expr completed in before_group_by_expressions.
                let group_by_expression = expr_as_column_expr(&analyzed_expr)?;
                data.group_by_expressions.push(group_by_expression);
            }

            if !data.before_group_by_expressions.contains(&analyzed_expr) {
                data.before_group_by_expressions.push(analyzed_expr);
            }
        }

        Ok(())
    }

    pub async fn analyze_having(&self, data: &mut AnalyzeData) -> Result<()> {
        if let Some(predicate) = &self.having {
            let expr = Self::analyze_expr_with_alias(predicate, data)?;

            data.add_aggregate_function(&expr)?;
            data.having_predicate = Some(Self::after_group_by_expr(&expr, data)?);
        }
        Ok(())
    }

    pub async fn analyze_order_by(&self, data: &mut AnalyzeData) -> Result<()> {
        for order_by_expr in &self.order_by {
            let expr = Self::analyze_expr_with_alias(&order_by_expr.expr, data)?;

            data.add_aggregate_function(&expr)?;
            let after_group_by_expr = Self::after_group_by_expr(&expr, data)?;
            data.before_having_expressions.push(after_group_by_expr);
            data.order_by_expressions.push(Expression::Sort {
                expr: Box::new(expr_as_column_expr(&after_group_by_expr)?),
                asc: order_by_expr.asc.unwrap_or(true),
                nulls_first: order_by_expr.asc.unwrap_or(true),
            });
        }

        Ok(())
    }

    pub async fn analyze_limit(&self, data: &mut AnalyzeData) -> Result<()> {
        // TODO: analyze limit
        Ok(())
    }

    fn after_group_by_expr(expr: &Expression, data: &mut AnalyzeData) -> Result<Expression> {
        let aggr_exprs = &data.aggregate_expressions;
        let rebased_expr = rebase_expr(&expr, aggr_exprs)?;
        rebase_expr(&rebased_expr, &data.before_group_by_expressions)
    }

    fn analyze_expr_with_alias(expr: &Expr, data: &mut AnalyzeData) -> Result<Expression> {
        let analyzed_expr = data.get_expr_analyze::<true>().analyze(expr)?;
        let projection_aliases = &data.projection_aliases;
        resolve_aliases_to_exprs(&analyzed_expr, projection_aliases)
    }

    async fn analyze_join(table: &TableWithJoins, data: &mut AnalyzeData) -> Result<()> {
        let TableWithJoins { relation, joins } = table;

        match joins.is_empty() {
            true => Err(ErrorCode::UnImplement("Cannot SELECT join.")),
            false => Self::analyze_table_factor(relation, data).await
        }
    }

    async fn analyze_table_factor(relation: &TableFactor, data: &mut AnalyzeData) -> Result<()> {
        match relation {
            TableFactor::Table { name, args, alias, .. } => {
                match args.is_empty() {
                    true => data.named_table(name, alias).await,
                    false if alias.is_none() => data.table_function(name, args).await,
                    false => Err(ErrorCode::SyntaxException("Table function cannot named.")),
                }
            },
            TableFactor::Derived { lateral, subquery, alias } => {
                if lateral {
                    return Err(ErrorCode::UnImplement("Cannot SELECT LATERAL subquery."));
                }

                let subquery = subquery.as_ref().clone();
                let query_statement = DfQueryStatement::try_from(subquery)?;
                match query_statement.analyze(data.ctx.clone()).await? {
                    AnalyzedResult::SelectQuery(new_data) => {
                        // TODO: named subquery
                        let subquery_finalize_schema = new_data.finalize_schema.clone();
                        data.tables_schema.push(TableSchema::UnNamed(subquery_finalize_schema));
                        Ok(())
                    }
                    _ => unreachable!()
                }
            },
            TableFactor::NestedJoin(nested) => Self::analyze_join(nested, data),
            TableFactor::TableFunction { .. } => Err(ErrorCode::UnImplement("Unsupported table function")),
        }
    }

    fn projection_exprs(items: &[SelectItem], data: &mut AnalyzeData) -> Result<Vec<Expression>> {
        // XXX: We do not allow projection to be used in projection, such as:
        // SELECT column_a + 1 AS alias_b, alias_b + 1 AS alias_c FROM table_name_1;
        // This is also not supported in MySQL, but maybe we should to support it?

        let query_context = data.ctx.clone();
        let tables_schema = data.tables_schema.clone();
        let expr_analyzer = ExpressionAnalyzer::with_tables(query_context, tables_schema);

        let mut output_columns = Vec::with_capacity(items.len());
        for item in items {
            match item {
                SelectItem::Wildcard => output_columns.push(Expression::Wildcard),
                SelectItem::UnnamedExpr(expr) => {
                    output_columns.push(expr_analyzer.analyze(expr)?);
                },
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr_alias = alias.value.clone();
                    let expr = Box::new(expr_analyzer.analyze(expr)?);
                    output_columns.push(Expression::Alias(expr_alias, expr));
                }
                _ => { return Err(ErrorCode::SyntaxException(format!("SelectItem: {:?} are not supported", item))); }
            };
        }

        Ok(output_columns)
    }
}

impl AnalyzeData {
    pub fn create(ctx: DatabendQueryContextRef) -> AnalyzeData {
        AnalyzeData { ctx, ..Default::default() }
    }

    pub fn get_expr_analyze<const allow_aggr: bool>(&self) -> ExpressionAnalyzer<allow_aggr> {
        let query_context = self.ctx.clone();
        let query_tables_schema = self.tables_schema.clone();
        ExpressionAnalyzer::<allow_aggr>::with_tables(query_context, query_tables_schema)
    }

    pub fn add_aggregate_function(&mut self, expr: &Expression) -> Result<()> {
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

        Ok(())
    }

    pub async fn dummy_source(&mut self) -> Result<()> {
        // We cannot reference field by name, such as `SELECT system.one.dummy`
        let context = self.ctx.as_ref();
        let dummy_table = context.get_table("system", "one")?;
        self.tables_schema.push(TableSchema::UnNamed(dummy_table.schema()));
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
        self.tables_schema.push(TableSchema::UnNamed(table_function.schema()));
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
                let table_schema = TableSchema::Default { database, table, schema };
                self.tables_schema.push(table_schema);
            }
            Some(table_alias) => {
                let alias = table_alias.name.value.clone();
                // let analyzed_schema = AnalyzedSchema::named_table(alias, schema);
                self.tables_schema.push(TableSchema::Named(alias, read_table.schema()));
            }
        }

        Ok(())
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

        if body.cluster_by.is_some() {
            return Err(ErrorCode::SyntaxException("Cluster by is unsupport"));
        }

        if body.sort_by.is_some() {
            return Err(ErrorCode::SyntaxException("Sort by is unsupport"));
        }

        if body.distribute_by.is_some() {
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

