use sqlparser::ast::{ObjectName, FunctionArg, Query, TableAlias, JoinOperator, TableWithJoins, TableFactor, Ident};
use common_exception::ErrorCode;
use common_exception::Result;
use crate::sessions::{DatabendQueryContextRef, DatabendQueryContext};
use common_planners::Expression;
use std::collections::HashMap;
use crate::sql::statements::analyzer_schema::AnalyzeQuerySchema;
use crate::sql::statements::analyzer_expr::ExpressionAnalyzer;
use crate::sql::statements::{DfQueryStatement, AnalyzableStatement, QueryRelation};
use std::convert::TryFrom;

pub struct AnalyzeQueryState {
    pub ctx: DatabendQueryContextRef,

    pub relation: Option<Box<QueryRelation>>,
    pub filter_predicate: Option<Expression>,
    pub before_group_by_expressions: Vec<Expression>,
    pub group_by_expressions: Vec<Expression>,
    pub aggregate_expressions: Vec<Expression>,
    pub before_having_expressions: Vec<Expression>,
    pub having_predicate: Option<Expression>,
    pub order_by_expressions: Vec<Expression>,
    pub projection_expressions: Vec<Expression>,

    pub from_schema: AnalyzeQuerySchema,
    pub before_aggr_schema: AnalyzeQuerySchema,
    pub after_aggr_schema: AnalyzeQuerySchema,
    // finalize_schema: DataSchemaRef,
    pub projection_aliases: HashMap<String, Expression>,
}

impl AnalyzeQueryState {
    pub async fn create(ctx: DatabendQueryContextRef, tables: &[TableWithJoins]) -> Result<Self> {
        let mut analyzed_tables = Vec::new();

        // Build RPN for tables. because async function unsupported recursion
        let rpn = RelationRPNBuilder::build(tables)?;
        for rpn_item in &rpn {
            match rpn_item {
                RelationRPNItem::Join(_) => {}
                RelationRPNItem::Table(v) => {
                    let schema = Self::analyze_table(ctx.clone(), v);
                    analyzed_tables.push(schema.await?);
                }
                RelationRPNItem::TableFunction(v) => {
                    let schema = Self::analyze_table_function(ctx.clone(), v);
                    analyzed_tables.push(schema.await?);
                }
                RelationRPNItem::Derived(v) => {
                    let subquery = &(*v.subquery);
                    let subquery = DfQueryStatement::try_from(subquery.clone())?;
                    let res = subquery.analyze(ctx.clone()).await?;
                }
            }
        }

        unimplemented!()
    }

    async fn analyze_table(ctx: DatabendQueryContextRef, item: &TableRPNItem) -> Result<AnalyzeQuerySchema> {
        // TODO(Winter): await query_context.get_table
        let (database, table) = Self::resolve_table(&item.name, &ctx)?;
        let read_table = ctx.get_table(&database, &table)?;

        match &item.alias {
            None => {
                let schema = read_table.schema();
                let name_prefix = vec![database, table];
                AnalyzeQuerySchema::from_schema(schema, name_prefix)
            }
            Some(table_alias) => {
                let schema = read_table.schema();
                let name_prefix = vec![table_alias.name.value.clone()];
                AnalyzeQuerySchema::from_schema(schema, name_prefix)
            }
        }
    }

    async fn analyze_table_function(ctx: DatabendQueryContextRef, item: &TableFunctionRPNItem) -> Result<AnalyzeQuerySchema> {
        if item.name.0.len() >= 2 {
            return Result::Err(ErrorCode::BadArguments(
                "Currently table can't have arguments",
            ));
        }

        let table_name = item.name.0[0].value.clone();
        let mut table_args = Vec::with_capacity(item.args.len());

        let analyzer_ctx = ctx.clone();
        let analyzer_schema = AnalyzeQuerySchema::none();
        let analyzer = ExpressionAnalyzer::with_source(analyzer_ctx, analyzer_schema, false);

        for table_arg in &item.args {
            table_args.push(match table_arg {
                FunctionArg::Named { arg, .. } => analyzer.analyze(arg).await?,
                FunctionArg::Unnamed(arg) => analyzer.analyze(arg).await?,
            });
        }

        let table_function = ctx.get_table_function(&table_name, Some(table_args))?;
        AnalyzeQuerySchema::from_schema(table_function.schema(), Vec::new())
    }

    fn resolve_table(name: &ObjectName, ctx: &DatabendQueryContext) -> Result<(String, String)> {
        match name.0.len() {
            0 => Err(ErrorCode::SyntaxException("Table name is empty")),
            1 => Ok((ctx.get_current_database(), name.0[0].value.clone())),
            2 => Ok((name.0[0].value.clone(), name.0[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException("Table name must be [`db`].`table`"))
        }
    }
}

struct TableRPNItem {
    name: ObjectName,
    alias: Option<TableAlias>,
}

struct DerivedRPNItem {
    subquery: Box<Query>,
    alias: Option<TableAlias>,
}

struct TableFunctionRPNItem {
    name: ObjectName,
    args: Vec<FunctionArg>,
}

enum RelationRPNItem {
    Table(TableRPNItem),
    TableFunction(TableFunctionRPNItem),
    Derived(DerivedRPNItem),
    Join(JoinOperator),
}

struct RelationRPNBuilder {
    rpn: Vec<RelationRPNItem>,
}

impl RelationRPNBuilder {
    pub fn build(exprs: &[TableWithJoins]) -> Result<Vec<RelationRPNItem>> {
        let mut builder = RelationRPNBuilder { rpn: Vec::new() };
        match exprs.is_empty() {
            true => builder.visit_dummy_table(),
            false => builder.visit(exprs)?
        }

        Ok(builder.rpn)
    }

    fn visit_dummy_table(&mut self) {
        self.rpn.push(RelationRPNItem::Table(TableRPNItem {
            name: ObjectName(vec![Ident::new("system"), Ident::new("one")]),
            alias: None,
        }));
    }

    fn visit(&mut self, exprs: &[TableWithJoins]) -> Result<()> {
        for expr in exprs {
            match self.rpn.is_empty() {
                true => { self.visit_joins(expr)?; }
                false => {
                    self.visit_joins(expr)?;
                    self.rpn.push(RelationRPNItem::Join(JoinOperator::CrossJoin));
                }
            }
        }

        Ok(())
    }

    fn visit_joins(&mut self, expr: &TableWithJoins) -> Result<()> {
        self.visit_table_factor(&expr.relation)?;

        for join in &expr.joins {
            self.visit_table_factor(&join.relation)?;
            self.rpn.push(RelationRPNItem::Join(join.join_operator.clone()));
        }

        Ok(())
    }

    fn visit_table_factor(&mut self, factor: &TableFactor) -> Result<()> {
        match factor {
            TableFactor::Table { name, args, alias, with_hints } => {
                if !with_hints.is_empty() {
                    return Err(ErrorCode::SyntaxException("MSSQL-specific `WITH (...)` hints is unsupported."));
                }

                match args.is_empty() {
                    true => self.visit_table(name, alias),
                    false if alias.is_none() => self.visit_table_function(name, args),
                    false => Err(ErrorCode::SyntaxException("Table function cannot named.")),
                }
            }
            TableFactor::Derived { lateral, subquery, alias } => {
                if *lateral {
                    return Err(ErrorCode::UnImplement("Cannot SELECT LATERAL subquery."));
                }

                self.rpn.push(RelationRPNItem::Derived(DerivedRPNItem { subquery: subquery.clone(), alias: alias.clone() }));
                Ok(())
            }
            TableFactor::NestedJoin(joins) => self.visit_joins(&joins),
            TableFactor::TableFunction { .. } => Err(ErrorCode::UnImplement("Unsupported table function")),
        }
    }

    fn visit_table(&mut self, name: &ObjectName, alias: &Option<TableAlias>) -> Result<()> {
        self.rpn.push(RelationRPNItem::Table(TableRPNItem { name: name.clone(), alias: alias.clone() }));
        Ok(())
    }

    fn visit_table_function(&mut self, name: &ObjectName, args: &[FunctionArg]) -> Result<()> {
        self.rpn.push(RelationRPNItem::TableFunction(TableFunctionRPNItem {
            name: name.clone(),
            args: args.to_owned(),
        }));
        Ok(())
    }
}