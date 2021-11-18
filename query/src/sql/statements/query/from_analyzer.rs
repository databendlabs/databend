use std::fs::read;
use sqlparser::ast::{FunctionArg, Ident, JoinOperator, ObjectName, Query, TableAlias, TableFactor, TableWithJoins};
use crate::sessions::DatabendQueryContextRef;
use common_exception::{ErrorCode, Result};
use crate::sql::statements::query::query_schema::AnalyzeQuerySchema;
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult, DfQueryStatement};
use crate::sql::statements::analyzer_expr::ExpressionAnalyzer;

pub struct FromAnalyzer {
    ctx: DatabendQueryContextRef,
}

impl FromAnalyzer {
    pub fn create(ctx: DatabendQueryContextRef) -> FromAnalyzer {
        FromAnalyzer { ctx }
    }

    pub async fn analyze(&self, query: &DfQueryStatement) -> Result<AnalyzeQuerySchema> {
        let mut analyzed_tables = Vec::new();

        // Build RPN for tables. because async function unsupported recursion
        let rpn = RelationRPNBuilder::build(&query.from)?;
        for rpn_item in &rpn {
            match rpn_item {
                RelationRPNItem::Join(_) => {
                    return Err(ErrorCode::UnImplement("Unimplemented SELECT JOIN yet."));
                }
                RelationRPNItem::Table(v) => {
                    let schema = self.table(v);
                    analyzed_tables.push(schema.await?);
                }
                RelationRPNItem::TableFunction(v) => {
                    let schema = self.table_function(v);
                    analyzed_tables.push(schema.await?);
                }
                RelationRPNItem::Derived(v) => {
                    let schema = self.subquery(v);
                    analyzed_tables.push(schema.await?);
                }
            }
        }

        if analyzed_tables.len() != 1 {
            return Err(ErrorCode::LogicalError("Logical error: this is relation rpn bug."));
        }

        Ok(analyzed_tables.remove(0))
    }

    async fn subquery(&self, v: &DerivedRPNItem) -> Result<AnalyzeQuerySchema> {
        let subquery = &(*v.subquery);
        let subquery = DfQueryStatement::try_from(subquery.clone())?;
        match subquery.analyze(self.ctx.clone()).await? {
            AnalyzedResult::SelectQuery(state) => {
                match &v.alias {
                    None => {
                        AnalyzeQuerySchema::from_subquery(state, Vec::new())
                    }
                    Some(alias) => {
                        let name_prefix = vec![alias.name.value.clone()];
                        AnalyzeQuerySchema::from_subquery(state, name_prefix)
                    }
                }
            }
            _ => Err(ErrorCode::LogicalError("Logical error, subquery analyzed data must be SelectQuery, it's a bug.")),
        }
    }

    async fn table(&self, item: &TableRPNItem) -> Result<AnalyzeQuerySchema> {
        // TODO(Winter): await query_context.get_table
        let (database, table) = self.resolve_table(&item.name)?;
        let read_table = self.ctx.get_table(&database, &table)?;

        match &item.alias {
            None => {
                let name_prefix = vec![database, table];
                AnalyzeQuerySchema::from_table(read_table, name_prefix)
            }
            Some(table_alias) => {
                let name_prefix = vec![table_alias.name.value.clone()];
                AnalyzeQuerySchema::from_table(read_table, name_prefix)
            }
        }
    }

    async fn table_function(&self, item: &TableFunctionRPNItem) -> Result<AnalyzeQuerySchema> {
        if item.name.0.len() >= 2 {
            return Result::Err(ErrorCode::BadArguments(
                "Currently table can't have arguments",
            ));
        }

        let table_name = item.name.0[0].value.clone();
        let mut table_args = Vec::with_capacity(item.args.len());
        let analyzer = ExpressionAnalyzer::create(self.ctx.clone());

        for table_arg in &item.args {
            table_args.push(match table_arg {
                FunctionArg::Named { arg, .. } => analyzer.analyze(arg).await?,
                FunctionArg::Unnamed(arg) => analyzer.analyze(arg).await?,
            });
        }

        let table_function = self.ctx.get_table_function(&table_name, Some(table_args))?;
        AnalyzeQuerySchema::from_table(table_function.as_table(), Vec::new())
    }

    fn resolve_table(&self, name: &ObjectName) -> Result<(String, String)> {
        match name.0.len() {
            0 => Err(ErrorCode::SyntaxException("Table name is empty")),
            1 => Ok((self.ctx.get_current_database(), name.0[0].value.clone())),
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

