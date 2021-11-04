use crate::sql::analyzer::{AnalyzableStatement, AnalyzedResult};
use sqlparser::ast::{Query, SetExpr, Select, TableWithJoins, TableFactor, ObjectName, TableAlias, FunctionArg, Expr};
use crate::sessions::{DatabendQueryContextRef, DatabendQueryContext};
use common_exception::{Result, ErrorCode};
use crate::catalogs::ToReadDataSourcePlan;
use std::sync::Arc;
use common_planners::{PlanNode, ReadDataSourcePlan, Expression};
use common_datavalues::{DataSchema, DataSchemaRef};
use common_arrow::arrow_format::ipc::flatbuffers::bitflags::_core::future::Future;
use crate::sql::statements::statement_common::{TableSchema, ExpressionAnalyzer};

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
    pub before_having_expressions: Vec<Expression>,
    pub having_predicate: Option<Expression>,
    pub order_by_expressions: Vec<Expression>,
    pub projection_expressions: Vec<Expression>,

    tables_schema: Vec<TableSchema>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for Box<Query> {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        if self.with.is_some() {
            return Result::Err(ErrorCode::UnImplement("CTE is not yet implement"));
        }

        let mut data = AnalyzeData::create(ctx);

        match &self.body {
            SetExpr::Select(query) => SelectAnalyzer::analyze(query, &mut data).await,
            other => Result::Err(ErrorCode::UnImplement(
                format!("Query {} is not yet implemented", other)
            ))
        }
    }
}

struct SelectAnalyzer {
    res: SelectQueryAnalyzedResult,
}

impl SelectAnalyzer {
    pub async fn analyze(query: &Select, data: &mut AnalyzeData) -> Result<AnalyzedResult> {
        if let Err(cause) = SelectAnalyzer::analyze_from(&query.from, data).await {
            return Err(cause.add_message_back("(while in analyze select from)."));
        }

        if let Err(cause) = SelectAnalyzer::analyze_filter(&query.selection, data).await {
            return Err(cause.add_message_back("(while in analyze select filter)."));
        }


        Ok(AnalyzedResult::SelectQuery(data))
    }

    pub async fn analyze_from(expr: &[TableWithJoins], data: &mut AnalyzeData) -> Result<()> {
        match expr.len() {
            0 => data.dummy_source().await,
            1 => Self::analyze_join(&expr[0], data).await,
            // It's not `JOIN` clause. Such as SELECT * FROM t1, t2;
            _ => Result::Err(ErrorCode::SyntaxException("Cannot SELECT multiple tables")),
        }
    }

    pub async fn analyze_filter(expr: &Option<Expr>, data: &mut AnalyzeData) -> Result<()> {
        match expr {
            None => Ok(()),
            Some(expr) => data.set_filter(expr).await
        }
    }

    async fn analyze_join(table: &TableWithJoins, data: &mut AnalyzeData) -> Result<()> {
        let TableWithJoins { relation, joins } = table;

        match joins.is_empty() {
            true => Err(ErrorCode::UnImplement("Cannot SELECT join.")),
            false => SelectAnalyzer::analyze_table_factor(data, relation).await
        }
    }

    async fn analyze_table_factor(data: &mut AnalyzeData, relation: &TableFactor) -> Result<()> {
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

                // TODO:
                // match subquery.analyze(data.ctx.clone()).await? {
                //     AnalyzedResult::SelectQuery(new_data) => *data = new_data,
                //     _ => Err()
                // }
            },
            TableFactor::NestedJoin(nested) => Self::analyze_join(nested, data),
            TableFactor::TableFunction { .. } => Err(ErrorCode::UnImplement("Unsupported table function")),
        }
    }
}

impl AnalyzeData {
    pub fn create(ctx: DatabendQueryContextRef) -> AnalyzeData {
        AnalyzeData { ctx, ..Default::default() }
    }

    pub async fn set_filter(&mut self, expr: &Expr) -> Result<()> {
        // TODO: collect pushdown predicates
        let ctx = self.ctx.clone();
        let tables_schema = self.tables_schema.clone();
        let expression_analyzer = ExpressionAnalyzer::with_tables(ctx, tables_schema);
        self.filter_predicate = Some(expression_analyzer.analyze(expr)?);
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
                let table_schema = TableSchema::Default { database, table, schema: schema };
                self.tables_schema.push(table_schema);
            }
            Some(table_alias) => {
                let alias = table_alias.name.value.clone();
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

