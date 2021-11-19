use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::{DataSchemaRef, DataSchemaRefExt};
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Mutex;
use common_planners::InsertIntoPlan;
use common_planners::PlanNode;
use common_streams::Source;
use common_streams::ValueSource;
use sqlparser::ast::{Expr, SetExpr};
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::Query;
use sqlparser::ast::SqliteOnConflict;
use common_tracing::tracing;
use crate::catalogs::Table;

use crate::sessions::DatabendQueryContext;
use crate::sessions::DatabendQueryContextRef;
use crate::sql::{DfStatement, PlanParser};
use crate::sql::statements::{AnalyzableStatement, DfQueryStatement};
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfInsertStatement {
    pub or: Option<SqliteOnConflict>,
    /// TABLE
    pub table_name: ObjectName,
    /// COLUMNS
    pub columns: Vec<Ident>,
    /// Overwrite (Hive)
    pub overwrite: bool,
    /// A SQL query that specifies what to insert
    pub source: Option<Box<Query>>,
    /// partitioned insert (Hive)
    pub partitioned: Option<Vec<Expr>>,
    /// format name
    pub format: Option<String>,
    /// Columns defined after PARTITION
    pub after_columns: Vec<Ident>,
    /// whether the insert has the table keyword (Hive)
    pub table: bool,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfInsertStatement {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        self.is_supported()?;

        match &self.source {
            None => Err(ErrorCode::SyntaxException("Insert must be have values or select.")),
            Some(source) => match source.body {
                SetExpr::Values(_) => self.analyze_insert_values(&ctx, source),
                SetExpr::Select(_) => self.analyze_insert_select(&ctx, source).await,
                _ => Err(ErrorCode::SyntaxException("Insert must be have values or select.")),
            }
        }
    }
}

impl DfInsertStatement {
    fn resolve_table(&self, ctx: &DatabendQueryContext) -> Result<(String, String)> {
        let Self {
            table_name: ObjectName(idents),
            ..
        } = self;
        match idents.len() {
            0 => Err(ErrorCode::SyntaxException("Insert table name is empty")),
            1 => Ok((ctx.get_current_database(), idents[0].value.clone())),
            2 => Ok((idents[0].value.clone(), idents[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException(
                "Insert table name must be [`db`].`table`",
            )),
        }
    }

    fn is_supported(&self) -> Result<()> {
        if self.overwrite {
            return Err(ErrorCode::SyntaxException(
                "Unsupport insert overwrite statement.",
            ));
        }

        if self.partitioned.is_some() {
            return Err(ErrorCode::SyntaxException(
                "Unsupport insert ... partition statement.",
            ));
        }

        if !self.after_columns.is_empty() {
            return Err(ErrorCode::SyntaxException(
                "Unsupport specify columns after partitions.",
            ));
        }

        Ok(())
    }

    fn analyze_insert_values(&self, ctx: &DatabendQueryContext, source: &Query) -> Result<AnalyzedResult> {
        tracing::debug!("{:?}", source);

        let (db, table) = self.resolve_table(&ctx)?;
        let write_table = ctx.get_table(&db, &table)?;
        let table_meta_id = write_table.get_id();
        let schema = self.insert_schema(write_table)?;

        let values = format!("{}", source);
        Ok(AnalyzedResult::SimpleQuery(PlanNode::InsertInto(
            InsertIntoPlan::insert_values(db, table, table_meta_id, schema, values)
        )))
    }

    async fn analyze_insert_select(&self, ctx: &DatabendQueryContextRef, source: &Query) -> Result<AnalyzedResult> {
        let (db, table) = self.resolve_table(&ctx)?;
        let write_table = ctx.get_table(&db, &table)?;
        let table_meta_id = write_table.get_id();
        let table_schema = self.insert_schema(write_table)?;

        let statement = DfQueryStatement::try_from(source.clone())?;
        let select_plan = PlanParser::build_plan(vec![DfStatement::Query(statement)], ctx.clone()).await?;
        Ok(AnalyzedResult::SimpleQuery(PlanNode::InsertInto(
            InsertIntoPlan::insert_select(db, table, table_meta_id, table_schema, select_plan)
        )))
    }

    fn insert_schema(&self, read_table: Arc<dyn Table>) -> Result<DataSchemaRef> {
        match self.columns.is_empty() {
            true => Ok(read_table.schema()),
            false => {
                let schema = read_table.schema();
                let fields = self
                    .columns
                    .iter()
                    .map(|ident| schema.field_with_name(&ident.value).map(|v| v.clone()))
                    .collect::<Result<Vec<_>>>()?;

                Ok(DataSchemaRefExt::create(fields))
            }
        }
    }
}
