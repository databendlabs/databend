use crate::sql::analyzer::{AnalyzableStatement, AnalyzedResult};
use crate::sessions::DatabendQueryContextRef;
use sqlparser::ast::{SqliteOnConflict, ObjectName, Ident, Expr, Query};
use common_exception::{Result, ErrorCode};
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRefExt;
use common_planners::{PlanNode, InsertIntoPlan};
use std::sync::Arc;
use common_infallible::Mutex;
use common_streams::{ValueSource, Source};

pub struct DfInsertStatement {
    or: Option<SqliteOnConflict>,
    /// TABLE
    table_name: ObjectName,
    /// COLUMNS
    columns: Vec<Ident>,
    /// Overwrite (Hive)
    overwrite: bool,
    /// A SQL query that specifies what to insert
    source: Option<Box<Query>>,
    /// partitioned insert (Hive)
    partitioned: Option<Vec<Expr>>,
    /// format name
    format: Option<String>,
    /// Columns defined after PARTITION
    after_columns: Vec<Ident>,
    /// whether the insert has the table keyword (Hive)
    table: bool,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfInsertStatement {
    async fn analyze(self, ctx: DatabendQueryContextRef) -> common_exception::Result<AnalyzedResult> {
        self.is_supported()?;

        let (db, table) = self.resolve_table(ctx)?;
        let table = ctx.get_table(&db, &table)?;
        let mut schema = table.schema();
        let table_meta_id = table.get_id();

        if !self.columns.is_empty() {
            let fields = self.columns
                .iter()
                .map(|ident| schema.field_with_name(&ident.value).map(|v| v.clone()))
                .collect::<Result<Vec<_>>>()?;

            schema = DataSchemaRefExt::create(fields);
        }

        let mut input_stream = futures::stream::iter::<Vec<DataBlock>>(vec![]);

        if let Some(source) = &self.source {
            if let sqlparser::ast::SetExpr::Values(_vs) = &source.body {
                let values = format!("{}", source);
                let block_size = ctx.get_settings().get_max_block_size()? as usize;
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

        let input_stream = Arc::new(Mutex::new(Some(Box::pin(input_stream))));
        Ok(AnalyzedResult::SimpleQuery(
            PlanNode::InsertInto(
                InsertIntoPlan { db_name, tbl_name, tbl_id: table_meta_id, schema, input_stream }
            )
        ))
    }
}

impl DfInsertStatement {
    fn resolve_table(&self, ctx: DatabendQueryContextRef) -> Result<(String, String)> {
        let Self { table_name: ObjectName(idents), .. } = self;
        match idents.len() {
            0 => Err(ErrorCode::SyntaxException("Insert table name is empty")),
            1 => Ok((ctx.get_current_database(), idents[0].value.clone())),
            2 => Ok((idents[0].value.clone(), idents[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException("Insert table name must be [`db`].`table`"))
        }
    }

    fn is_supported(&self) -> Result<()> {
        if self.overwrite {
            return Err(ErrorCode::SyntaxException("Unsupport insert overwrite statement."));
        }

        if self.partitioned {
            return Err(ErrorCode::SyntaxException("Unsupport insert ... partition statement."));
        }

        if self.after_columns {
            return Err(ErrorCode::SyntaxException("Unsupport specify columns after partitions."));
        }

        Ok(())
    }
}
