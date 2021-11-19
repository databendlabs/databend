use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Mutex;
use common_planners::InsertIntoPlan;
use common_planners::PlanNode;
use common_streams::Source;
use common_streams::ValueSource;
use sqlparser::ast::Expr;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::Query;
use sqlparser::ast::SqliteOnConflict;

use crate::sessions::DatabendQueryContext;
use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
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

        let (db, table) = self.resolve_table(&ctx)?;
        let source = ctx.get_table(&db, &table)?;
        let mut schema = source.schema();
        let table_meta_id = source.get_id();

        if !self.columns.is_empty() {
            let fields = self
                .columns
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

        Ok(AnalyzedResult::SimpleQuery(PlanNode::InsertInto(
            InsertIntoPlan {
                db_name: db,
                tbl_name: table,
                tbl_id: table_meta_id,
                schema,
                input_stream: Arc::new(Mutex::new(Some(Box::pin(input_stream)))),
            },
        )))
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
}
