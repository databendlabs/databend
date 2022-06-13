// Copyright 2021 Datafuse Labs.
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

use std::io::Cursor;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::BufferReader;
use common_io::prelude::CheckpointReader;
use common_planners::InsertInputSource;
use common_planners::InsertPlan;
use common_planners::InsertValueBlock;
use common_planners::PlanNode;
use common_streams::NDJsonSourceBuilder;
use common_streams::Source;
use common_tracing::tracing;
use sqlparser::ast::Expr;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::OnInsert;
use sqlparser::ast::Query;
use sqlparser::ast::SqliteOnConflict;

use crate::formats::FormatFactory;
use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfQueryStatement;
use crate::sql::statements::ValueSource;
use crate::sql::DfStatement;
use crate::sql::PlanParser;
use crate::storages::Table;

#[derive(Debug, Clone, PartialEq)]
pub struct DfInsertStatement<'a> {
    pub or: Option<SqliteOnConflict>,
    /// TABLE
    pub object_name: ObjectName,
    /// COLUMNS
    pub columns: Vec<Ident>,
    /// Overwrite (Hive)
    pub overwrite: bool,
    /// A SQL query that specifies what to insert
    pub source: InsertSource<'a>,
    /// partitioned insert (Hive)
    pub partitioned: Option<Vec<Expr>>,
    /// format name
    pub format: Option<String>,
    /// Columns defined after PARTITION
    pub after_columns: Vec<Ident>,
    /// whether the insert has the table keyword (Hive)
    pub table: bool,
    /// on duplicate key update
    pub on: Option<OnInsert>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource<'a> {
    /// for insert format
    Empty,
    Select(Box<Query>),
    StreamFormat(&'a str),
}

#[async_trait::async_trait]
impl<'a> AnalyzableStatement for DfInsertStatement<'a> {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        self.is_supported()?;

        let (catalog_name, database_name, table_name) = self.resolve_table(&ctx)?;
        let write_table = ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;
        let table_id = write_table.get_id();
        let schema = self.insert_schema(write_table)?;

        let input_source = match &self.source {
            InsertSource::Empty => self.analyze_insert_without_source().await,
            InsertSource::StreamFormat(stream_format) => {
                self.analyze_stream_format(ctx.clone(), *stream_format, &schema, self.format.clone())
                    .await
            }
            InsertSource::Select(select) => self.analyze_insert_select(ctx.clone(), select).await,
        }?;

        Ok(AnalyzedResult::SimpleQuery(Box::new(PlanNode::Insert(
            InsertPlan {
                catalog_name,
                database_name,
                table_name,
                table_id,
                schema,
                overwrite: self.overwrite,
                source: input_source,
            },
        ))))
    }
}

impl<'a> DfInsertStatement<'a> {
    fn resolve_table(&self, ctx: &QueryContext) -> Result<(String, String, String)> {
        let parts = &self.object_name.0;
        match parts.len() {
            0 => Err(ErrorCode::SyntaxException("Insert table name is empty")),
            1 => Ok((
                ctx.get_current_catalog(),
                ctx.get_current_database(),
                parts[0].value.clone(),
            )),
            2 => Ok((
                ctx.get_current_catalog(),
                parts[0].value.clone(),
                parts[1].value.clone(),
            )),
            3 => Ok((
                parts[0].value.clone(),
                parts[1].value.clone(),
                parts[2].value.clone(),
            )),
            _ => Err(ErrorCode::SyntaxException(
                "Insert table name must be [`catalog`].[`db`].`table`",
            )),
        }
    }

    fn is_supported(&self) -> Result<()> {
        if self.partitioned.is_some() {
            return Err(ErrorCode::SyntaxException(
                "Unsupported insert ... partition statement.",
            ));
        }

        if !self.after_columns.is_empty() {
            return Err(ErrorCode::SyntaxException(
                "Unsupported specify columns after partitions.",
            ));
        }

        Ok(())
    }

    async fn analyze_stream_format(
        &self,
        ctx: Arc<QueryContext>,
        stream_str: &'a str,
        schema: &DataSchemaRef,
        format: Option<String>,
    ) -> Result<InsertInputSource> {
        let stream_str = stream_str.trim_start();
        tracing::debug!("{:?}", stream_str);
        let settings = ctx.get_format_settings()?;
        // TODO migrate format into format factory
        let format = format.map(|v| v.to_uppercase());
        match format.as_ref().map(|v| v.as_str()) {
            Some("VALUES") | None =>  {
                    let bytes = stream_str.as_bytes();
                    let cursor = Cursor::new(bytes);
                    let mut reader = CheckpointReader::new(BufferReader::new(cursor));
                    let source = ValueSource::new(ctx.clone(), schema.clone());
                    let block = source.read(&mut reader).await?;
                    Ok(InsertInputSource::Values(InsertValueBlock { block }))
            },
            Some("JSONEACHROW") => {
                let builder = NDJsonSourceBuilder::create(schema.clone(), settings);
                let cursor = futures::io::Cursor::new(
                    stream_str.as_bytes()
                );
                let mut source = builder.build(cursor)?;
                let mut blocks = Vec::new();
                loop {
                    match source.read().await? {
                        Some(v) => blocks.push(v),
                        None => break,
                    }
                }
                let block = DataBlock::concat_blocks(&blocks)?;
                Ok(InsertInputSource::Values(InsertValueBlock { block }))
            },
            
            // format factory
            Some(name) => {
                let input_format =
                    FormatFactory::instance().get_input(name, schema.clone(), settings)?;
                
                let data_slice = stream_str.as_bytes();
                let mut input_state = input_format.create_state();
                let skip_size = input_format
                        .skip_header(data_slice, &mut input_state)?;
                        
                let _ = input_format
                        .read_buf(&data_slice[skip_size..], &mut input_state)?;
                let blocks = input_format.deserialize_data(&mut input_state)?;
                let block = DataBlock::concat_blocks(&blocks)?;
                Ok(InsertInputSource::Values(InsertValueBlock { block }))
            }
            
        }
    }

    async fn analyze_insert_without_source(&self) -> Result<InsertInputSource> {
        let format = self.format.as_ref().ok_or_else(|| {
            ErrorCode::SyntaxException("FORMAT must be specified in streaming insertion")
        })?;
        Ok(InsertInputSource::StreamingWithFormat(format.clone()))
    }

    async fn analyze_insert_select(
        &self,
        ctx: Arc<QueryContext>,
        source: &Query,
    ) -> Result<InsertInputSource> {
        let statement = DfQueryStatement::try_from(source.clone())?;
        let select_plan =
            PlanParser::build_plan(vec![DfStatement::Query(Box::new(statement))], ctx).await?;
        Ok(InsertInputSource::SelectPlan(Box::new(select_plan)))
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
