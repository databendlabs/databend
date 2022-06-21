// Copyright 2022 Datafuse Labs.
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
use std::ops::Not;
use std::sync::Arc;

use common_ast::ast::Expr;
use common_ast::ast::InsertSource;
use common_ast::ast::InsertStmt;
use common_ast::ast::Statement;
use common_ast::parser::error::Backtrace;
use common_ast::parser::parse_comma_separated_exprs;
use common_ast::parser::token::Token;
use common_ast::parser::tokenize_sql;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::TypeDeserializer;
use common_datavalues::TypeDeserializerImpl;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::BufferReader;
use common_io::prelude::CheckpointReader;
use common_io::prelude::*;
use common_planners::Expression;
use common_streams::NDJsonSourceBuilder;
use common_streams::Source;
use common_tracing::tracing;

use crate::formats::FormatFactory;
use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;
use crate::sql::binder::Binder;
use crate::sql::binder::ScalarBinder;
use crate::sql::exec::ExpressionBuilder;
use crate::sql::optimizer::optimize;
use crate::sql::plans::Insert;
use crate::sql::plans::InsertInputSource;
use crate::sql::plans::InsertValueBlock;
use crate::sql::plans::Plan;
use crate::sql::BindContext;
use crate::sql::MetadataRef;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_insert(
        &mut self,
        bind_context: &BindContext,
        stmt: &InsertStmt<'a>,
    ) -> Result<Plan> {
        let catalog_name = match stmt.catalog.clone() {
            Some(catalog) => catalog.name.clone(),
            None => self.ctx.get_current_catalog(),
        };
        let database_name = match stmt.database.clone() {
            Some(database) => database.name.clone(),
            None => self.ctx.get_current_database(),
        };
        let table_name = stmt.table.name.clone();
        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;
        let table_id = table.get_id();

        let schema: DataSchemaRef = match stmt.columns.is_empty() {
            true => table.schema(),
            false => {
                let schema = table.schema();
                let fields = stmt
                    .columns
                    .iter()
                    .map(|ident| schema.field_with_name(&ident.name).map(|v| v.clone()))
                    .collect::<Result<Vec<_>>>()?;
                DataSchemaRefExt::create(fields)
            }
        };

        let input_source: Result<InsertInputSource> = match stmt.source.clone() {
            InsertSource::Streaming {
                format,
                rest_tokens,
            } => {
                let stream_str = self.analyze_streaming_intput(rest_tokens)?;
                self.analyze_stream_format(bind_context, &stream_str, Some(format), schema.clone())
                    .await
            }
            InsertSource::Values { rest_tokens } => {
                let stream_str = self.analyze_streaming_intput(rest_tokens)?;
                self.analyze_stream_format(
                    bind_context,
                    &stream_str,
                    Some("VALUES".to_string()),
                    schema.clone(),
                )
                .await
            }
            InsertSource::Select { query } => {
                let statement = Statement::Query(query);
                let select_plan = self.bind_statement(bind_context, &statement).await?;
                let optimized_plan = optimize(self.ctx.clone(), select_plan)?;
                Ok(InsertInputSource::SelectPlan(Box::new(optimized_plan)))
            }
        };

        let plan = Insert {
            catalog: catalog_name,
            database: database_name,
            table: table_name,
            table_id,
            schema,
            overwrite: stmt.overwrite,
            source: input_source?,
        };

        Ok(Plan::Insert(Box::new(plan)))
    }

    pub(in crate::sql::planner::binder) fn analyze_streaming_intput(
        &self,
        tokens: &[Token],
    ) -> Result<String> {
        let first_token = tokens
            .first()
            .ok_or_else(|| ErrorCode::SyntaxException("Missing token"))?;
        let last_token = tokens
            .last()
            .ok_or_else(|| ErrorCode::SyntaxException("Missing token"))?;
        let source = first_token.source;
        let start = first_token.span.start;
        let end = last_token.span.end;
        Ok(source[start..end].to_string())
    }

    pub(in crate::sql::planner::binder) async fn analyze_stream_format(
        &self,
        bind_context: &BindContext,
        stream_str: &'a str,
        format: Option<String>,
        schema: DataSchemaRef,
    ) -> Result<InsertInputSource> {
        let stream_str = stream_str.trim_start();
        tracing::debug!("{:?}", stream_str);
        let settings = self.ctx.get_format_settings()?;
        // TODO migrate format into format factory
        let format = format.map(|v| v.to_uppercase());
        match format.as_deref() {
            Some("VALUES") | None => {
                let bytes = stream_str.as_bytes();
                let cursor = Cursor::new(bytes);
                let mut reader = CheckpointReader::new(BufferReader::new(cursor));
                let source = ValueSourceV2::new(
                    self.ctx.clone(),
                    bind_context,
                    schema,
                    self.metadata.clone(),
                );
                let block = source.read(&mut reader).await?;
                Ok(InsertInputSource::Values(InsertValueBlock { block }))
            }
            Some("JSONEACHROW") => {
                let builder = NDJsonSourceBuilder::create(schema.clone(), settings);
                let cursor = futures::io::Cursor::new(stream_str.as_bytes());
                let mut source = builder.build(cursor)?;
                let mut blocks = Vec::new();
                while let Some(v) = source.read().await? {
                    blocks.push(v);
                }
                let block = DataBlock::concat_blocks(&blocks)?;
                Ok(InsertInputSource::Values(InsertValueBlock { block }))
            }
            // format factory
            Some(name) => {
                let input_format =
                    FormatFactory::instance().get_input(name, schema.clone(), settings)?;

                let data_slice = stream_str.as_bytes();
                let mut input_state = input_format.create_state();
                let skip_size = input_format.skip_header(data_slice, &mut input_state)?;

                let _ = input_format.read_buf(&data_slice[skip_size..], &mut input_state)?;
                let blocks = input_format.deserialize_data(&mut input_state)?;
                let block = DataBlock::concat_blocks(&blocks)?;
                Ok(InsertInputSource::Values(InsertValueBlock { block }))
            }
        }
    }
}

pub struct ValueSourceV2<'a> {
    ctx: Arc<QueryContext>,
    bind_context: &'a BindContext,
    schema: DataSchemaRef,
    metadata: MetadataRef,
}

impl<'a> ValueSourceV2<'a> {
    pub fn new(
        ctx: Arc<QueryContext>,
        bind_context: &'a BindContext,
        schema: DataSchemaRef,
        metadata: MetadataRef,
    ) -> Self {
        Self {
            ctx,
            schema,
            bind_context,
            metadata,
        }
    }

    pub async fn read<R: BufferRead>(&self, reader: &mut CheckpointReader<R>) -> Result<DataBlock> {
        let mut desers = self
            .schema
            .fields()
            .iter()
            .map(|f| f.data_type().create_deserializer(1024))
            .collect::<Vec<_>>();

        let col_size = desers.len();
        let mut rows = 0;

        loop {
            let _ = reader.ignore_white_spaces()?;
            if !reader.has_data_left()? {
                break;
            }
            // Not the first row
            if rows != 0 {
                reader.must_ignore_byte(b',')?;
            }

            self.parse_next_row(
                reader,
                col_size,
                &mut desers,
                self.bind_context,
                self.metadata.clone(),
            )
            .await?;
            rows += 1;
        }

        if rows == 0 {
            return Ok(DataBlock::empty_with_schema(self.schema.clone()));
        }

        let columns = desers
            .iter_mut()
            .map(|deser| deser.finish_to_column())
            .collect::<Vec<_>>();

        Ok(DataBlock::create(self.schema.clone(), columns))
    }

    /// Parse single row value, like ('111', 222, 1 + 1)
    async fn parse_next_row<R: BufferRead>(
        &self,
        reader: &mut CheckpointReader<R>,
        col_size: usize,
        desers: &mut [TypeDeserializerImpl],
        bind_context: &BindContext,
        metadata: MetadataRef,
    ) -> Result<()> {
        let _ = reader.ignore_white_spaces()?;
        reader.checkpoint();

        // Start of the row --- '('
        if !reader.ignore_byte(b'(')? {
            return Err(ErrorCode::BadDataValueType(
                "Must start with parentheses".to_string(),
            ));
        }

        let format = self.ctx.get_format_settings()?;
        for col_idx in 0..col_size {
            let _ = reader.ignore_white_spaces()?;
            let col_end = if col_idx + 1 == col_size { b')' } else { b',' };

            let deser = desers
                .get_mut(col_idx)
                .ok_or_else(|| ErrorCode::BadBytes("Deserializer is None"))?;

            let (need_fallback, pop_count) = deser
                .de_text_quoted(reader, &format)
                .and_then(|_| {
                    let _ = reader.ignore_white_spaces()?;
                    let need_fallback = reader.ignore_byte(col_end)?.not();
                    Ok((need_fallback, col_idx + 1))
                })
                .unwrap_or((true, col_idx));

            // Deserializer and expr-parser both will eat the end ')' of the row.
            if need_fallback {
                for deser in desers.iter_mut().take(pop_count) {
                    deser.pop_data_value()?;
                }
                skip_to_next_row(reader, 1)?;

                // Parse from expression and append all columns.
                let buf = reader.get_checkpoint_buffer();
                // let exprs = parse_exprs(buf, session_type)?;

                let sql = std::str::from_utf8(buf).unwrap();
                let tokens = tokenize_sql(sql)?;
                let backtrace = Backtrace::new();
                let exprs =
                    parse_comma_separated_exprs(&tokens[1..tokens.len() as usize], &backtrace)?;

                let values = exprs_to_datavalue(
                    exprs,
                    &self.schema,
                    self.ctx.clone(),
                    bind_context,
                    metadata,
                )
                .await?;
                reader.reset_checkpoint();

                for (append_idx, deser) in desers.iter_mut().enumerate().take(col_size) {
                    deser.append_data_value(values[append_idx].clone(), &format)?;
                }

                return Ok(());
            }
        }

        Ok(())
    }
}

// Values |(xxx), (yyy), (zzz)
pub fn skip_to_next_row<R: BufferRead>(
    reader: &mut CheckpointReader<R>,
    mut balance: i32,
) -> Result<()> {
    let _ = reader.ignore_white_spaces()?;

    let mut quoted = false;

    while balance > 0 {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            break;
        }

        let size = buffer.len();

        let it = buffer
            .iter()
            .position(|&c| c == b'(' || c == b')' || c == b'\\' || c == b'\'');

        if let Some(it) = it {
            let c = buffer[it];
            reader.consume(it + 1);

            match c {
                b'\\' => {
                    continue;
                }
                b'\'' => {
                    quoted ^= true;
                    continue;
                }
                b')' => {
                    if !quoted {
                        balance -= 1;
                    }
                }
                b'(' => {
                    if !quoted {
                        balance += 1;
                    }
                }
                _ => {}
            }
        } else {
            reader.consume(size);
        }
    }
    Ok(())
}

async fn exprs_to_datavalue<'a>(
    exprs: Vec<Expr<'a>>,
    schema: &DataSchemaRef,
    ctx: Arc<QueryContext>,
    bind_context: &BindContext,
    metadata: MetadataRef,
) -> Result<Vec<DataValue>> {
    if exprs.len() != schema.num_fields() {
        return Err(ErrorCode::BadDataValueType(
            "Expression size not match schema num of cols".to_string(),
        ));
    }
    let mut expressions = Vec::with_capacity(exprs.len());
    for (i, expr) in exprs.iter().enumerate() {
        let mut scalar_binder = ScalarBinder::new(bind_context, ctx.clone(), metadata.clone());
        let scalar = scalar_binder.bind(expr).await?.0;
        let expression_builder = ExpressionBuilder::create(metadata.clone());
        let expr = expression_builder.build(&scalar)?;
        let expr = if &expr.to_data_type(schema)? != schema.field(i).data_type() {
            Expression::Cast {
                expr: Box::new(expr),
                data_type: schema.field(i).data_type().clone(),
                pg_style: false,
            }
        } else {
            expr
        };
        expressions.push(Expression::Alias(
            schema.field(i).name().to_string(),
            Box::new(expr),
        ));
    }

    let dummy = DataSchemaRefExt::create(vec![DataField::new("dummy", u8::to_data_type())]);
    let one_row_block = DataBlock::create(dummy.clone(), vec![Series::from_data(vec![1u8])]);
    let executor = ExpressionExecutor::try_create(
        ctx,
        "Insert into from values",
        dummy,
        schema.clone(),
        expressions,
        true,
    )?;

    let res = executor.execute(&one_row_block)?;
    let datavalues: Vec<DataValue> = res.columns().iter().map(|col| col.get(0)).collect();
    Ok(datavalues)
}
