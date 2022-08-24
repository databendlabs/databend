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
use common_ast::parser::parse_comma_separated_exprs;
use common_ast::parser::token::Token;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::TypeDeserializer;
use common_datavalues::TypeDeserializerImpl;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::FormatFactory;
use common_io::prelude::BufferReader;
use common_io::prelude::*;
use common_pipeline_transforms::processors::transforms::Transform;
use common_streams::NDJsonSourceBuilder;
use common_streams::Source;
use tracing::debug;

use crate::evaluator::Evaluator;
use crate::pipelines::processors::transforms::ExpressionTransformV2;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::binder::Binder;
use crate::sql::binder::ScalarBinder;
use crate::sql::normalize_identifier;
use crate::sql::optimizer::optimize;
use crate::sql::optimizer::OptimizerConfig;
use crate::sql::optimizer::OptimizerContext;
use crate::sql::planner::semantic::NameResolutionContext;
use crate::sql::plans::CastExpr;
use crate::sql::plans::ConstantExpr;
use crate::sql::plans::Insert;
use crate::sql::plans::InsertInputSource;
use crate::sql::plans::InsertValueBlock;
use crate::sql::plans::Plan;
use crate::sql::plans::Scalar;
use crate::sql::BindContext;
use crate::sql::MetadataRef;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_insert(
        &mut self,
        bind_context: &BindContext,
        stmt: &InsertStmt<'a>,
    ) -> Result<Plan> {
        let InsertStmt {
            catalog,
            database,
            table,
            columns,
            source,
            overwrite,
        } = stmt;
        let catalog_name = catalog.as_ref().map_or_else(
            || self.ctx.get_current_catalog(),
            |ident| normalize_identifier(ident, &self.name_resolution_ctx).name,
        );
        let database_name = database.as_ref().map_or_else(
            || self.ctx.get_current_database(),
            |ident| normalize_identifier(ident, &self.name_resolution_ctx).name,
        );
        let table_name = normalize_identifier(table, &self.name_resolution_ctx).name;
        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;
        let table_id = table.get_id();

        let schema = if columns.is_empty() {
            table.schema()
        } else {
            let schema = table.schema();
            let fields = columns
                .iter()
                .map(|ident| {
                    schema
                        .field_with_name(
                            &normalize_identifier(ident, &self.name_resolution_ctx).name,
                        )
                        .map(|v| v.clone())
                })
                .collect::<Result<Vec<_>>>()?;
            DataSchemaRefExt::create(fields)
        };

        let input_source: Result<InsertInputSource> = match source.clone() {
            InsertSource::Streaming {
                format,
                rest_tokens,
            } => {
                let stream_str = self.analyze_streaming_input(rest_tokens)?;
                self.analyze_stream_format(bind_context, &stream_str, Some(format), schema.clone())
                    .await
            }
            InsertSource::Values { rest_tokens } => {
                let stream_str = self.analyze_streaming_input(rest_tokens)?;
                let str = stream_str.trim_end_matches(';');
                self.analyze_stream_format(
                    bind_context,
                    str,
                    Some("VALUES".to_string()),
                    schema.clone(),
                )
                .await
            }
            InsertSource::Select { query } => {
                let statement = Statement::Query(query);
                let select_plan = self.bind_statement(bind_context, &statement).await?;
                // Don't enable distributed optimization for `INSERT INTO ... SELECT ...` for now
                let opt_ctx = Arc::new(OptimizerContext::new(OptimizerConfig::default()));
                let optimized_plan = optimize(self.ctx.clone(), opt_ctx, select_plan)?;
                Ok(InsertInputSource::SelectPlan(Box::new(optimized_plan)))
            }
        };

        let plan = Insert {
            catalog: catalog_name,
            database: database_name,
            table: table_name,
            table_id,
            schema,
            overwrite: *overwrite,
            source: input_source?,
        };

        Ok(Plan::Insert(Box::new(plan)))
    }

    pub(in crate::sql::planner::binder) fn analyze_streaming_input(
        &self,
        tokens: &[Token],
    ) -> Result<String> {
        if tokens.is_empty() {
            return Ok("".to_string());
        }
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
        debug!("{:?}", stream_str);
        let settings = self.ctx.get_format_settings()?;
        // TODO migrate format into format factory
        let format = format.map(|v| v.to_uppercase());
        match format.as_deref() {
            Some("VALUES") | None => {
                let bytes = stream_str.as_bytes();
                let cursor = Cursor::new(bytes);
                let mut reader = NestedCheckpointReader::new(BufferReader::new(cursor));
                let source = ValueSourceV2::new(
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
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
                let skip_size = input_format.skip_header(data_slice, &mut input_state, 0)?;

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
    name_resolution_ctx: &'a NameResolutionContext,
    bind_context: &'a BindContext,
    schema: DataSchemaRef,
    metadata: MetadataRef,
}

impl<'a> ValueSourceV2<'a> {
    pub fn new(
        ctx: Arc<QueryContext>,
        name_resolution_ctx: &'a NameResolutionContext,
        bind_context: &'a BindContext,
        schema: DataSchemaRef,
        metadata: MetadataRef,
    ) -> Self {
        Self {
            ctx,
            name_resolution_ctx,
            schema,
            bind_context,
            metadata,
        }
    }

    pub async fn read<R: BufferRead>(
        &self,
        reader: &mut NestedCheckpointReader<R>,
    ) -> Result<DataBlock> {
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
        reader: &mut NestedCheckpointReader<R>,
        col_size: usize,
        desers: &mut [TypeDeserializerImpl],
        bind_context: &BindContext,
        metadata: MetadataRef,
    ) -> Result<()> {
        let _ = reader.ignore_white_spaces()?;
        reader.push_checkpoint();

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

                let sql = std::str::from_utf8(buf).unwrap();
                let settings = self.ctx.get_settings();
                let sql_dialect = settings.get_sql_dialect()?;
                let tokens = tokenize_sql(sql)?;
                let backtrace = Backtrace::new();
                let exprs = parse_comma_separated_exprs(
                    &tokens[1..tokens.len() as usize],
                    sql_dialect,
                    &backtrace,
                )?;

                let values = exprs_to_datavalue(
                    exprs,
                    &self.schema,
                    self.ctx.clone(),
                    self.name_resolution_ctx,
                    bind_context,
                    metadata,
                )
                .await?;

                reader.pop_checkpoint();

                for (append_idx, deser) in desers.iter_mut().enumerate().take(col_size) {
                    deser.append_data_value(values[append_idx].clone(), &format)?;
                }

                return Ok(());
            }
        }

        reader.pop_checkpoint();
        Ok(())
    }
}

// Values |(xxx), (yyy), (zzz)
pub fn skip_to_next_row<R: BufferRead>(
    reader: &mut NestedCheckpointReader<R>,
    mut balance: i32,
) -> Result<()> {
    let _ = reader.ignore_white_spaces()?;

    let mut quoted = false;
    let mut escaped = false;

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

            if it == 0 && escaped {
                escaped = false;
                continue;
            }
            escaped = false;

            match c {
                b'\\' => {
                    escaped = true;
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
            escaped = false;
            reader.consume(size);
        }
    }
    Ok(())
}

async fn exprs_to_datavalue<'a>(
    exprs: Vec<Expr<'a>>,
    schema: &DataSchemaRef,
    ctx: Arc<QueryContext>,
    name_resolution_ctx: &NameResolutionContext,
    bind_context: &BindContext,
    metadata: MetadataRef,
) -> Result<Vec<DataValue>> {
    let schema_fields_len = schema.fields().len();
    let mut expressions = Vec::with_capacity(schema_fields_len);
    for (i, expr) in exprs.iter().enumerate() {
        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            ctx.clone(),
            name_resolution_ctx,
            metadata.clone(),
            &[],
        );
        let (mut scalar, data_type) = scalar_binder.bind(expr).await?;
        let field_data_type = schema.field(i).data_type();
        if data_type.ne(field_data_type) {
            scalar = Scalar::CastExpr(CastExpr {
                argument: Box::new(scalar),
                from_type: Box::new(data_type),
                target_type: Box::new(field_data_type.clone()),
            })
        }
        expressions.push((
            Evaluator::eval_scalar(&scalar)?,
            schema.field(i).name().to_string(),
        ));
    }
    if exprs.len() < schema_fields_len {
        for idx in exprs.len()..schema_fields_len {
            let field = schema.field(idx);
            if let Some(default_expr) = field.default_expr() {
                expressions.push((
                    Evaluator::eval_physical_scalar(&serde_json::from_str(default_expr)?)?,
                    schema.field(idx).name().to_string(),
                ));
            } else {
                // If field data type is nullable, then we'll fill it with null.
                if field.data_type().is_nullable() {
                    let scalar = Scalar::ConstantExpr(ConstantExpr {
                        value: DataValue::Null,
                        data_type: Box::new(field.data_type().clone()),
                    });
                    expressions.push((
                        Evaluator::eval_scalar(&scalar)?,
                        schema.field(idx).name().to_string(),
                    ));
                } else {
                    return Err(ErrorCode::LogicalError(format!(
                        "null value in column {} violates not-null constraint",
                        schema.field(idx).name()
                    )));
                }
            }
        }
    }

    let dummy = DataSchemaRefExt::create(vec![DataField::new("dummy", u8::to_data_type())]);
    let one_row_block = DataBlock::create(dummy, vec![Series::from_data(vec![1u8])]);
    let func_ctx = ctx.try_get_function_context()?;
    let mut expression_transform = ExpressionTransformV2 {
        expressions,
        func_ctx,
    };
    let res = expression_transform.transform(one_row_block)?;
    let datavalues: Vec<DataValue> = res.columns().iter().skip(1).map(|col| col.get(0)).collect();
    Ok(datavalues)
}
