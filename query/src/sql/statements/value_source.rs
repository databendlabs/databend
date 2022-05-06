//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::ops::Not;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::BacktraceGuard;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use common_planners::Expression;
use sqlparser::ast::Expr;
use sqlparser::dialect::Dialect;
use sqlparser::dialect::GenericDialect;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::Tokenizer;

use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;
use crate::sessions::SessionType;
use crate::sql::statements::ExpressionAnalyzer;

pub struct ValueSource {
    ctx: Arc<QueryContext>,
    schema: DataSchemaRef,
    analyzer: ExpressionAnalyzer,
    backtrace_guard: BacktraceGuard,
}

impl ValueSource {
    pub fn new(ctx: Arc<QueryContext>, schema: DataSchemaRef) -> Self {
        Self {
            schema,
            ctx: ctx.clone(),
            analyzer: ExpressionAnalyzer::create(ctx),
            backtrace_guard: BacktraceGuard::new(true),
        }
    }

    pub async fn read<'a>(&self, reader: &mut CpBufferReader<'a>) -> Result<DataBlock> {
        let mut desers = self
            .schema
            .fields()
            .iter()
            .map(|f| f.data_type().create_deserializer(1024))
            .collect::<Vec<_>>();

        let col_size = desers.len();
        let mut rows = 0;
        let session_type = self.ctx.get_current_session().get_type();

        loop {
            let _ = reader.ignore_white_spaces()?;
            if !reader.has_data_left()? {
                break;
            }
            // Not the first row
            if rows != 0 {
                let _ = reader.must_ignore_byte(b',')?;
            }

            self.parse_next_row(reader, col_size, &mut desers, &session_type)
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
    async fn parse_next_row<'a>(
        &self,
        reader: &mut CpBufferReader<'a>,
        col_size: usize,
        desers: &mut [TypeDeserializerImpl],
        session_type: &SessionType,
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

            // Disable backtrace here.
            self.backtrace_guard.disable();
            let (need_fallback, pop_count) = deser
                .de_text_quoted(reader, &format)
                .and_then(|_| {
                    let _ = reader.ignore_white_spaces()?;
                    let need_fallback = reader.ignore_byte(col_end)?.not();
                    Ok((need_fallback, col_idx + 1))
                })
                .unwrap_or((true, col_idx));
            // Enable backtrace again.
            self.backtrace_guard.enable();

            // Deserializer and expr-parser both will eat the end ')' of the row.
            if need_fallback {
                for deser in desers.iter_mut().take(pop_count) {
                    deser.pop_data_value()?;
                }
                skip_to_next_row(reader, 1)?;

                // Parse from expression and append all columns.
                let buf = reader.get_checkpoint_buffer();
                let exprs = parse_exprs(buf, session_type)?;
                reader.reset_checkpoint();

                let values =
                    exprs_to_datavalue(exprs, &self.analyzer, &self.schema, self.ctx.clone())
                        .await?;

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
pub fn skip_to_next_row(reader: &mut CpBufferReader, mut balance: i32) -> Result<()> {
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

async fn exprs_to_datavalue(
    exprs: Vec<Expr>,
    analyzer: &ExpressionAnalyzer,
    schema: &DataSchemaRef,
    ctx: Arc<QueryContext>,
) -> Result<Vec<DataValue>> {
    let mut expressions = Vec::with_capacity(exprs.len());
    for (i, expr) in exprs.iter().enumerate() {
        let expr = analyzer.analyze(expr).await?;
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

fn parse_exprs(buf: &[u8], typ: &SessionType) -> std::result::Result<Vec<Expr>, ParserError> {
    let dialect: &dyn Dialect = match typ {
        SessionType::MySQL => &MySqlDialect {},
        _ => &GenericDialect {},
    };
    let sql = std::str::from_utf8(buf).unwrap();
    let mut tokenizer = Tokenizer::new(dialect, sql);
    let (tokens, position_map) = tokenizer.tokenize()?;
    let mut parser = Parser::new(tokens, position_map, dialect);

    parser.expect_token(&Token::LParen)?;
    let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
    parser.expect_token(&Token::RParen)?;
    Ok(exprs)
}
