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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use common_planners::Expression;
use sqlparser::ast::Expr;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::Tokenizer;

use crate::pipelines::transforms::ExpressionExecutor;
use crate::sql::statements::ExpressionAnalyzer;

pub struct ValueSource {
    schema: DataSchemaRef,
    analyzer: ExpressionAnalyzer,
}

impl ValueSource {
    pub fn new(schema: DataSchemaRef, analyzer: ExpressionAnalyzer) -> Self {
        Self { schema, analyzer }
    }

    pub async fn read<'a>(self, reader: &mut CpBufferReader<'a>) -> Result<DataBlock> {
        let mut buf = Vec::new();
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
            // not the first row
            if rows != 0 {
                reader.until(b',', &mut buf)?;
            }
            let _ = reader.ignore_white_spaces()?;
            reader.checkpoint();

            let mut datavalues: Option<Vec<DataValue>> = None;

            if !reader.ignore_byte(b'(')? {
                return Err(ErrorCode::BadDataValueType(
                    "Must start with parentheses".to_string(),
                ));
            }

            for (col, deser) in desers.iter_mut().enumerate().take(col_size) {
                if let Some(values) = &datavalues {
                    deser.append_data_value(values[col].clone())?;
                    continue;
                }
                let _ = reader.ignore_white_spaces()?;
                if col > 0 {
                    reader.must_ignore_byte(b',')?;
                    let _ = reader.ignore_white_spaces()?;
                }

                if deser.de_text_quoted(reader).is_err() {
                    skip_to_next_row(reader, 1)?;
                    // parse from expression
                    // set datavalues
                    let buf = reader.get_checkpoint_buffer();
                    let exprs = parse_exprs(buf)?;
                    reader.reset_checkpoint();

                    let values = exprs_to_datavalue(exprs, &self.analyzer, &self.schema).await?;
                    deser.append_data_value(values[col].clone())?;
                    datavalues = Some(values);
                } else {
                    // Check ')' for last colomn
                    if col + 1 == col_size {
                        let _ = reader.ignore_white_spaces()?;
                        reader.must_ignore_byte(b')')?;
                    }
                }
            }

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
}

// values |(xxx), (yyy), (zzz)
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
) -> Result<Vec<DataValue>> {
    let mut expressions = Vec::with_capacity(exprs.len());
    for (i, expr) in exprs.iter().enumerate() {
        let expr = analyzer.analyze(expr).await?;
        let expr = if &expr.to_data_type(schema)? != schema.field(i).data_type() {
            Expression::Cast {
                expr: Box::new(expr),
                data_type: schema.field(i).data_type().clone(),
                is_nullable: schema.field(i).is_nullable(),
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

fn parse_exprs(buf: &[u8]) -> std::result::Result<Vec<Expr>, ParserError> {
    println!("buf -> {:?}", String::from_utf8_lossy(buf).to_string());

    let dialect = GenericDialect {};
    let sql = std::str::from_utf8(buf).unwrap();
    let mut tokenizer = Tokenizer::new(&dialect, sql);
    let (tokens, pos_map) = tokenizer.tokenize()?;
    let mut parser = Parser::new(tokens, pos_map, &dialect);

    parser.expect_token(&Token::LParen)?;
    let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
    parser.expect_token(&Token::RParen)?;
    Ok(exprs)
}
