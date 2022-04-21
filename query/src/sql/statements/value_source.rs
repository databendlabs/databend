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

use std::io::Cursor;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use common_planners::Expression;
use sqlparser::ast::Expr;
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
    schema: DataSchemaRef,
    analyzer: ExpressionAnalyzer,
}

impl ValueSource {
    pub fn new(schema: DataSchemaRef, analyzer: ExpressionAnalyzer) -> Self {
        Self { schema, analyzer }
    }

    pub async fn read<'a>(self, reader: &mut CpBufferReader<'a>) -> Result<DataBlock> {
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
                let _ = reader.must_ignore_byte(b',')?;
            }

            self.parse_single_row(reader, col_size, &mut desers).await?;

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

<<<<<<< HEAD
    /// Parse single row value, like ('111', '222', 333)
    async fn parse_single_row<'a>(
        analyzer: ExpressionAnalyzer,
        ctx: Arc<QueryContext>,
    ) -> Result<DataBlock> {
        let values = parse_exprs(bytes, ctx.get_current_session().get_type())?;

        let mut blocks = vec![];
        for value in values {
            let block = exprs_to_datablock(value, &analyzer, &self.schema, ctx.clone()).await?;
            blocks.push(block);
>>>>>>> upstream/main
        }
    }
}

async fn exprs_to_datavalue(
    exprs: Vec<Expr>,
    analyzer: &ExpressionAnalyzer,
    schema: &DataSchemaRef,
    ctx: Arc<QueryContext>,
) -> Result<DataBlock> {
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
        "Insert into from values",
        dummy,
        schema.clone(),
        expressions,
        true,
        ctx,
    )?;
    let res = executor.execute(&one_row_block)?;

    let datavalues: Vec<DataValue> = res.columns().iter().map(|col| col.get(0)).collect();
    Ok(datavalues)
}

fn parse_exprs(buf: &[u8], typ: SessionType) -> std::result::Result<Vec<Vec<Expr>>, ParserError> {
    match typ {
        SessionType::MySQL => {
            let dialect = MySqlDialect {};
            let sql = std::str::from_utf8(buf).unwrap();
            let mut tokenizer = Tokenizer::new(&dialect, sql);
            let (tokens, position_map) = tokenizer.tokenize()?;
            let mut parser = Parser::new(tokens, position_map, &dialect);
            parser.parse_values()
        }
        _ => {
            let dialect = GenericDialect {};
            let sql = std::str::from_utf8(buf).unwrap();
            let mut tokenizer = Tokenizer::new(&dialect, sql);
            let (tokens, position_map) = tokenizer.tokenize()?;
            let mut parser = Parser::new(tokens, position_map, &dialect);
            parser.parse_values()
        }
    }
}
