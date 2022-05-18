//  Copyright 2022 Datafuse Labs.
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
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::validate_expression;
use common_planners::Expression;
use sqlparser::ast::Expr;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;
use sqlparser::tokenizer::Tokenizer;

use crate::sessions::QueryContext;
use crate::sql::statements::ExpressionAnalyzer;
use crate::storages::fuse::table_functions::string_value;
use crate::table_functions::TableArgs;

pub fn parse_func_table_args(table_args: &TableArgs) -> Result<(String, String)> {
    match table_args {
        // Todo(zhyass): support 3 arguments.
        Some(args) if args.len() == 2 => {
            let db = string_value(&args[0])?;
            let tbl = string_value(&args[1])?;
            Ok((db, tbl))
        }
        _ => Err(ErrorCode::BadArguments(format!(
            "expecting database and table name (as two string literals), but got {:?}",
            table_args
        ))),
    }
}

pub async fn get_cluster_keys(
    ctx: Arc<QueryContext>,
    schema: DataSchemaRef,
    definition: &str,
) -> Result<Vec<Expression>> {
    let exprs = parse_cluster_keys(definition)?;

    let mut expressions = vec![];
    let expression_analyzer = ExpressionAnalyzer::create(ctx);
    for expr in exprs.iter() {
        let expression = expression_analyzer.analyze(expr).await?;
        validate_expression(&expression, &schema)?;
        expressions.push(expression);
    }
    Ok(expressions)
}

fn parse_cluster_keys(definition: &str) -> Result<Vec<Expr>> {
    let dialect = &GenericDialect {};
    let mut tokenizer = Tokenizer::new(dialect, definition);
    match tokenizer.tokenize() {
        Ok((tokens, position_map)) => {
            let mut parser = Parser::new(tokens, position_map, dialect);
            parser.expect_token(&Token::LParen)?;
            let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
            parser.expect_token(&Token::RParen)?;
            Ok(exprs)
        }
        Err(tokenize_error) => Err(ErrorCode::SyntaxException(format!(
            "Can not tokenize definition: {}, Error: {:?}",
            definition, tokenize_error
        ))),
    }
}
