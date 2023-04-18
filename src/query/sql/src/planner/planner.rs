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

use std::sync::Arc;

use common_ast::ast::Expr;
use common_ast::ast::Literal;
use common_ast::ast::Statement;
use common_ast::parser::parse_sql;
use common_ast::parser::token::HintPrefix;
use common_ast::parser::token::HintSuffix;
use common_ast::parser::token::Token;
use common_ast::parser::token::TokenKind;
use common_ast::parser::token::Tokenizer;
use common_ast::walk_statement_mut;
use common_ast::Dialect;
use common_catalog::catalog::CatalogManager;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use parking_lot::RwLock;

use super::semantic::AggregateRewriter;
use super::semantic::DistinctToGroupBy;
use crate::optimizer::optimize;
use crate::optimizer::OptimizerConfig;
use crate::optimizer::OptimizerContext;
use crate::plans::Plan;
use crate::Binder;
use crate::Metadata;
use crate::MetadataRef;
use crate::NameResolutionContext;

const PROBE_INSERT_INITIAL_TOKENS: usize = 128;
const PROBE_INSERT_MAX_TOKENS: usize = 128 * 8;

pub struct Planner {
    ctx: Arc<dyn TableContext>,
}

#[derive(Debug, Clone)]
pub struct PlanExtras {
    pub metadata: MetadataRef,
    pub format: Option<String>,
    pub statement: Statement,
}

impl Planner {
    pub fn new(ctx: Arc<dyn TableContext>) -> Self {
        Planner { ctx }
    }

    #[async_backtrace::framed]
    pub async fn plan_sql(&mut self, sql: &str) -> Result<(Plan, PlanExtras)> {
        let settings = self.ctx.get_settings();
        let sql_dialect = settings.get_sql_dialect()?;

        // Step 1: Tokenize the SQL.
        let mut tokenizer = Tokenizer::new(sql).peekable();

        // Only tokenize the beginning tokens for `INSERT INTO` statement because the tokens of values is unused.
        //
        // Stop the tokenizer on unrecognized token because some values inputs (e.g. CSV) may not be valid for the tokenizer.
        // See also: https://github.com/datafuselabs/databend/issues/6669
        let is_insert_stmt = tokenizer
            .peek()
            .and_then(|token| Some(token.as_ref().ok()?.kind))
            == Some(TokenKind::INSERT);
        let tokens: Vec<Token> = if is_insert_stmt {
            (&mut tokenizer)
                .take(PROBE_INSERT_INITIAL_TOKENS)
                .take_while(|token| token.is_ok())
                // Make sure the tokens stream is always ended with EOI.
                .chain(std::iter::once(Ok(Token::new_eoi(sql))))
                .collect::<Result<_>>()
                .unwrap()
        } else {
            (&mut tokenizer).collect::<Result<_>>()?
        };

        let mut tokens = Self::preparse_opt_hints(&tokens);

        loop {
            let res = async {
                // Step 2: Parse the SQL.
                let (mut stmt, format) = parse_sql(&tokens, sql_dialect)?;
                self.replace_stmt(&mut stmt, sql_dialect);

                // Step 3: Bind AST with catalog, and generate a pure logical SExpr
                let metadata = Arc::new(RwLock::new(Metadata::default()));
                let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
                let binder = Binder::new(
                    self.ctx.clone(),
                    CatalogManager::instance(),
                    name_resolution_ctx,
                    metadata.clone(),
                );
                let plan = binder.bind(&stmt).await?;

                // Step 4: Optimize the SExpr with optimizers, and generate optimized physical SExpr
                let opt_ctx = Arc::new(OptimizerContext::new(OptimizerConfig {
                    enable_distributed_optimization: !self.ctx.get_cluster().is_empty(),
                }));

                let optimized_plan = optimize(self.ctx.clone(), opt_ctx, plan)?;
                Ok((optimized_plan, PlanExtras {
                    metadata,
                    format,
                    statement: stmt,
                }))
            }
            .await;

            if res.is_err() && matches!(tokenizer.peek(), Some(Ok(_))) {
                // Remove the previous EOI.
                tokens.pop();
                // Tokenize more and try again.
                if tokens.len() < PROBE_INSERT_MAX_TOKENS {
                    let iter = (&mut tokenizer)
                        .take(tokens.len() * 2)
                        .take_while(|token| token.is_ok())
                        .chain(std::iter::once(Ok(Token::new_eoi(sql))))
                        .map(|token| token.unwrap())
                        // Make sure the tokens stream is always ended with EOI.
                        .chain(std::iter::once(Token::new_eoi(sql)));
                    tokens.extend(iter);
                } else {
                    let iter = (&mut tokenizer)
                        .take_while(|token| token.is_ok())
                        .map(|token| token.unwrap())
                        // Make sure the tokens stream is always ended with EOI.
                        .chain(std::iter::once(Token::new_eoi(sql)));
                    tokens.extend(iter);
                };
            } else {
                return res;
            }
        }
    }

    fn preparse_opt_hints<'a>(tokens: &'a [Token<'a>]) -> Vec<Token<'a>> {
        let mut prefix_idx = 0;
        let mut matched_prefix = 0;
        let mut matched_suffix = 0;
        let mut pre_suf_vec: Vec<(usize, usize)> = Vec::new();
        for (i, token) in tokens.iter().enumerate() {
            // The hint syntax must follow immediately after an INSERT, UPDATE, DELETE, SELECT, or REPLACE keyword
            // that begins the statement block.
            if i != 0
                && (tokens[i - 1].kind == TokenKind::INSERT
                    || tokens[i - 1].kind == TokenKind::SELECT
                    || tokens[i - 1].kind == TokenKind::UPDATE
                    || tokens[i - 1].kind == TokenKind::DELETE
                    || tokens[i - 1].kind == TokenKind::REPLACE)
                && token.kind == HintPrefix
            {
                continue;
            }
            if token.kind == HintPrefix {
                prefix_idx = i;
                matched_prefix += 1;
            } else if token.kind == HintSuffix && matched_prefix == (matched_suffix + 1) {
                matched_suffix += 1;
                pre_suf_vec.push((prefix_idx, i));
            }
        }
        if !pre_suf_vec.is_empty() {
            let mut new_tokens: Vec<Token> = Vec::new();
            // tokens:     [1,2,3,4,5,6,7]
            // pre_suf_vec:  [1,2] [4,5]
            // new_tokens: [1,4,7]
            for (i, (prefix, suffix)) in pre_suf_vec.iter().enumerate() {
                let (prefix, suffix) = (*prefix, *suffix);
                if i < matched_suffix {
                    let mut start = 0;
                    if i != 0 {
                        let (_, last_suffix) = pre_suf_vec[i - 1];
                        start = last_suffix + 1;
                    }
                    new_tokens.extend_from_slice(&tokens[start..prefix]);
                    if i == matched_suffix - 1 {
                        new_tokens.extend_from_slice(&tokens[suffix + 1..]);
                    }
                }
            }
            new_tokens
        } else {
            tokens.to_vec()
        }
    }

    fn add_max_rows_limit(&self, statement: &mut Statement) {
        let max_rows = self.ctx.get_settings().get_max_result_rows().unwrap();
        if max_rows == 0 {
            return;
        }

        if let Statement::Query(query) = statement {
            if query.limit.is_empty() {
                query.limit = vec![Expr::Literal {
                    span: None,
                    lit: Literal::UInt64(max_rows),
                }];
            }
        }
    }

    fn replace_stmt(&self, stmt: &mut Statement, sql_dialect: Dialect) {
        walk_statement_mut(&mut DistinctToGroupBy::default(), stmt);
        walk_statement_mut(&mut AggregateRewriter { sql_dialect }, stmt);

        self.add_max_rows_limit(stmt);
    }
}
