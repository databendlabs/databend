// Copyright 2021 Datafuse Labs
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
use std::time::Instant;

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::InsertSource;
use databend_common_ast::ast::InsertStmt;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Statement;
use databend_common_ast::parser::parse_raw_insert_stmt;
use databend_common_ast::parser::parse_raw_replace_stmt;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::token::Token;
use databend_common_ast::parser::token::TokenKind;
use databend_common_ast::parser::token::Tokenizer;
use databend_common_ast::parser::Dialect;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use derive_visitor::DriveMut;
use log::info;
use log::warn;
use parking_lot::RwLock;

use super::semantic::AggregateRewriter;
use super::semantic::DistinctToGroupBy;
use crate::optimizer::optimize;
use crate::optimizer::OptimizerContext;
use crate::planner::query_executor::QueryExecutor;
use crate::plans::Plan;
use crate::Binder;
use crate::CountSetOps;
use crate::Metadata;
use crate::NameResolutionContext;
use crate::VariableNormalizer;

const PROBE_INSERT_INITIAL_TOKENS: usize = 128;
const PROBE_INSERT_MAX_TOKENS: usize = 128 * 8;

pub struct Planner {
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) query_executor: Option<Arc<dyn QueryExecutor>>,
}

#[derive(Debug, Clone)]
pub struct PlanExtras {
    pub format: Option<String>,
    pub statement: Statement,
}

impl Planner {
    pub fn new(ctx: Arc<dyn TableContext>) -> Self {
        Planner {
            ctx,
            query_executor: None,
        }
    }

    pub fn new_with_query_executor(
        ctx: Arc<dyn TableContext>,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Self {
        Planner {
            ctx,
            query_executor: Some(query_executor),
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn plan_sql(&mut self, sql: &str) -> Result<(Plan, PlanExtras)> {
        let extras = self.parse_sql(sql)?;
        let plan = self.plan_stmt(&extras.statement).await?;
        Ok((plan, extras))
    }

    #[fastrace::trace]
    pub fn parse_sql(&self, sql: &str) -> Result<PlanExtras> {
        let settings = self.ctx.get_settings();
        let sql_dialect = settings.get_sql_dialect()?;
        // compile prql to sql for prql dialect
        let mut prql_converted = false;
        let final_sql: String = match sql_dialect == Dialect::PRQL {
            true => {
                let options = prqlc::Options::default();
                match prqlc::compile(sql, &options) {
                    Ok(res) => {
                        info!("Try convert prql to sql succeed, generated sql: {}", &res);
                        prql_converted = true;
                        res
                    }
                    Err(e) => {
                        warn!(
                            "Try convert prql to sql failed, still use raw sql to parse. error: {}",
                            e.to_string()
                        );
                        sql.to_string()
                    }
                }
            }
            false => sql.to_string(),
        };

        // Step 1: Tokenize the SQL.
        let mut tokenizer = Tokenizer::new(&final_sql).peekable();

        // Only tokenize the beginning tokens for `INSERT INTO` statement because the rest tokens after `VALUES` is unused.
        // Stop the tokenizer on unrecognized token because some values inputs (e.g. CSV) may not be valid for the tokenizer.
        // See also: https://github.com/datafuselabs/databend/issues/6669
        let first_token = tokenizer
            .peek()
            .and_then(|token| Some(token.as_ref().ok()?.kind));
        let is_insert_stmt = matches!(first_token, Some(TokenKind::INSERT)) && {
            let mut tokenizer = Tokenizer::new(&final_sql);
            tokenizer.next_chunk::<3>().is_ok_and(|first_three_tokens| {
                matches!(first_token, Some(TokenKind::INSERT))
                    && !first_three_tokens.iter().any(|token| {
                        matches!(
                            token.as_ref().map(|t| t.kind),
                            Ok(TokenKind::ALL) | Ok(TokenKind::FIRST)
                        )
                    })
            })
        };
        let is_replace_stmt = matches!(first_token, Some(TokenKind::REPLACE));
        let is_insert_or_replace_stmt = is_insert_stmt || is_replace_stmt;
        let mut tokens: Vec<Token> = if is_insert_or_replace_stmt {
            (&mut tokenizer)
                .take(PROBE_INSERT_INITIAL_TOKENS)
                .take_while(|token| token.is_ok())
                // Make sure the tokens stream is always ended with EOI.
                .chain(std::iter::once(Ok(Token::new_eoi(&final_sql))))
                .collect::<databend_common_ast::Result<_>>()
                .unwrap()
        } else {
            (&mut tokenizer).collect::<databend_common_ast::Result<_>>()?
        };

        loop {
            let res = try {
                // Step 2: Parse the SQL.
                let (mut stmt, format) = if is_insert_stmt {
                    (parse_raw_insert_stmt(&tokens, sql_dialect)?, None)
                } else if is_replace_stmt {
                    (parse_raw_replace_stmt(&tokens, sql_dialect)?, None)
                } else {
                    parse_sql(&tokens, sql_dialect)?
                };
                if !matches!(stmt, Statement::SetStmt { .. })
                    && sql_dialect == Dialect::PRQL
                    && !prql_converted
                {
                    return Err(ErrorCode::SyntaxException("convert prql to sql failed."));
                }

                self.replace_stmt(&mut stmt)?;

                PlanExtras {
                    format,
                    statement: stmt,
                }
            };

            let mut maybe_partial_insert = false;

            if is_insert_or_replace_stmt && matches!(tokenizer.peek(), Some(Ok(_))) {
                if let Ok(PlanExtras {
                    statement:
                        Statement::Insert(InsertStmt {
                            source: InsertSource::Select { .. },
                            ..
                        }),
                    ..
                }) = &res
                {
                    maybe_partial_insert = true;
                }
            }

            if (maybe_partial_insert || res.is_err()) && matches!(tokenizer.peek(), Some(Ok(_))) {
                // Remove the EOI.
                tokens.pop();
                // Tokenize more and try again.
                if !maybe_partial_insert && tokens.len() < PROBE_INSERT_MAX_TOKENS {
                    let iter = (&mut tokenizer)
                        .take(tokens.len() * 2)
                        .take_while(|token| token.is_ok())
                        .map(|token| token.unwrap())
                        // Make sure the tokens stream is always ended with EOI.
                        .chain(std::iter::once(Token::new_eoi(&final_sql)));
                    tokens.extend(iter);
                } else {
                    // Take the whole tokenizer
                    let iter = (&mut tokenizer)
                        .take_while(|token| token.is_ok())
                        .map(|token| token.unwrap())
                        // Make sure the tokens stream is always ended with EOI.
                        .chain(std::iter::once(Token::new_eoi(&final_sql)));
                    tokens.extend(iter);
                };
            } else {
                return res;
            }
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn plan_stmt(&mut self, stmt: &Statement) -> Result<Plan> {
        let start = Instant::now();
        let query_kind = get_query_kind(stmt);
        let settings = self.ctx.get_settings();
        // Step 3: Bind AST with catalog, and generate a pure logical SExpr
        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let mut enable_planner_cache = self.ctx.get_settings().get_enable_planner_cache()?;
        let planner_cache_key = if enable_planner_cache {
            Some(Self::planner_cache_key(&stmt.to_string()))
        } else {
            None
        };

        if enable_planner_cache {
            let (c, plan) = self.get_cache(
                name_resolution_ctx.clone(),
                planner_cache_key.as_ref().unwrap(),
                stmt,
            );
            if let Some(plan) = plan {
                info!("logical plan from cache, time used: {:?}", start.elapsed());
                // update for clickhouse handler
                self.ctx.attach_query_str(query_kind, stmt.to_mask_sql());
                return Ok(plan.plan);
            }
            enable_planner_cache = c;
        }

        let metadata = Arc::new(RwLock::new(Metadata::default()));
        let binder = Binder::new(
            self.ctx.clone(),
            CatalogManager::instance(),
            name_resolution_ctx,
            metadata.clone(),
        )
        .with_subquery_executor(self.query_executor.clone());

        // must attach before bind, because ParquetRSTable::create used it.
        self.ctx.attach_query_str(query_kind, stmt.to_mask_sql());
        let plan = binder.bind(stmt).await?;
        // attach again to avoid the query kind is overwritten by the subquery
        self.ctx.attach_query_str(query_kind, stmt.to_mask_sql());

        // Step 4: Optimize the SExpr with optimizers, and generate optimized physical SExpr
        let opt_ctx = OptimizerContext::new(self.ctx.clone(), metadata.clone())
            .with_enable_distributed_optimization(!self.ctx.get_cluster().is_empty())
            .with_enable_join_reorder(unsafe { !settings.get_disable_join_reorder()? })
            .with_enable_dphyp(settings.get_enable_dphyp()?)
            .with_max_push_down_limit(settings.get_max_push_down_limit()?)
            .with_sample_executor(self.query_executor.clone());

        let optimized_plan = optimize(opt_ctx, plan).await?;

        if enable_planner_cache {
            self.set_cache(planner_cache_key.clone().unwrap(), optimized_plan.clone());
        }

        info!("logical plan built, time used: {:?}", start.elapsed());
        Ok(optimized_plan)
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
                    value: Literal::UInt64(max_rows),
                }];
            }
        }
    }

    fn replace_stmt(&self, stmt: &mut Statement) -> Result<()> {
        let name_resolution_ctx =
            NameResolutionContext::try_from(self.ctx.get_settings().as_ref())?;

        let mut variable_normalizer =
            VariableNormalizer::new(&name_resolution_ctx, self.ctx.clone());

        stmt.drive_mut(&mut variable_normalizer);
        variable_normalizer.render_error()?;

        stmt.drive_mut(&mut DistinctToGroupBy::default());
        stmt.drive_mut(&mut AggregateRewriter::default());
        let mut set_ops_counter = CountSetOps::default();
        stmt.drive_mut(&mut set_ops_counter);
        let max_set_ops = self.ctx.get_settings().get_max_set_operator_count()?;
        if max_set_ops < set_ops_counter.count as u64 {
            return Err(ErrorCode::SyntaxException(format!(
                "The number of set operations: {} exceeds the limit: {}",
                set_ops_counter.count, max_set_ops
            )));
        }

        self.add_max_rows_limit(stmt);
        Ok(())
    }
}

pub fn get_query_kind(stmt: &Statement) -> QueryKind {
    match stmt {
        Statement::Query { .. } => QueryKind::Query,
        Statement::StatementWithSettings { stmt, .. } => get_query_kind(stmt),
        Statement::CopyIntoTable(_) => QueryKind::CopyIntoTable,
        Statement::CopyIntoLocation(_) => QueryKind::CopyIntoLocation,
        Statement::Explain { .. } => QueryKind::Explain,
        Statement::Insert(_) => QueryKind::Insert,
        Statement::Replace(_)
        | Statement::Delete(_)
        | Statement::MergeInto(_)
        | Statement::OptimizeTable(_)
        | Statement::Update(_) => QueryKind::Update,
        _ => QueryKind::Other,
    }
}
