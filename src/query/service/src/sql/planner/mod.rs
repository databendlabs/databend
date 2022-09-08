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

use common_ast::parser::parse_sql;
use common_ast::parser::token::Token;
use common_ast::parser::token::TokenKind;
use common_ast::parser::token::Tokenizer;
use common_ast::Backtrace;
use common_exception::Result;
use parking_lot::RwLock;
pub use plans::ScalarExpr;

use crate::clusters::ClusterHelper;
use crate::sessions::QueryContext;
use crate::sql::optimizer::optimize;
pub use crate::sql::planner::binder::BindContext;

pub(crate) mod binder;
mod format;
mod metadata;
pub mod plans;
mod semantic;

pub use binder::Binder;
pub use binder::ColumnBinding;
pub use binder::Visibility;
pub use metadata::find_smallest_column;
pub use metadata::ColumnEntry;
pub use metadata::Metadata;
pub use metadata::MetadataRef;
pub use metadata::TableEntry;
pub use metadata::DUMMY_TABLE_INDEX;
pub use semantic::normalize_identifier;
pub use semantic::IdentifierNormalizer;
pub use semantic::NameResolutionContext;

use self::plans::Plan;
use super::optimizer::OptimizerConfig;
use super::optimizer::OptimizerContext;
use crate::sessions::TableContext;

const PROBE_INSERT_INITIAL_TOKENS: usize = 128;
const PROBE_INSERT_MAX_TOKENS: usize = 128 * 8;

pub struct Planner {
    ctx: Arc<QueryContext>,
}

impl Planner {
    pub fn new(ctx: Arc<QueryContext>) -> Self {
        Planner { ctx }
    }

    pub async fn plan_sql(&mut self, sql: &str) -> Result<(Plan, MetadataRef, Option<String>)> {
        let settings = self.ctx.get_settings();
        let sql_dialect = settings.get_sql_dialect()?;

        // Step 1: Tokenize the SQL.
        let mut tokenizer = Tokenizer::new(sql).peekable();
        let is_insert_stmt = tokenizer
            .peek()
            .and_then(|token| Some(token.as_ref().ok()?.kind))
            == Some(TokenKind::INSERT);
        // Only tokenize the beginning tokens for `INSERT INTO` statement because it's unnecessary to tokenize tokens for values.
        // 
        // Stop the tokenizer on unrecognized token because some values inputs (e.g. CSV) may not be recognized by the tokenizer.
        // See also: https://github.com/datafuselabs/databend/issues/6669
        let mut tokens: Vec<Token> = if is_insert_stmt {
            (&mut tokenizer)
                .take(PROBE_INSERT_INITIAL_TOKENS)
                .take_while(|token| token.is_ok())
                .collect::<Result<_>>().unwrap()
        } else {
            (&mut tokenizer).collect::<Result<_>>()?
        };

        loop {
            let res = async {
                // Step 2: Parse the SQL.
                let backtrace = Backtrace::new();
                let (stmt, format) = parse_sql(&tokens, sql_dialect, &backtrace)?;

                // Step 3: Bind AST with catalog, and generate a pure logical SExpr
                let metadata = Arc::new(RwLock::new(Metadata::create()));
                let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
                let binder = Binder::new(
                    self.ctx.clone(),
                    self.ctx.get_catalog_manager()?,
                    name_resolution_ctx,
                    metadata.clone(),
                );
                let plan = binder.bind(&stmt).await?;

                // Step 4: Optimize the SExpr with optimizers, and generate optimized physical SExpr
                let opt_ctx = Arc::new(OptimizerContext::new(OptimizerConfig {
                    enable_distributed_optimization: !self.ctx.get_cluster().is_empty(),
                }));
                let optimized_plan = optimize(self.ctx.clone(), opt_ctx, plan)?;

                Ok((optimized_plan, metadata.clone(), format))
            }
            .await;

            if res.is_err()
                && tokens.len() < PROBE_INSERT_MAX_TOKENS
                && matches!(tokenizer.peek(), Some(Ok(_)))
            {
                // Tokenize more and try again.
                for token in (&mut tokenizer).take(tokens.len() * 2).take_while(|token| token.is_ok()) {
                    tokens.push(token.unwrap());
                }
            } else {
                return res;
            }
        }
    }
}
