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

use common_ast::ast::InsertSource;
use common_ast::ast::InsertStmt;
use common_ast::ast::Statement;
use common_ast::parser::parse_sql;
use common_ast::parser::token::Token;
use common_ast::parser::token::TokenKind;
use common_ast::parser::token::Tokenizer;
use common_ast::Backtrace;
use common_exception::Result;
use nom::Offset;
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

static PROBE_INSERT_MAX_TOKENS: usize = 128;

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

        // Step 1: Tokenize the beginning of the SQL, so as to find `INSERT INTO` statement without
        // tokenizing all SQL.
        let mut tokenizer = Tokenizer::new(sql);
        let mut tokens: Vec<Token> = (&mut tokenizer)
            .take(PROBE_INSERT_MAX_TOKENS)
            .collect::<Result<_>>()?;

        let mut try_fast_parse_insert =
            tokens.first().map(|token| token.kind) == Some(TokenKind::INSERT);

        loop {
            let res: Result<(Plan, MetadataRef, Option<String>)> = try {
                let backtrace = Backtrace::new();
                let (stmt, format) = if try_fast_parse_insert {
                    let (mut stmt, format) = parse_sql(&tokens, sql_dialect, &backtrace)?;
                    // Extend rest_str to the end of the SQL.
                    if let Statement::Insert(InsertStmt {
                        source:
                            InsertSource::Streaming { rest_str, .. } | InsertSource::Values { rest_str },
                        ..
                    }) = &mut stmt
                    {
                        *rest_str = &sql[sql.offset(rest_str)..];
                    }
                    (stmt, format)
                } else {
                    // Fall back to tokenize all SQL.
                    for token in &mut tokenizer {
                        tokens.push(token?);
                    }
                    parse_sql(&tokens, sql_dialect, &backtrace)?
                };

                // Step 2: bind AST with catalog, and generate a pure logical SExpr
                let metadata = Arc::new(RwLock::new(Metadata::create()));
                let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
                let binder = Binder::new(
                    self.ctx.clone(),
                    self.ctx.get_catalog_manager()?,
                    name_resolution_ctx,
                    metadata.clone(),
                );
                let plan = binder.bind(&stmt).await?;

                // Step 3: optimize the SExpr with optimizers, and generate optimized physical SExpr
                let opt_ctx = Arc::new(OptimizerContext::new(OptimizerConfig {
                    enable_distributed_optimization: !self.ctx.get_cluster().is_empty(),
                }));
                let optimized_plan = optimize(self.ctx.clone(), opt_ctx, plan)?;

                (optimized_plan, metadata.clone(), format)
            };

            if try_fast_parse_insert && res.is_err() {
                try_fast_parse_insert = false;
            } else {
                return res;
            }
        }
    }
}
