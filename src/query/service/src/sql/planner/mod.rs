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
use common_ast::parser::tokenize_sql;
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
pub use metadata::find_smallest_column;
pub use metadata::ColumnEntry;
pub use metadata::Metadata;
pub use metadata::MetadataRef;
pub use metadata::TableEntry;
pub use semantic::normalize_identifier;
pub use semantic::IdentifierNormalizer;
pub use semantic::NameResolutionContext;

use self::plans::Plan;
use super::optimizer::OptimizerConfig;
use super::optimizer::OptimizerContext;
use crate::sessions::TableContext;

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

        // Step 1: parse SQL text into AST
        let tokens = tokenize_sql(sql)?;
        let backtrace = Backtrace::new();
        let (stmt, format) = parse_sql(&tokens, sql_dialect, &backtrace)?;

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

        Ok((optimized_plan, metadata.clone(), format))
    }
}
