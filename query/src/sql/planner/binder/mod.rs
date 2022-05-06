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

pub use bind_context::BindContext;
pub use bind_context::ColumnBinding;
use common_ast::ast::Statement;
use common_exception::Result;

use crate::catalogs::Catalog;
use crate::sessions::QueryContext;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::metadata::Metadata;
use crate::storages::Table;

mod aggregate;
mod bind_context;
mod project;
mod scalar;
mod scalar_common;
mod scalar_visitor;
mod select;

/// Binder is responsible to transform AST of a query into a canonical logical SExpr.
///
/// During this phase, it will:
/// - Resolve columns and tables with Catalog
/// - Check semantic of query
/// - Validate expressions
/// - Build `Metadata`
pub struct Binder {
    ctx: Arc<QueryContext>,
    catalog: Arc<dyn Catalog>,
    metadata: Metadata,
}

impl Binder {
    pub fn new(ctx: Arc<QueryContext>, catalog: Arc<dyn Catalog>) -> Self {
        Binder {
            ctx,
            catalog,
            metadata: Metadata::create(),
        }
    }

    pub async fn bind<'a>(mut self, stmt: &Statement<'a>) -> Result<BindResult> {
        let init_bind_context = BindContext::create();
        let bind_context = self.bind_statement(stmt, &init_bind_context).await?;
        Ok(BindResult::create(bind_context, self.metadata))
    }

    async fn bind_statement<'a>(
        &mut self,
        stmt: &Statement<'a>,
        bind_context: &BindContext,
    ) -> Result<BindContext> {
        match stmt {
            Statement::Query(query) => {
                let bind_context = self.bind_query(query, bind_context).await?;
                Ok(bind_context)
            }
            _ => todo!(),
        }
    }

    async fn resolve_data_source(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        // Resolve table with catalog
        let table_meta = self.catalog.get_table(tenant, database, table).await?;
        Ok(table_meta)
    }
}

pub struct BindResult {
    pub bind_context: BindContext,
    pub metadata: Metadata,
}

impl BindResult {
    pub fn create(bind_context: BindContext, metadata: Metadata) -> Self {
        BindResult {
            bind_context,
            metadata,
        }
    }

    pub fn s_expr(&self) -> &SExpr {
        self.bind_context.expression.as_ref().unwrap()
    }
}
