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

use crate::catalogs::CatalogManager;
use crate::sessions::QueryContext;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::metadata::Metadata;
use crate::storages::Table;

mod aggregate;
mod bind_context;
mod join;
mod limit;
mod project;
mod scalar;
mod scalar_common;
mod scalar_visitor;
mod select;
mod sort;

/// Binder is responsible to transform AST of a query into a canonical logical SExpr.
///
/// During this phase, it will:
/// - Resolve columns and tables with Catalog
/// - Check semantic of query
/// - Validate expressions
/// - Build `Metadata`
pub struct Binder {
    ctx: Arc<QueryContext>,
    catalogs: Arc<CatalogManager>,
    metadata: Metadata,
}

impl<'a> Binder {
    pub fn new(ctx: Arc<QueryContext>, catalogs: Arc<CatalogManager>) -> Self {
        Binder {
            ctx,
            catalogs,
            metadata: Metadata::create(),
        }
    }

    pub async fn bind(mut self, stmt: &Statement<'a>) -> Result<BindResult> {
        let init_bind_context = BindContext::new();
        let (s_expr, bind_context) = self.bind_statement(stmt, &init_bind_context).await?;
        Ok(BindResult::create(s_expr, bind_context, self.metadata))
    }

    async fn bind_statement(
        &mut self,
        stmt: &Statement<'a>,
        bind_context: &BindContext,
    ) -> Result<(SExpr, BindContext)> {
        match stmt {
            Statement::Query(query) => self.bind_query(query, bind_context).await,
            _ => todo!(),
        }
    }

    async fn resolve_data_source(
        &self,
        tenant: &str,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        // Resolve table with catalog
        let catalog = self.catalogs.get_catalog(catalog_name)?;
        let table_meta = catalog.get_table(tenant, database_name, table_name).await?;
        Ok(table_meta)
    }
}

pub struct BindResult {
    pub s_expr: SExpr,
    pub bind_context: BindContext,
    pub metadata: Metadata,
}

impl BindResult {
    pub fn create(s_expr: SExpr, bind_context: BindContext, metadata: Metadata) -> Self {
        BindResult {
            s_expr,
            bind_context,
            metadata,
        }
    }
}
