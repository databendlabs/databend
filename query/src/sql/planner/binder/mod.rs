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

pub use aggregate::AggregateInfo;
pub use bind_context::BindContext;
pub use bind_context::ColumnBinding;
use common_ast::ast::Statement;
use common_ast::ast::TimeTravelPoint;
use common_datavalues::DataTypeImpl;
use common_exception::ErrorCode;
use common_exception::Result;

use self::subquery::SubqueryRewriter;
use super::plans::Plan;
use crate::catalogs::CatalogManager;
use crate::sessions::QueryContext;
use crate::sql::planner::metadata::MetadataRef;
use crate::storages::NavigationPoint;
use crate::storages::Table;

mod aggregate;
mod bind_context;
mod ddl;
mod distinct;
mod join;
mod limit;
mod project;
mod scalar;
mod scalar_common;
mod scalar_visitor;
mod select;
mod sort;
mod subquery;
mod table;

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
    metadata: MetadataRef,
}

impl<'a> Binder {
    pub fn new(
        ctx: Arc<QueryContext>,
        catalogs: Arc<CatalogManager>,
        metadata: MetadataRef,
    ) -> Self {
        Binder {
            ctx,
            catalogs,
            metadata,
        }
    }

    pub async fn bind(mut self, stmt: &Statement<'a>) -> Result<Plan> {
        let init_bind_context = BindContext::new();
        let plan = self.bind_statement(&init_bind_context, stmt).await?;
        Ok(plan)
    }

    #[async_recursion::async_recursion]
    async fn bind_statement(
        &mut self,
        bind_context: &BindContext,
        stmt: &Statement<'a>,
    ) -> Result<Plan> {
        match stmt {
            Statement::Query(query) => {
                let (mut s_expr, bind_context) = self.bind_query(bind_context, query).await?;
                let mut rewriter = SubqueryRewriter::new(self.metadata.clone());
                s_expr = rewriter.rewrite(&s_expr)?;
                Ok(Plan::Query {
                    s_expr,
                    metadata: self.metadata.clone(),
                    bind_context: Box::new(bind_context),
                })
            }

            Statement::Explain { query, kind } => {
                let plan = self.bind_statement(bind_context, query).await?;
                Ok(Plan::Explain {
                    kind: kind.clone(),
                    plan: Box::new(plan),
                })
            }

            Statement::CreateTable(stmt) => {
                let plan = self.bind_create_table(stmt).await?;
                Ok(plan)
            }
            
            Statement::ShowStages => {
                Ok(Plan::ShowStages)
            }

            Statement::ShowMetrics => Ok(Plan::ShowMetrics),
            Statement::ShowProcessList => Ok(Plan::ShowProcessList),
            Statement::ShowSettings => Ok(Plan::ShowSettings),

            _ => Err(ErrorCode::UnImplement(format!(
                "UnImplemented stmt {stmt} in binder"
            ))),
        }
    }

    async fn resolve_data_source(
        &self,
        tenant: &str,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        travel_point: &Option<TimeTravelPoint>,
    ) -> Result<Arc<dyn Table>> {
        // Resolve table with catalog
        let catalog = self.catalogs.get_catalog(catalog_name)?;
        let mut table_meta = catalog.get_table(tenant, database_name, table_name).await?;
        if let Some(TimeTravelPoint::Snapshot(s)) = travel_point {
            table_meta = table_meta
                .navigate_to(self.ctx.clone(), &NavigationPoint::SnapshotID(s.to_owned()))
                .await?;
        }
        Ok(table_meta)
    }

    /// Create a new ColumnBinding with assigned index
    pub(super) fn create_column_binding(
        &mut self,
        table_name: Option<String>,
        column_name: String,
        data_type: DataTypeImpl,
    ) -> ColumnBinding {
        let index = self
            .metadata
            .write()
            .add_column(column_name.clone(), data_type.clone(), None);
        ColumnBinding {
            table_name,
            column_name,
            index,
            data_type,
            visible_in_unqualified_wildcard: true,
        }
    }
}
