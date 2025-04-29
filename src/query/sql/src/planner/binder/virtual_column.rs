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

use databend_common_ast::ast::RefreshVirtualColumnStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowVirtualColumnsStmt;
use databend_common_exception::Result;
use log::debug;

use crate::binder::Binder;
use crate::normalize_identifier;
use crate::plans::Plan;
use crate::plans::RefreshVirtualColumnPlan;
use crate::plans::RewriteKind;
use crate::BindContext;
use crate::SelectBuilder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_refresh_virtual_column(
        &mut self,
        stmt: &RefreshVirtualColumnStmt,
    ) -> Result<Plan> {
        let RefreshVirtualColumnStmt {
            catalog,
            database,
            table,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::RefreshVirtualColumn(Box::new(
            RefreshVirtualColumnPlan {
                catalog,
                database,
                table,
                segment_locs: None,
            },
        )))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_virtual_columns(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowVirtualColumnsStmt,
    ) -> Result<Plan> {
        let ShowVirtualColumnsStmt {
            catalog,
            database,
            table,
            limit,
        } = stmt;

        let catalog_name = match catalog {
            None => self.ctx.get_current_catalog(),
            Some(ident) => {
                let catalog = normalize_identifier(ident, &self.name_resolution_ctx).name;
                self.ctx.get_catalog(&catalog).await?;
                catalog
            }
        };
        let catalog = self.ctx.get_catalog(&catalog_name).await?;
        let database = match database {
            None => self.ctx.get_current_database(),
            Some(ident) => {
                let database = normalize_identifier(ident, &self.name_resolution_ctx).name;
                catalog
                    .get_database(&self.ctx.get_tenant(), &database)
                    .await?;
                database
            }
        };

        let mut select_builder = SelectBuilder::from("default.system.virtual_columns");
        select_builder
            .with_column("database")
            .with_column("table")
            .with_column("source_column")
            .with_column("virtual_column_id")
            .with_column("virtual_column_name")
            .with_column("virtual_column_type");

        select_builder.with_filter(format!("database = '{database}'"));
        if let Some(table) = table {
            let table = normalize_identifier(table, &self.name_resolution_ctx).name;
            select_builder.with_filter(format!("table = '{table}'"));
        }

        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("virtual_column_name LIKE '{pattern}'"));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };
        debug!("show virtual columns rewrite to: {:?}", query);

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowVirtualColumns)
            .await
    }
}
