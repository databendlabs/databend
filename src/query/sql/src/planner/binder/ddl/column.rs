// Copyright 2023 Datafuse Labs.
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

use common_ast::ast::ShowColumnsStmt;
use common_ast::ast::ShowLimit;
use common_exception::Result;
use tracing::debug;

use crate::normalize_identifier;
use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::BindContext;
use crate::Binder;
use crate::SelectBuilder;

impl Binder {
    pub(in crate::planner::binder) async fn bind_show_columns(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowColumnsStmt,
    ) -> Result<Plan> {
        let ShowColumnsStmt {
            catalog,
            database,
            table,
            full,
            limit,
        } = stmt;

        let catalog_name = match catalog {
            None => self.ctx.get_current_catalog(),
            Some(ident) => {
                let catalog = normalize_identifier(ident, &self.name_resolution_ctx).name;
                self.ctx.get_catalog(&catalog)?;
                catalog
            }
        };
        let catalog = self.ctx.get_catalog(&catalog_name)?;
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

        let table = {
            let table = normalize_identifier(table, &self.name_resolution_ctx).name;
            catalog
                .get_table(&self.ctx.get_tenant(), database.as_str(), &table)
                .await?;
            table
        };

        let mut select_builder = SelectBuilder::from("system.columns");

        select_builder
            .with_column("name AS Field")
            .with_column("type AS Type")
            .with_column("is_nullable AS `Null`")
            .with_column("data_type AS Data_Type")
            .with_column("default_kind AS Default_Kind")
            .with_column("default_expression AS Default_Expression");

        if *full {
            select_builder.with_column("comment AS Comment");
        }

        select_builder
            .with_order_by("database")
            .with_order_by("table")
            .with_order_by("name");

        select_builder
            .with_filter(format!("database = '{database}'"))
            .with_filter(format!("table = '{table}'"));

        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("name LIKE '{pattern}'"));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };
        debug!("show columns rewrite to: {:?}", query);
        self.bind_rewrite_to_query(bind_context, query.as_str(), RewriteKind::ShowColumns)
            .await
    }
}
