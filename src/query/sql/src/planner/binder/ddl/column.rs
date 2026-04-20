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

use databend_common_ast::ast::ShowColumnsStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_exception::Result;
use log::debug;

use crate::BindContext;
use crate::Binder;
use crate::SelectBuilder;
use crate::binder::util::TableIdentifier;
use crate::plans::Plan;
use crate::plans::RewriteKind;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_columns(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowColumnsStmt,
    ) -> Result<Plan> {
        let ShowColumnsStmt { table, full, limit } = stmt;

        let table_identifier = TableIdentifier::new_with_ref(self, table, &None);
        let (catalog_name, database, table, branch) = (
            table_identifier.catalog_name(),
            table_identifier.database_name(),
            table_identifier.table_name(),
            table_identifier.branch_name(),
        );
        let catalog = self.ctx.get_catalog(&catalog_name).await?;
        catalog
            .get_table_with_branch(
                &self.ctx.get_tenant(),
                database.as_str(),
                &table,
                branch.as_deref(),
            )
            .await?;
        let table_filter_name = branch
            .as_ref()
            .map(|branch| format!("{table}/{branch}"))
            .unwrap_or_else(|| table.clone());

        let current_catalog = catalog.name();
        let mut select_builder =
            SelectBuilder::from(&format!("{}.information_schema.columns", current_catalog));

        select_builder
            .with_column("column_name AS `Field`")
            .with_column("column_type AS `Type`")
            .with_column("is_nullable AS `Null`")
            .with_column("default AS `Default`")
            .with_column("extra AS `Extra`")
            .with_column("column_key AS `Key`");

        if *full {
            select_builder
                .with_column("collation_name AS `Collation`")
                .with_column("privileges AS `Privileges`")
                .with_column("column_comment AS `Comment`");
        }

        select_builder
            .with_order_by("table_schema")
            .with_order_by("table_name")
            .with_order_by("column_name");

        select_builder
            .with_filter(format!("table_schema = '{database}'"))
            .with_filter(format!("table_name = '{table_filter_name}'"));

        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("column_name LIKE '{pattern}'"));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };
        debug!("show columns rewrite to: {:?}", query);
        self.bind_rewrite_to_query(
            bind_context,
            query.as_str(),
            RewriteKind::ShowColumns(catalog_name, database, table),
        )
        .await
    }
}
