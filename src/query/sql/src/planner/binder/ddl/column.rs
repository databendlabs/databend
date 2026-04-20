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
use databend_common_ast::ast::quote::QuotedString;
use databend_common_exception::Result;
use log::debug;

use crate::BindContext;
use crate::Binder;
use crate::binder::check_table_ref_access;
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
        if branch.is_some() {
            check_table_ref_access(self.ctx.as_ref())?;
        }
        catalog
            .get_table_with_branch(
                &self.ctx.get_tenant(),
                database.as_str(),
                &table,
                branch.as_deref(),
            )
            .await?;
        let current_catalog = catalog.name();
        let branch_filter_name = branch.clone().unwrap_or_default();
        let output_columns = if *full {
            "column_name AS `Field`,
                column_type AS `Type`,
                is_nullable AS `Null`,
                `default` AS `Default`,
                extra AS `Extra`,
                column_key AS `Key`,
                collation_name AS `Collation`,
                privileges AS `Privileges`,
                column_comment AS `Comment`"
        } else {
            "column_name AS `Field`,
                column_type AS `Type`,
                is_nullable AS `Null`,
                `default` AS `Default`,
                extra AS `Extra`,
                column_key AS `Key`"
        };

        let mut filters = vec![
            format!("database = {}", QuotedString(database.as_str(), '\'')),
            format!("`table` = {}", QuotedString(table.as_str(), '\'')),
            format!(
                "branch = {}",
                QuotedString(branch_filter_name.as_str(), '\'')
            ),
        ];
        match limit {
            None => {}
            Some(ShowLimit::Like { pattern }) => {
                filters.push(format!("column_name LIKE {}", QuotedString(pattern, '\'')));
            }
            Some(ShowLimit::Where { selection }) => {
                filters.push(format!("({selection})"));
            }
        };
        let filters = filters.join(" AND ");
        let query = format!(
            "SELECT {output_columns}
            FROM (
                SELECT
                    name AS column_name,
                    CASE
                        WHEN UPPER(data_type) IN ('VARCHAR', 'STRING') THEN CONCAT('varchar(', '16382', ')')
                        ELSE LOWER(data_type)
                    END AS column_type,
                    is_nullable,
                    default_expression AS `default`,
                    NULL AS extra,
                    NULL AS column_key,
                    CASE
                        WHEN UPPER(data_type) IN ('VARCHAR', 'STRING') THEN 'utf8mb4_general_ci'
                        ELSE NULL
                    END AS collation_name,
                    NULL AS privileges,
                    comment AS column_comment,
                    database,
                    `table`,
                    branch
                FROM {current_catalog}.system.columns
            ) AS columns_source
            WHERE {filters}
            ORDER BY column_name",
        );
        debug!("show columns rewrite to: {:?}", query);
        self.bind_rewrite_to_query(
            bind_context,
            query.as_str(),
            RewriteKind::ShowColumns(catalog_name, database, table),
        )
        .await
    }
}
