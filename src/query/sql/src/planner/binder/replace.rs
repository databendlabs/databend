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

use std::sync::Arc;

use common_ast::ast::ReplaceStmt;
use common_exception::Result;

use crate::binder::Binder;
use crate::normalize_identifier;
use crate::plans::CopyIntoTableMode;
use crate::plans::InsertInputSource;
use crate::plans::Plan;
use crate::plans::Replace;
use crate::BindContext;
impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_replace(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ReplaceStmt,
    ) -> Result<Plan> {
        let ReplaceStmt {
            catalog,
            database,
            table,
            on_conflict_columns,
            columns,
            source,
            ..
        } = stmt;

        let catalog_name = catalog.as_ref().map_or_else(
            || self.ctx.get_current_catalog(),
            |ident| normalize_identifier(ident, &self.name_resolution_ctx).name,
        );
        let database_name = database.as_ref().map_or_else(
            || self.ctx.get_current_database(),
            |ident| normalize_identifier(ident, &self.name_resolution_ctx).name,
        );
        let table_name = normalize_identifier(table, &self.name_resolution_ctx).name;
        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;
        let table_id = table.get_id();

        let schema = if columns.is_empty() {
            table.schema()
        } else {
            let schema = table.schema();
            let field_indexes = columns
                .iter()
                .map(|ident| {
                    schema.index_of(&normalize_identifier(ident, &self.name_resolution_ctx).name)
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(schema.project(&field_indexes))
        };

        let on_conflict_fields = on_conflict_columns
            .iter()
            .map(|ident| {
                schema
                    .field_with_name(&normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .map(|v| v.clone())
            })
            .collect::<Result<Vec<_>>>()?;

        let input_source: Result<InsertInputSource> = self
            .get_source(
                bind_context,
                catalog_name.clone(),
                database_name.clone(),
                table_name.clone(),
                schema.clone(),
                source.clone(),
                CopyIntoTableMode::Replace,
            )
            .await;

        let plan = Replace {
            catalog: catalog_name.to_string(),
            database: database_name.to_string(),
            table: table_name,
            table_id,
            on_conflict_fields,
            schema,
            source: input_source?,
        };

        Ok(Plan::Replace(Box::new(plan)))
    }
}
