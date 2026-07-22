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

use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_table_ref;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::planner::NameResolutionContext;
use databend_common_sql::planner::normalize_identifier;

use crate::sessions::TableContext;

pub(crate) struct TableNameParser {
    current_catalog: String,
    current_database: String,
    dialect: Dialect,
    name_resolution_ctx: NameResolutionContext,
}

impl TableNameParser {
    pub(crate) fn new(ctx: &Arc<dyn TableContext>) -> Result<Self> {
        let settings = ctx.get_settings();
        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
        let dialect = settings.get_sql_dialect().unwrap_or_default();

        Ok(Self {
            current_catalog: ctx.get_current_catalog(),
            current_database: ctx.get_current_database(),
            dialect,
            name_resolution_ctx,
        })
    }

    /// Parse table name in format "table", "db.table", or "catalog.db.table".
    /// Correctly handles quoted identifiers and normalizes them according to session settings.
    pub(crate) fn parse_table_name(&self, name: &str) -> Result<(String, String, String)> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(ErrorCode::BadArguments("object_name must not be empty"));
        }

        let table_ref = parse_table_ref(trimmed, self.dialect).map_err(|e| {
            ErrorCode::BadArguments(format!("Invalid table name '{}': {}", name, e.1))
        })?;

        let catalog = table_ref
            .catalog
            .map(|i| normalize_identifier(&i, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.current_catalog.clone());
        let database = table_ref
            .database
            .map(|i| normalize_identifier(&i, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.current_database.clone());
        let table = normalize_identifier(&table_ref.table, &self.name_resolution_ctx).name;

        Ok((catalog, database, table))
    }

    pub(crate) fn normalize_column_identifier(&self, name: &str) -> String {
        let trimmed = name.trim();
        if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
            return trimmed[1..trimmed.len() - 1].replace("\"\"", "\"");
        }

        if self.name_resolution_ctx.unquoted_ident_case_sensitive {
            trimmed.to_string()
        } else {
            trimmed.to_lowercase()
        }
    }
}
