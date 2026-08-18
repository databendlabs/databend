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

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::database::is_system_database;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TimeNavigation;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::Binder;

pub(crate) struct SessionBranchTable {
    pub table: Arc<dyn Table>,
    pub branch: Option<String>,
}

pub(crate) fn applicable_session_branch_for_table(
    ctx: &dyn TableContext,
    session_branch: Option<String>,
    catalog: &str,
    database: &str,
    table_name: &str,
) -> Option<String> {
    session_branch.filter(|_| {
        catalog == CATALOG_DEFAULT
            && !is_system_database(database)
            && !ctx.is_temp_table(catalog, database, table_name)
    })
}

pub(crate) fn reject_session_branch(ctx: &dyn TableContext, operation: &str) -> Result<()> {
    if let Some(branch) = ctx.get_settings().get_session_branch()? {
        return Err(ErrorCode::Unimplemented(format!(
            "{operation} is not supported when session_branch is set to '{branch}'"
        )));
    }
    Ok(())
}

/// Warns when a persisted definition ignores `session_branch`.
pub(crate) fn warn_session_branch_ignored(ctx: &dyn TableContext, operation: &str) -> Result<()> {
    if let Some(branch) = ctx.get_settings().get_session_branch()? {
        ctx.push_warning(format!(
            "{operation}: session_branch '{branch}' does not apply to persisted \
             definitions; base tables are used unless an explicit `table/branch` \
             reference is written"
        ));
    }
    Ok(())
}

pub(crate) fn table_supports_session_branch(table: &dyn Table) -> bool {
    table.engine() == "FUSE"
        && !table.is_temp()
        && !table.is_read_only()
        && !table.options().contains_key("TRANSIENT")
}

impl Binder {
    pub(crate) async fn resolve_schema_branch(
        &self,
        catalog: &str,
        database: &str,
        table_name: &str,
        explicit_branch: Option<String>,
    ) -> Result<Option<String>> {
        if explicit_branch.is_some() {
            return Ok(explicit_branch);
        }

        let session_branch = applicable_session_branch_for_table(
            self.ctx.as_ref(),
            self.ctx.get_settings().get_session_branch()?,
            catalog,
            database,
            table_name,
        );
        let Some(session_branch) = session_branch else {
            return Ok(None);
        };

        let table = self
            .ctx
            .resolve_data_source(catalog, database, table_name, None, None)
            .await?;
        Ok(table_supports_session_branch(table.as_ref()).then_some(session_branch))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn resolve_read_table_with_session_branch(
        &self,
        catalog: &str,
        database: &str,
        table_name: &str,
        explicit_branch: Option<String>,
        navigation: Option<&TimeNavigation>,
        max_batch_size: Option<u64>,
        suppress_session_branch: bool,
    ) -> Result<SessionBranchTable> {
        if explicit_branch.is_some() {
            let table = self.resolve_data_source(
                &self.ctx,
                catalog,
                database,
                table_name,
                explicit_branch.as_deref(),
                navigation,
                max_batch_size,
            )?;
            return Ok(SessionBranchTable {
                table,
                branch: explicit_branch,
            });
        }

        // Persisted definitions ignore `session_branch`; explicit branches win.
        let session_branch = if suppress_session_branch {
            None
        } else {
            self.ctx.get_settings().get_session_branch()?
        };
        let session_branch = applicable_session_branch_for_table(
            self.ctx.as_ref(),
            session_branch,
            catalog,
            database,
            table_name,
        );
        let Some(session_branch) = session_branch else {
            let table = self.resolve_data_source(
                &self.ctx,
                catalog,
                database,
                table_name,
                None,
                navigation,
                max_batch_size,
            )?;
            return Ok(SessionBranchTable {
                table,
                branch: None,
            });
        };

        // The session branch only applies to tables that support Databend table refs.
        // For eligible tables, branch selection is strict: a missing branch must not silently
        // fall back to the base table.
        let base_table = self.resolve_data_source(
            &self.ctx,
            catalog,
            database,
            table_name,
            None,
            None,
            max_batch_size,
        )?;
        if !table_supports_session_branch(base_table.as_ref()) {
            let table = if let Some(desc) = navigation {
                databend_common_base::runtime::block_on(base_table.navigate_to(&self.ctx, desc))?
            } else {
                base_table
            };
            return Ok(SessionBranchTable {
                table,
                branch: None,
            });
        }

        let branch_table = self.resolve_data_source(
            &self.ctx,
            catalog,
            database,
            table_name,
            Some(session_branch.as_str()),
            None,
            max_batch_size,
        )?;
        if navigation.is_some_and(TimeNavigation::contains_table_tag) {
            return Err(Self::unsupported_tag_navigation_error(
                catalog,
                database,
                table_name,
                &session_branch,
            ));
        }

        let table = if let Some(desc) = navigation {
            databend_common_base::runtime::block_on(branch_table.navigate_to(&self.ctx, desc))?
        } else {
            branch_table
        };
        Ok(SessionBranchTable {
            table,
            branch: Some(session_branch),
        })
    }

    pub(crate) fn resolve_write_branch_with_session_branch(
        &self,
        catalog: &str,
        database: &str,
        table_name: &str,
        explicit_branch: Option<String>,
        suppress_session_branch: bool,
    ) -> Result<Option<String>> {
        if explicit_branch.is_some() {
            return Ok(explicit_branch);
        }

        // Writes do not fall back to base; a missing session branch must fail.
        let session_branch = if suppress_session_branch {
            None
        } else {
            self.ctx.get_settings().get_session_branch()?
        };
        let session_branch = applicable_session_branch_for_table(
            self.ctx.as_ref(),
            session_branch,
            catalog,
            database,
            table_name,
        );
        let Some(session_branch) = session_branch else {
            return Ok(None);
        };

        Ok(Some(session_branch))
    }

    fn unsupported_tag_navigation_error(
        catalog: &str,
        database: &str,
        table_name: &str,
        branch: &str,
    ) -> ErrorCode {
        ErrorCode::Unimplemented(format!(
            "Unsupported TAG navigation on branch reference `{catalog}.{database}.{table_name}/{branch}`"
        ))
    }
}
