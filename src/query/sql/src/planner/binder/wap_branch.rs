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
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TimeNavigation;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::Binder;

pub(crate) struct WapBranchTable {
    pub table: Arc<dyn Table>,
    pub branch: Option<String>,
}

pub(crate) fn applicable_wap_branch_for_table(
    ctx: &dyn TableContext,
    wap_branch: Option<String>,
    catalog: &str,
    database: &str,
    table_name: &str,
) -> Option<String> {
    wap_branch.filter(|_| {
        catalog == CATALOG_DEFAULT
            && !is_system_database(database)
            && !ctx.is_temp_table(catalog, database, table_name)
    })
}

pub(crate) fn reject_wap_branch(ctx: &dyn TableContext, operation: &str) -> Result<()> {
    if let Some(branch) = ctx.get_settings().get_wap_branch()? {
        return Err(ErrorCode::Unimplemented(format!(
            "{operation} is not supported when wap_branch is set to '{branch}'"
        )));
    }
    Ok(())
}

/// Warns when a persisted definition ignores the session `wap_branch`.
pub(crate) fn warn_wap_branch_ignored(ctx: &dyn TableContext, operation: &str) -> Result<()> {
    if let Some(branch) = ctx.get_settings().get_wap_branch()? {
        ctx.push_warning(format!(
            "{operation}: session wap_branch '{branch}' does not apply to persisted \
             definitions; base tables are used unless an explicit `table/branch` \
             reference is written"
        ));
    }
    Ok(())
}

impl Binder {
    pub(crate) fn current_wap_branch(&self) -> Result<Option<String>> {
        self.ctx.get_settings().get_wap_branch()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn resolve_read_table_with_wap_branch(
        &self,
        catalog: &str,
        database: &str,
        table_name: &str,
        explicit_branch: Option<String>,
        navigation: Option<&TimeNavigation>,
        max_batch_size: Option<u64>,
        suppress_wap_branch: bool,
    ) -> Result<WapBranchTable> {
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
            return Ok(WapBranchTable {
                table,
                branch: explicit_branch,
            });
        }

        // Persisted definitions ignore session `wap_branch`; explicit branches win.
        let wap_branch = if suppress_wap_branch {
            None
        } else {
            self.current_wap_branch()?
        };
        let wap_branch = applicable_wap_branch_for_table(
            self.ctx.as_ref(),
            wap_branch,
            catalog,
            database,
            table_name,
        );
        let Some(wap_branch) = wap_branch else {
            let table = self.resolve_base_read_table(
                catalog,
                database,
                table_name,
                navigation,
                max_batch_size,
            )?;
            return Ok(WapBranchTable {
                table,
                branch: None,
            });
        };

        match self.resolve_data_source(
            &self.ctx,
            catalog,
            database,
            table_name,
            Some(wap_branch.as_str()),
            None,
            max_batch_size,
        ) {
            Ok(branch_table) => {
                if Self::contains_tag_navigation(navigation) {
                    return Err(Self::unsupported_tag_navigation_error(
                        catalog,
                        database,
                        table_name,
                        &wap_branch,
                    ));
                }

                let table = if let Some(desc) = navigation {
                    databend_common_base::runtime::block_on(
                        branch_table.navigate_to(&self.ctx, desc),
                    )?
                } else {
                    branch_table
                };
                Ok(WapBranchTable {
                    table,
                    branch: Some(wap_branch),
                })
            }
            Err(err) if err.code() == ErrorCode::UNKNOWN_REFERENCE => {
                let table = self.resolve_base_read_table(
                    catalog,
                    database,
                    table_name,
                    navigation,
                    max_batch_size,
                )?;
                Ok(WapBranchTable {
                    table,
                    branch: None,
                })
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) fn resolve_write_branch_with_wap_branch(
        &self,
        catalog: &str,
        database: &str,
        table_name: &str,
        explicit_branch: Option<String>,
    ) -> Result<Option<String>> {
        if explicit_branch.is_some() {
            return Ok(explicit_branch);
        }

        // Writes do not fall back to base; a missing WAP branch must fail.
        let wap_branch = applicable_wap_branch_for_table(
            self.ctx.as_ref(),
            self.current_wap_branch()?,
            catalog,
            database,
            table_name,
        );
        let Some(wap_branch) = wap_branch else {
            return Ok(None);
        };

        Ok(Some(wap_branch))
    }

    fn resolve_base_read_table(
        &self,
        catalog: &str,
        database: &str,
        table_name: &str,
        navigation: Option<&TimeNavigation>,
        max_batch_size: Option<u64>,
    ) -> Result<Arc<dyn Table>> {
        self.resolve_data_source(
            &self.ctx,
            catalog,
            database,
            table_name,
            None,
            navigation,
            max_batch_size,
        )
    }

    fn contains_tag_navigation(navigation: Option<&TimeNavigation>) -> bool {
        matches!(
            navigation,
            Some(TimeNavigation::TimeTravel(NavigationPoint::TableTag(_)))
                | Some(TimeNavigation::Changes {
                    at: NavigationPoint::TableTag(_),
                    ..
                })
                | Some(TimeNavigation::Changes {
                    end: Some(NavigationPoint::TableTag(_)),
                    ..
                })
        )
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
