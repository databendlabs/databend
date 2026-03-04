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

use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;

use crate::interpreters::access::AccessChecker;
use crate::sessions::QueryContext;
use crate::sql::plans::Plan;

pub struct ManagementModeAccess {}
impl ManagementModeAccess {
    pub fn create() -> Box<dyn AccessChecker> {
        Box::new(ManagementModeAccess {})
    }
}

#[async_trait::async_trait]
impl AccessChecker for ManagementModeAccess {
    // Check what we can do if in management mode.
    #[async_backtrace::framed]
    async fn check(&self, ctx: &Arc<QueryContext>, plan: &Plan) -> Result<()> {
        // Allows for management-mode.
        if GlobalConfig::instance().query.common.management_mode {
            let ok = match plan {
                Plan::Query {rewrite_kind, .. } => {
                    use databend_common_sql::plans::RewriteKind;
                        match rewrite_kind  {
                            Some( v) => matches!(v,
                            RewriteKind::ShowDatabases
                            | RewriteKind::ShowDropDatabases
                            | RewriteKind::ShowTables(_, _)
                            | RewriteKind::ShowColumns(_, _, _)
                            | RewriteKind::ShowEngines
                            | RewriteKind::ShowSettings
                            | RewriteKind::ShowVariables
                            | RewriteKind::ShowFunctions
                            | RewriteKind::ShowUserFunctions
                            | RewriteKind::ShowTableFunctions
                            | RewriteKind::ShowUsers
                            // show grants will access meta, can not true in mm.
                            // | RewriteKind::ShowGrants
                            | RewriteKind::ShowStages
                            | RewriteKind::DescribeStage
                            | RewriteKind::ListStage
                            | RewriteKind::Call
                            | RewriteKind::ShowRoles
                            | RewriteKind::ShowLocks
                            | RewriteKind::ShowStreams(_)
                            | RewriteKind::ShowTags),
                            _ => false
                        }
                },
                // Show.
                Plan::ShowCreateDatabase(_)
                | Plan::ShowCreateTable(_)

                // Set
                | Plan::Set(_)

                // Database.
                | Plan::CreateDatabase(_)
                | Plan::DropDatabase(_)

                // Table.
                | Plan::CreateTable(_)
                | Plan::DropTable(_)
                | Plan::DropView(_)
                | Plan::CreateView(_)
                | Plan::CreateStream(_)
                | Plan::DropStream(_)

                // Dynamic table.
                | Plan::CreateDynamicTable(_)

                // User.
                | Plan::AlterUser(_)
                | Plan::CreateUser(_)
                | Plan::DropUser(_)
                | Plan::DescUser(_)

                // Roles.
                | Plan::CreateRole(_)
                | Plan::DropRole(_)

                // Privilege.
                | Plan::GrantPriv(_)
                | Plan::RevokePriv(_)
                | Plan::GrantRole(_)
                | Plan::RevokeRole(_)
                // Stage.
                | Plan::CreateStage(_)
                | Plan::AlterStage(_)
                | Plan::DropStage(_)
                // Network policy.
                | Plan::CreateNetworkPolicy(_)
                | Plan::AlterNetworkPolicy(_)
                | Plan::DropNetworkPolicy(_)
                // Password policy.
                | Plan::CreatePasswordPolicy(_)
                | Plan::AlterPasswordPolicy(_)
                | Plan::DropPasswordPolicy(_)

                // UDF
                | Plan::CreateUDF(_)
                | Plan::AlterUDF(_)
                | Plan::DropUDF(_)
                | Plan::SetObjectTags(_)
                | Plan::UnsetObjectTags(_)
                | Plan::UseDatabase(_) => true,
                Plan::DescribeTable(plan) => {
                    let catalog = &plan.catalog;
                    let database = &plan.database;
                    let table = &plan.table;
                    let table = ctx.get_table(catalog, database, table).await?;
                    !matches!(table.get_table_info().engine(), VIEW_ENGINE|STREAM_ENGINE)
                },
                _ => false,
            };

            if !ok {
                return Err(ErrorCode::ManagementModePermissionDenied(format!(
                    "Management Mode Error: Access denied for operation:{:?} in management-mode",
                    plan.format_indent(Default::default())?
                )));
            }
        };

        Ok(())
    }
}
