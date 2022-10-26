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

use common_exception::Result;
use common_meta_types::GrantObject;
use common_meta_types::UserPrivilegeType;

use crate::interpreters::access::AccessChecker;
use crate::sessions::QueryContext;
use crate::sql::plans::Plan;

pub struct PrivilegeAccess {
    ctx: Arc<QueryContext>,
}

impl PrivilegeAccess {
    pub fn create(ctx: Arc<QueryContext>) -> Box<dyn AccessChecker> {
        Box::new(PrivilegeAccess { ctx })
    }
}

#[async_trait::async_trait]
impl AccessChecker for PrivilegeAccess {
    async fn check(&self, plan: &Plan) -> Result<()> {
        let session = self.ctx.get_current_session();

        match plan {
            Plan::Query { .. } => {}
            Plan::Explain { .. } => {}
            Plan::Copy(_) => {}
            Plan::Call(_) => {}

            // Database.
            Plan::ShowCreateDatabase(_) => {}
            Plan::CreateDatabase(_) => {
                session
                    .validate_privilege(&GrantObject::Global, UserPrivilegeType::Create)
                    .await?;
            }
            Plan::DropDatabase(_) => {
                session
                    .validate_privilege(&GrantObject::Global, UserPrivilegeType::Drop)
                    .await?;
            }
            Plan::UndropDatabase(_) => {
                session
                    .validate_privilege(&GrantObject::Global, UserPrivilegeType::Drop)
                    .await?;
            }
            Plan::RenameDatabase(_) => {
                session
                    .validate_privilege(&GrantObject::Global, UserPrivilegeType::Alter)
                    .await?;
            }
            Plan::UseDatabase(_) => {}

            // Table.
            Plan::ShowCreateTable(_) => {}
            Plan::DescribeTable(_) => {}
            Plan::CreateTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        UserPrivilegeType::Create,
                    )
                    .await?;
            }
            Plan::DropTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        UserPrivilegeType::Drop,
                    )
                    .await?;
            }
            Plan::UndropTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        UserPrivilegeType::Drop,
                    )
                    .await?;
            }
            Plan::RenameTable(_) => {}
            Plan::AlterTableClusterKey(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        UserPrivilegeType::Alter,
                    )
                    .await?;
            }
            Plan::DropTableClusterKey(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        UserPrivilegeType::Drop,
                    )
                    .await?;
            }
            Plan::ReclusterTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        UserPrivilegeType::Alter,
                    )
                    .await?;
            }
            Plan::TruncateTable(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Table(
                            plan.catalog.clone(),
                            plan.database.clone(),
                            plan.table.clone(),
                        ),
                        UserPrivilegeType::Delete,
                    )
                    .await?;
            }
            Plan::OptimizeTable(_) => {}
            Plan::ExistsTable(_) => {}

            // Others.
            Plan::Insert(_) => {}
            Plan::Delete(_) => {}
            Plan::CreateView(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        UserPrivilegeType::Alter,
                    )
                    .await?;
            }
            Plan::AlterView(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        UserPrivilegeType::Alter,
                    )
                    .await?;
            }
            Plan::DropView(plan) => {
                session
                    .validate_privilege(
                        &GrantObject::Database(plan.catalog.clone(), plan.database.clone()),
                        UserPrivilegeType::Drop,
                    )
                    .await?;
            }
            Plan::AlterUser(_) => {}
            Plan::CreateUser(_) => {}
            Plan::DropUser(_) => {}
            Plan::CreateUDF(_) => {}
            Plan::AlterUDF(_) => {}
            Plan::DropUDF(_) => {}
            Plan::CreateRole(_) => {}
            Plan::DropRole(_) => {}
            Plan::GrantRole(_) => {}
            Plan::GrantPriv(_) => {}
            Plan::ShowGrants(_) => {}
            Plan::RevokePriv(_) => {}
            Plan::RevokeRole(_) => {}
            Plan::ListStage(_) => {}
            Plan::CreateStage(_) => {}
            Plan::DropStage(_) => {}
            Plan::RemoveStage(_) => {}
            Plan::Presign(_) => {}
            Plan::SetVariable(_) => {}
            Plan::Kill(_) => {
                session
                    .validate_privilege(&GrantObject::Global, UserPrivilegeType::Super)
                    .await?;
            }
            Plan::CreateShare(_) => {}
            Plan::DropShare(_) => {}
            Plan::GrantShareObject(_) => {}
            Plan::RevokeShareObject(_) => {}
            Plan::AlterShareTenants(_) => {}
            Plan::DescShare(_) => {}
            Plan::ShowShares(_) => {}
            Plan::ShowObjectGrantPrivileges(_) => {}
            Plan::ShowGrantTenantsOfShare(_) => {}
            Plan::ExplainAst { .. } => {}
            Plan::ExplainSyntax { .. } => {}
        }

        Ok(())
    }
}
