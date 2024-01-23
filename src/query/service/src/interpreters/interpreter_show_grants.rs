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

use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::PrincipalIdentity;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_sql::plans::ShowGrantsPlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ShowGrantsInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowGrantsPlan,
}

impl ShowGrantsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowGrantsPlan) -> Result<Self> {
        Ok(ShowGrantsInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowGrantsInterpreter {
    fn name(&self) -> &str {
        "ShowGrantsInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();

        // TODO: add permission check on reading user grants
        let (identity, grant_set) = match self.plan.principal {
            None => {
                let user = self.ctx.get_current_user()?;
                (user.identity().to_string(), user.grants)
            }
            Some(ref principal) => match principal {
                PrincipalIdentity::User(user) => {
                    let user = UserApiProvider::instance()
                        .get_user(&tenant, user.clone())
                        .await?;
                    (user.identity().to_string(), user.grants)
                }
                PrincipalIdentity::Role(role) => {
                    let role = UserApiProvider::instance()
                        .get_role(&tenant, role.clone())
                        .await?;
                    (format!("ROLE `{}`", role.identity()), role.grants)
                }
            },
        };
        // TODO: display roles list instead of the inherited roles
        let grant_entries = RoleCacheManager::instance()
            .find_related_roles(&tenant, &grant_set.roles())
            .await?
            .into_iter()
            .map(|role| role.grants)
            .fold(grant_set, |a, b| a | b)
            .entries();

        let mut grant_list: Vec<String> = Vec::new();
        for grant_entry in grant_entries {
            let object = grant_entry.object();
            match object {
                GrantObject::TableById(catalog_name, db_id, table_id) => {
                    let privileges_str =
                        if grant_entry.has_all_available_privileges_without_ownership() {
                            "ALL".to_string()
                        } else {
                            let privileges: UserPrivilegeSet = (*grant_entry.privileges()).into();
                            privileges.to_string()
                        };
                    let catalog = self.ctx.get_catalog(catalog_name).await?;
                    let db_name = catalog.get_db_name_by_id(*db_id).await?;
                    let table_name = catalog.get_table_name_by_id(*table_id).await?;
                    grant_list.push(format!(
                        "GRANT {} ON '{}'.'{}'.'{}' TO {}",
                        &privileges_str, catalog_name, db_name, table_name, identity
                    ));
                }
                GrantObject::DatabaseById(catalog_name, db_id) => {
                    let privileges_str =
                        if grant_entry.has_all_available_privileges_without_ownership() {
                            "ALL".to_string()
                        } else {
                            let privileges: UserPrivilegeSet = (*grant_entry.privileges()).into();
                            privileges.to_string()
                        };
                    let catalog = self.ctx.get_catalog(catalog_name).await?;
                    let db_name = catalog.get_db_name_by_id(*db_id).await?;
                    grant_list.push(format!(
                        "GRANT {} ON '{}'.'{}'.* TO {}",
                        &privileges_str, catalog_name, db_name, identity
                    ));
                }
                _ => {
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
            }
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(grant_list),
        ])])
    }
}
