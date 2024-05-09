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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_meta_app::principal::GrantEntry;
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

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();

        // TODO: add permission check on reading user grants
        let (grant_to, name, identity, grant_set) = match self.plan.principal {
            None => {
                let user = self.ctx.get_current_user()?;
                (
                    "USER".to_string(),
                    user.name.to_string(),
                    user.identity().display().to_string(),
                    user.grants,
                )
            }
            Some(ref principal) => match principal {
                PrincipalIdentity::User(user) => {
                    let user = UserApiProvider::instance()
                        .get_user(&tenant, user.clone())
                        .await?;
                    (
                        "USER".to_string(),
                        user.name.to_string(),
                        user.identity().display().to_string(),
                        user.grants,
                    )
                }
                PrincipalIdentity::Role(role) => {
                    let role_info = UserApiProvider::instance()
                        .get_role(&tenant, role.clone())
                        .await?;
                    (
                        "ROLE".to_string(),
                        role.to_string(),
                        format!("ROLE `{}`", role_info.identity()),
                        role_info.grants,
                    )
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

        // must split with two hashmap, hashmap key is catalog name.
        // maybe contain: default.db1 and default.db2.t,
        // It will re-write the exists key.
        let mut catalog_db_ids: HashMap<String, Vec<(u64, String)>> = HashMap::new();
        let mut catalog_table_ids: HashMap<String, Vec<(u64, u64, String)>> = HashMap::new();

        fn get_priv_str(grant_entry: &GrantEntry) -> String {
            if grant_entry.has_all_available_privileges() {
                "ALL".to_string()
            } else {
                let privileges: UserPrivilegeSet = (*grant_entry.privileges()).into();
                privileges.to_string()
            }
        }

        let mut object_id = vec![];
        let mut object_name = vec![];
        let mut privileges = vec![];
        for grant_entry in grant_entries {
            let object = grant_entry.object();
            match object {
                GrantObject::TableById(catalog_name, db_id, table_id) => {
                    let privileges_str = get_priv_str(&grant_entry);
                    if let Some(tables_id_priv) = catalog_table_ids.get(catalog_name) {
                        let mut tables_id_priv = tables_id_priv.clone();
                        tables_id_priv.push((*db_id, *table_id, privileges_str));
                        catalog_table_ids.insert(catalog_name.clone(), tables_id_priv.clone());
                    } else {
                        catalog_table_ids.insert(catalog_name.clone(), vec![(
                            *db_id,
                            *table_id,
                            privileges_str,
                        )]);
                    }
                }
                GrantObject::DatabaseById(catalog_name, db_id) => {
                    let privileges_str = get_priv_str(&grant_entry);
                    if let Some(dbs_id_priv) = catalog_db_ids.get(catalog_name) {
                        let mut dbs_id_priv = dbs_id_priv.clone();
                        dbs_id_priv.push((*db_id, privileges_str));
                        catalog_db_ids.insert(catalog_name.clone(), dbs_id_priv.clone());
                    } else {
                        catalog_db_ids.insert(catalog_name.clone(), vec![(*db_id, privileges_str)]);
                    }
                }
                GrantObject::Database(catalog_name, database_name) => {
                    object_name.push(format!("{}.{}.*", catalog_name, database_name));
                    object_id.push(None);
                    privileges.push(get_priv_str(&grant_entry));
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
                GrantObject::Table(catalog_name, database_name, table_name) => {
                    object_name.push(format!("{}.{}.{}", catalog_name, database_name, table_name));
                    object_id.push(None);
                    privileges.push(get_priv_str(&grant_entry));
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
                GrantObject::Stage(stage_name) => {
                    object_name.push(stage_name.to_string());
                    object_id.push(None);
                    privileges.push(get_priv_str(&grant_entry));
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
                GrantObject::UDF(udf_name) => {
                    object_name.push(udf_name.to_string());
                    object_id.push(None);
                    privileges.push(get_priv_str(&grant_entry));
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
                GrantObject::Task(task_name) => {
                    object_name.push(task_name.to_string());
                    object_id.push(None);
                    privileges.push(get_priv_str(&grant_entry));
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
                GrantObject::Global => {
                    // grant all on *.* to a
                    object_name.push("*.*".to_string());
                    object_id.push(None);
                    privileges.push(get_priv_str(&grant_entry));
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
            }
        }

        for (catalog_name, dbs_priv_id) in catalog_db_ids {
            let catalog = self.ctx.get_catalog(&catalog_name).await?;
            let db_ids = dbs_priv_id.iter().map(|res| res.0).collect::<Vec<u64>>();
            let privileges_strs = dbs_priv_id
                .iter()
                .map(|res| res.1.clone())
                .collect::<Vec<String>>();
            let dbs_name = catalog.mget_database_names_by_ids(&tenant, &db_ids).await?;

            for (i, db_name) in dbs_name.iter().enumerate() {
                if let Some(db_name) = db_name {
                    object_name.push(db_name.to_string());
                    object_id.push(Some(db_ids[i]));
                    privileges.push(privileges_strs[i].to_string());
                    grant_list.push(format!(
                        "GRANT {} ON '{}'.'{}'.* TO {}",
                        &privileges_strs[i], catalog_name, db_name, identity
                    ));
                }
            }
        }

        for (catalog_name, tables_priv_id) in catalog_table_ids {
            let catalog = self.ctx.get_catalog(&catalog_name).await?;
            let db_ids = tables_priv_id.iter().map(|res| res.0).collect::<Vec<u64>>();
            let table_ids = tables_priv_id.iter().map(|res| res.1).collect::<Vec<u64>>();
            let privileges_strs = tables_priv_id
                .iter()
                .map(|res| res.2.clone())
                .collect::<Vec<String>>();
            let dbs_name = catalog.mget_database_names_by_ids(&tenant, &db_ids).await?;
            let tables_name = catalog.mget_table_names_by_ids(&tenant, &table_ids).await?;

            for (i, table_name) in tables_name.iter().enumerate() {
                if let Some(table_name) = table_name {
                    if let Some(db_name) = &dbs_name[i] {
                        object_name.push(format!("{}.{}.{}", catalog_name, db_name, table_name));
                        object_id.push(Some(table_ids[i]));
                        privileges.push(privileges_strs[i].to_string());
                        grant_list.push(format!(
                            "GRANT {} ON '{}'.'{}'.'{}' TO {}",
                            &privileges_strs[i], catalog_name, db_name, table_name, identity
                        ));
                    }
                }
            }
        }

        let names: Vec<String> = vec![name; privileges.len()];
        let grant_tos: Vec<String> = vec![grant_to; privileges.len()];
        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(privileges),
            StringType::from_data(object_name),
            UInt64Type::from_opt_data(object_id),
            StringType::from_data(grant_tos),
            StringType::from_data(names),
            StringType::from_data(grant_list),
        ])])
    }
}
