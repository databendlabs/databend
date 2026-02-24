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

use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::StringType;
use databend_common_management::RoleApi;
use databend_common_management::WarehouseInfo;
use databend_common_meta_api::DatamaskApi;
use databend_common_meta_api::RowAccessPolicyApi;
use databend_common_meta_app::principal::GrantEntry;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_sql::validate_function_arg;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_users::Object;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_enterprise_resources_management::ResourcesManagement;
use itertools::Itertools;

use crate::meta_service_error;

const SHOW_GRANTS: &str = "show_grants";

pub struct ShowGrants {
    grant_type: String,
    name: String,
    catalog: String,
    db_name: String,
    table_info: TableInfo,
}

// show grants for user/role name
// show grants to table/database/stage/udf name

impl ShowGrants {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args = table_args.positioned;
        // Check args len.
        validate_function_arg(table_func_name, args.len(), Some((2, 4)), 2)?;

        if !args.iter().all(|arg| matches!(arg, Scalar::String(_))) {
            return Err(ErrorCode::BadDataValueType(format!(
                "Expected String type, but got {:?}",
                args
            )));
        }

        let grant_type = args[0].as_string().unwrap().to_string();
        let name = args[1].as_string().unwrap().to_string();
        let (catalog, db_name) = if args.len() == 3 {
            (args[2].as_string().unwrap().to_string(), "".to_string())
        } else if args.len() == 4 {
            (
                args[2].as_string().unwrap().to_string(),
                args[3].as_string().unwrap().to_string(),
            )
        } else {
            ("".to_string(), "".to_string())
        };

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: Self::schema(&grant_type),
                engine: SHOW_GRANTS.to_owned(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self {
            grant_type,
            name,
            catalog,
            db_name,
            table_info,
        }))
    }

    fn schema(grant_type: &str) -> Arc<TableSchema> {
        if grant_type.to_lowercase() == "role_grantee" {
            TableSchemaRefExt::create(vec![
                TableField::new("role", TableDataType::String),
                TableField::new("granted_to", TableDataType::String),
                TableField::new("grantee_name", TableDataType::String),
            ])
        } else {
            TableSchemaRefExt::create(vec![
                TableField::new("privileges", TableDataType::String),
                TableField::new("object_name", TableDataType::String),
                TableField::new(
                    "object_id",
                    TableDataType::Nullable(Box::from(TableDataType::String)),
                ),
                TableField::new("grant_to", TableDataType::String),
                TableField::new("name", TableDataType::String),
                TableField::new("grants", TableDataType::String),
            ])
        }
    }
}

#[async_trait::async_trait]
impl Table for ShowGrants {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(TableArgs::new_positioned(vec![
            Scalar::String(self.grant_type.clone()),
            Scalar::String(self.name.clone()),
            Scalar::String(self.catalog.clone()),
            Scalar::String(self.db_name.clone()),
        ]))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                ShowGrantsSource::create(
                    ctx.clone(),
                    output,
                    self.grant_type.clone(),
                    self.name.clone(),
                    self.catalog.clone(),
                    self.db_name.clone(),
                )
            },
            1,
        )?;

        Ok(())
    }
}

struct ShowGrantsSource {
    ctx: Arc<dyn TableContext>,
    grant_type: String,
    name: String,
    catalog: String,
    db_name: String,
    finished: bool,
}

impl ShowGrantsSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        grant_type: String,
        name: String,
        catalog: String,
        db_name: String,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, ShowGrantsSource {
            ctx,
            grant_type,
            name,
            catalog,
            db_name,
            finished: false,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for ShowGrantsSource {
    const NAME: &'static str = "show_grants";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }

        let res = match self.grant_type.to_lowercase().as_str() {
            "role" | "user" => {
                show_account_grants(self.ctx.clone(), &self.grant_type, &self.name).await?
            }
            "table" | "database" | "udf" | "stage" | "warehouse" | "connection" | "sequence"
            | "procedure" | "masking_policy" | "row_access_policy" => {
                show_object_grant(
                    self.ctx.clone(),
                    &self.grant_type,
                    &self.name,
                    &self.catalog,
                    &self.db_name,
                )
                .await?
            }
            "role_grantee" => show_role_grantees(self.ctx.clone(), &self.name).await?,
            _ => {
                return Err(ErrorCode::InvalidArgument(format!(
                    "Expected 'user|role|table|database|udf|stage|warehouse|connection|sequence|procedure|masking_policy|row_access_policy|role_grantee', but got {:?}",
                    self.grant_type
                )));
            }
        };

        // Mark done.
        self.finished = true;
        Ok(res)
    }
}

async fn show_role_grantees(ctx: Arc<dyn TableContext>, name: &str) -> Result<Option<DataBlock>> {
    let tenant = ctx.get_tenant();
    let user_api = UserApiProvider::instance();

    let name = name.to_string();

    let user_type_str = "USER".to_string();
    let role_type_str = "ROLE".to_string();

    let mut collected_grantees: Vec<(String, String)> = Vec::new();

    let f = |roles: &HashSet<String>,
             type_str: String,
             grantee_name: String,
             name: &String|
     -> Option<(String, String)> {
        if roles.contains(name) {
            Some((type_str, grantee_name))
        } else {
            None
        }
    };
    let users = user_api.get_users(&tenant).await?;
    let user_grantees = users.into_iter().filter_map(|user| {
        f(
            user.grants.roles(),
            user_type_str.to_string(),
            user.name.to_string(),
            &name,
        )
    });
    collected_grantees.extend(user_grantees);

    let roles = user_api.role_api(&tenant).get_meta_roles().await?;
    let role_grantees = roles.into_iter().filter_map(|role_item| {
        f(
            role_item.grants.roles(),
            role_type_str.to_string(),
            role_item.name.to_string(),
            &name,
        )
    });
    collected_grantees.extend(role_grantees);

    let (granted_to_vec, grantee_name_vec): (Vec<String>, Vec<String>) =
        collected_grantees.into_iter().unzip();

    let len = grantee_name_vec.len();
    let role_column: Vec<String> = std::iter::repeat_n(name, len).collect();

    Ok(Some(DataBlock::new_from_columns(vec![
        StringType::from_data(role_column),
        StringType::from_data(granted_to_vec),
        StringType::from_data(grantee_name_vec),
    ])))
}

async fn show_account_grants(
    ctx: Arc<dyn TableContext>,
    grant_type: &str,
    name: &str,
) -> Result<Option<DataBlock>> {
    let tenant = ctx.get_tenant();
    let expand_roles = ctx.get_settings().get_enable_expand_roles()?;
    let current_user = ctx.get_current_user()?;
    let has_grant_priv = ctx
        .validate_privilege(&GrantObject::Global, UserPrivilegeType::Grant, false)
        .await
        .is_ok();

    let user_api = UserApiProvider::instance();

    let warehouse_mgr = GlobalInstance::get::<Arc<dyn ResourcesManagement>>();
    let warehouses = if warehouse_mgr.support_forward_warehouse_request() {
        warehouse_mgr.list_warehouses().await?
    } else {
        vec![]
    };

    // TODO: add permission check on reading user grants
    let (grant_to, name, identity, grant_set, roles_list) = match grant_type {
        "user" => {
            let user = user_api
                .get_user(&tenant, UserIdentity::new(name, "%"))
                .await?;
            if current_user.identity().username != name && !has_grant_priv {
                let mut roles: Vec<String> = current_user.grants.roles_vec();
                roles.sort();

                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege [Grant] is required on *.* for user {} with roles [{}]",
                    &current_user.identity().display(),
                    roles.join(",")
                )));
            }
            (
                "USER".to_string(),
                user.name.to_string(),
                user.identity().display().to_string(),
                user.grants,
                None,
            )
        }
        "role" => {
            let current_user_roles = ctx.get_all_effective_roles().await?;
            let effective_roles_names: Vec<String> = current_user_roles
                .iter()
                .map(|role| role.name.to_string())
                .collect();
            if !effective_roles_names.contains(&name.to_string()) && !has_grant_priv {
                let mut roles: Vec<String> = current_user.grants.roles_vec();
                roles.sort();
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege [Grant] is required on *.* for user {} with roles [{}]",
                    &current_user.identity().display(),
                    roles.join(",")
                )));
            }

            let role_info = user_api.get_role(&tenant, name.to_string()).await?;
            let role_info_roles: Vec<String> = role_info.grants.roles_vec();
            let related_roles = RoleCacheManager::instance()
                .find_related_roles(&tenant, &role_info_roles)
                .await?;
            let mut roles: Vec<String> = related_roles
                .iter()
                .map(|role| role.name.to_string())
                .collect();
            roles.push(name.to_string());
            (
                "ROLE".to_string(),
                name.to_string(),
                format!("ROLE `{}`", role_info.identity()),
                role_info.grants,
                Some(roles),
            )
        }
        _ => {
            return Err(ErrorCode::InvalidArgument(format!(
                "Expected 'user|role', but got {:?}",
                grant_type
            )));
        }
    };

    // TODO: display roles list instead of the inherited roles
    let grant_set_roles: Vec<String> = grant_set.roles_vec();
    let related_roles = RoleCacheManager::instance()
        .find_related_roles(&tenant, &grant_set_roles)
        .await?;

    let roles: Vec<String> = if let Some(roles) = roles_list {
        roles
    } else {
        related_roles
            .iter()
            .map(|role| role.name.to_string())
            .sorted()
            .collect()
    };

    let folded;
    let grant_entries = if expand_roles {
        folded = related_roles
            .into_iter()
            .map(|role| role.grants)
            .fold(grant_set, |a, b| a | b);
        folded.entries()
    } else {
        grant_set.entries()
    };

    let mut grant_list: Vec<String> = Vec::new();

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
    if !expand_roles {
        for role in &roles {
            if !(grant_type == "role" && role == &name || name == BUILTIN_ROLE_ACCOUNT_ADMIN) {
                object_name.push("".to_string());
                object_id.push(None);
                privileges.push("".to_string());
                grant_list.push(format!("GRANT ROLE {} to {}", role, identity));
            }
        }
    }
    // must split with two hashmap, hashmap key is catalog name.
    // maybe contain: default.db1 and default.db2.t,
    // It will re-write the exists key.
    let mut catalog_db_ids: HashMap<String, Vec<(u64, String)>> = HashMap::new();
    let mut catalog_table_ids: HashMap<String, Vec<(u64, u64, String)>> = HashMap::new();

    let procedure_api = UserApiProvider::instance().procedure_api(&tenant);

    for grant_entry in grant_entries {
        let object = grant_entry.object();
        let privilege = get_priv_str(grant_entry);
        // Ownership will list ownerships kv
        if privilege.to_lowercase() != "ownership" {
            match object {
                GrantObject::TableById(catalog_name, db_id, table_id) => {
                    let privileges_str = get_priv_str(grant_entry);
                    if let Some(tables_id_priv) = catalog_table_ids.get_mut(catalog_name) {
                        tables_id_priv.push((*db_id, *table_id, privileges_str));
                    } else {
                        catalog_table_ids.insert(catalog_name.clone(), vec![(
                            *db_id,
                            *table_id,
                            privileges_str,
                        )]);
                    }
                }
                GrantObject::DatabaseById(catalog_name, db_id) => {
                    let privileges_str = get_priv_str(grant_entry);
                    if let Some(dbs_id_priv) = catalog_db_ids.get_mut(catalog_name) {
                        dbs_id_priv.push((*db_id, privileges_str));
                    } else {
                        catalog_db_ids.insert(catalog_name.clone(), vec![(*db_id, privileges_str)]);
                    }
                }
                GrantObject::Database(catalog_name, database_name) => {
                    object_name.push(format!("{}.{}.*", catalog_name, database_name));
                    object_id.push(None);
                    privileges.push(get_priv_str(grant_entry));
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
                GrantObject::Table(catalog_name, database_name, table_name) => {
                    object_name.push(format!("{}.{}.{}", catalog_name, database_name, table_name));
                    object_id.push(None);
                    privileges.push(get_priv_str(grant_entry));
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
                GrantObject::Stage(stage_name) => {
                    object_name.push(stage_name.to_string());
                    object_id.push(None);
                    privileges.push(get_priv_str(grant_entry));
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
                GrantObject::UDF(udf_name) => {
                    object_name.push(udf_name.to_string());
                    object_id.push(None);
                    privileges.push(get_priv_str(grant_entry));
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
                GrantObject::Warehouse(id) => {
                    if let Some(sw) = warehouses
                        .iter()
                        .filter_map(|w| {
                            if let WarehouseInfo::SystemManaged(sw) = w {
                                Some(sw)
                            } else {
                                None
                            }
                        })
                        .find(|sw| sw.role_id == *id)
                    {
                        object_name.push(sw.id.to_string());
                        object_id.push(Some(id.to_string()));
                        privileges.push(get_priv_str(grant_entry));
                        grant_list.push(format!("{} TO {}", grant_entry, identity));
                    }
                }
                GrantObject::Connection(connection) => {
                    object_name.push(connection.to_string());
                    object_id.push(None);
                    privileges.push(get_priv_str(grant_entry));
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
                GrantObject::Sequence(seq) => {
                    object_name.push(seq.to_string());
                    object_id.push(None);
                    privileges.push(get_priv_str(grant_entry));
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
                GrantObject::Procedure(procedure_id) => {
                    if let Some(p) = procedure_api
                        .get_procedure_name_by_id(*procedure_id)
                        .await
                        .map_err(meta_service_error)?
                    {
                        object_name.push(p);
                        object_id.push(Some(procedure_id.to_string()));
                        privileges.push(get_priv_str(grant_entry));
                        grant_list.push(format!("{} TO {}", grant_entry, identity));
                    }
                }
                GrantObject::MaskingPolicy(policy_id) => {
                    let meta_api = UserApiProvider::instance().get_meta_store_client();
                    if let Some(policy_name) = meta_api
                        .get_data_mask_name_by_id(&ctx.get_tenant(), *policy_id)
                        .await
                        .map_err(meta_service_error)?
                    {
                        object_name.push(policy_name);
                        object_id.push(Some(policy_id.to_string()));
                        privileges.push(get_priv_str(grant_entry));
                        grant_list.push(format!("{} TO {}", grant_entry, identity));
                    }
                }
                GrantObject::RowAccessPolicy(policy_id) => {
                    let meta_api = UserApiProvider::instance().get_meta_store_client();
                    if let Some(policy_name) = meta_api
                        .get_row_access_policy_name_by_id(&ctx.get_tenant(), *policy_id)
                        .await
                        .map_err(meta_service_error)?
                    {
                        object_name.push(policy_name);
                        object_id.push(Some(policy_id.to_string()));
                        privileges.push(get_priv_str(grant_entry));
                        grant_list.push(format!("{} TO {}", grant_entry, identity));
                    }
                }
                GrantObject::Global => {
                    // grant all on *.* to a
                    object_name.push("*.*".to_string());
                    object_id.push(None);
                    privileges.push(get_priv_str(grant_entry));
                    grant_list.push(format!("{} TO {}", grant_entry, identity));
                }
            }
        }
    }

    // If roles contains account_admin, it means this role has all roles.
    // No need to display ownership.
    if !roles.contains(&BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()) {
        let roles = if expand_roles {
            roles
        } else if grant_type == "role" {
            vec![name.to_string()]
        } else {
            vec![]
        };
        // When roles is empty:
        // 1. show grants for user
        // 2. not expand roles
        // So no need to get ownerships.
        if !roles.is_empty() {
            let ownerships = user_api.role_api(&tenant).list_ownerships().await?;
            for ownership in ownerships {
                if roles.contains(&ownership.data.role) {
                    match ownership.data.object {
                        OwnershipObject::Database {
                            catalog_name,
                            db_id,
                        } => {
                            let privileges_str = "OWNERSHIP".to_string();
                            if let Some(dbs_id_priv) = catalog_db_ids.get_mut(&catalog_name) {
                                dbs_id_priv.push((db_id, privileges_str));
                            } else {
                                catalog_db_ids
                                    .insert(catalog_name.clone(), vec![(db_id, privileges_str)]);
                            }
                        }
                        OwnershipObject::Table {
                            catalog_name,
                            db_id,
                            table_id,
                        } => {
                            let privileges_str = "OWNERSHIP".to_string();
                            if let Some(tables_id_priv) = catalog_table_ids.get_mut(&catalog_name) {
                                tables_id_priv.push((db_id, table_id, privileges_str));
                            } else {
                                catalog_table_ids.insert(catalog_name.clone(), vec![(
                                    db_id,
                                    table_id,
                                    privileges_str,
                                )]);
                            }
                        }
                        OwnershipObject::Stage { name } => {
                            object_name.push(name.to_string());
                            object_id.push(None);
                            privileges.push("OWNERSHIP".to_string());
                            grant_list
                                .push(format!("GRANT OWNERSHIP ON STAGE {} TO {}", name, identity));
                        }
                        OwnershipObject::UDF { name } => {
                            object_name.push(name.to_string());
                            object_id.push(None);
                            privileges.push("OWNERSHIP".to_string());
                            grant_list
                                .push(format!("GRANT OWNERSHIP ON UDF {} TO {}", name, identity));
                        }
                        OwnershipObject::Warehouse { id } => {
                            if let Some(sw) = warehouses
                                .iter()
                                .filter_map(|w| {
                                    if let WarehouseInfo::SystemManaged(sw) = w {
                                        Some(sw)
                                    } else {
                                        None
                                    }
                                })
                                .find(|sw| sw.role_id == id)
                            {
                                object_name.push(sw.id.to_string());
                                object_id.push(Some(id.to_string()));
                                privileges.push("OWNERSHIP".to_string());
                                grant_list.push(format!(
                                    "GRANT OWNERSHIP ON WAREHOUSE {} TO {}",
                                    id, identity
                                ));
                            }
                        }
                        OwnershipObject::Connection { name } => {
                            object_name.push(name.to_string());
                            object_id.push(None);
                            privileges.push("OWNERSHIP".to_string());
                            grant_list.push(format!(
                                "GRANT OWNERSHIP ON CONNECTION {} TO {}",
                                name, identity
                            ));
                        }
                        OwnershipObject::Sequence { name } => {
                            object_name.push(name.to_string());
                            object_id.push(None);
                            privileges.push("OWNERSHIP".to_string());
                            grant_list.push(format!(
                                "GRANT OWNERSHIP ON SEQUENCE {} TO {}",
                                name, identity
                            ));
                        }
                        OwnershipObject::Procedure { procedure_id } => {
                            if let Some(p) = procedure_api
                                .get_procedure_name_by_id(procedure_id)
                                .await
                                .map_err(meta_service_error)?
                            {
                                object_name.push(p.to_string());
                                object_id.push(Some(procedure_id.to_string()));
                                privileges.push("OWNERSHIP".to_string());
                                grant_list.push(format!(
                                    "GRANT OWNERSHIP ON PROCEDURE {} TO {}",
                                    p, identity
                                ));
                            }
                        }
                        OwnershipObject::MaskingPolicy { policy_id } => {
                            let meta_api = UserApiProvider::instance().get_meta_store_client();
                            if let Some(policy_name) = meta_api
                                .get_data_mask_name_by_id(&ctx.get_tenant(), policy_id)
                                .await
                                .map_err(meta_service_error)?
                            {
                                object_name.push(policy_name.clone());
                                object_id.push(Some(policy_id.to_string()));
                                privileges.push("OWNERSHIP".to_string());
                                grant_list.push(format!(
                                    "GRANT OWNERSHIP ON MASKING POLICY {} TO {}",
                                    policy_name, identity
                                ));
                            }
                        }
                        OwnershipObject::RowAccessPolicy { policy_id } => {
                            let meta_api = UserApiProvider::instance().get_meta_store_client();
                            if let Some(policy_name) = meta_api
                                .get_row_access_policy_name_by_id(&ctx.get_tenant(), policy_id)
                                .await
                                .map_err(meta_service_error)?
                            {
                                object_name.push(policy_name.clone());
                                object_id.push(Some(policy_id.to_string()));
                                privileges.push("OWNERSHIP".to_string());
                                grant_list.push(format!(
                                    "GRANT OWNERSHIP ON ROW ACCESS POLICY {} TO {}",
                                    policy_name, identity
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    for (catalog_name, dbs_priv_id) in catalog_db_ids.into_iter() {
        let catalog = ctx.get_catalog(&catalog_name).await?;
        let db_id_set = dbs_priv_id
            .iter()
            .map(|res| res.0)
            .collect::<HashSet<u64>>();
        let mut db_ids = db_id_set.into_iter().collect::<Vec<u64>>();
        db_ids.sort();
        let db_names = catalog.mget_database_names_by_ids(&tenant, &db_ids).await?;
        let db_map = db_ids
            .into_iter()
            .zip(db_names.into_iter())
            .filter(|(_, db_name)| db_name.is_some())
            .map(|(db_id, db_name)| (db_id, db_name.unwrap()))
            .collect::<HashMap<_, _>>();
        for (db_id, privilege_str) in dbs_priv_id.into_iter() {
            if let Some(db_name) = db_map.get(&db_id) {
                let grant_str = format!(
                    "GRANT {} ON '{}'.'{}'.* TO {}",
                    privilege_str, catalog_name, db_name, identity
                );
                object_name.push(db_name.to_string());
                object_id.push(Some(db_id.to_string()));
                privileges.push(privilege_str);
                grant_list.push(grant_str);
            }
        }
    }

    for (catalog_name, tables_priv_id) in catalog_table_ids.into_iter() {
        let catalog = ctx.get_catalog(&catalog_name).await?;
        let db_id_set = tables_priv_id
            .iter()
            .map(|res| res.0)
            .collect::<HashSet<u64>>();
        let mut db_ids = db_id_set.into_iter().collect::<Vec<u64>>();
        db_ids.sort();
        let db_names = catalog.mget_database_names_by_ids(&tenant, &db_ids).await?;
        let db_map = db_ids
            .into_iter()
            .zip(db_names.into_iter())
            .filter(|(_, db_name)| db_name.is_some())
            .map(|(db_id, db_name)| (db_id, db_name.unwrap()))
            .collect::<HashMap<_, _>>();

        let table_id_set = tables_priv_id
            .iter()
            .map(|res| res.1)
            .collect::<HashSet<u64>>();
        let mut table_ids = table_id_set.into_iter().collect::<Vec<u64>>();
        table_ids.sort();
        let table_names = catalog
            .mget_table_names_by_ids(&tenant, &table_ids, false)
            .await?;
        let table_map = table_ids
            .into_iter()
            .zip(table_names.into_iter())
            .filter(|(_, table_name)| table_name.is_some())
            .map(|(table_id, table_name)| (table_id, table_name.unwrap()))
            .collect::<HashMap<_, _>>();

        for (db_id, table_id, privilege_str) in tables_priv_id.into_iter() {
            if let Some(db_name) = db_map.get(&db_id) {
                if let Some(table_name) = table_map.get(&table_id) {
                    let grant_str = format!(
                        "GRANT {} ON '{}'.'{}'.'{}' TO {}",
                        &privilege_str, catalog_name, db_name, table_name, identity
                    );
                    object_name.push(format!("{}.{}.{}", catalog_name, db_name, table_name));
                    object_id.push(Some(table_id.to_string()));
                    privileges.push(privilege_str);
                    grant_list.push(grant_str);
                }
            }
        }
    }

    let names: Vec<String> = vec![name; privileges.len()];
    let grant_tos: Vec<String> = vec![grant_to; privileges.len()];
    Ok(Some(DataBlock::new_from_columns(vec![
        StringType::from_data(privileges),
        StringType::from_data(object_name),
        StringType::from_opt_data(object_id),
        StringType::from_data(grant_tos),
        StringType::from_data(names),
        StringType::from_data(grant_list),
    ])))
}

async fn show_object_grant(
    ctx: Arc<dyn TableContext>,
    grant_type: &str,
    name: &str,
    catalog_name: &str,
    db_name: &str,
) -> Result<Option<DataBlock>> {
    let tenant = ctx.get_tenant();
    let user_api = UserApiProvider::instance();
    let roles = user_api.get_roles(&tenant).await?;
    let visibility_checker = ctx.get_visibility_checker(false, Object::All).await?;
    let current_user = ctx.get_current_user()?.identity().username;
    let (object, owner_object, object_id, object_name) = match grant_type {
        "table" => {
            let catalog = ctx.get_catalog(catalog_name).await?;
            let db_id = catalog
                .get_database(&tenant, db_name)
                .await?
                .get_db_info()
                .database_id
                .db_id;
            let table_id = catalog.get_table(&tenant, db_name, name).await?.get_id();
            if !visibility_checker.check_table_visibility(
                catalog_name,
                db_name,
                name,
                db_id,
                table_id,
            ) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: No privilege on table {} for user {}.",
                    name, current_user
                )));
            }
            (
                GrantObject::TableById(catalog_name.to_string(), db_id, table_id),
                OwnershipObject::Table {
                    catalog_name: catalog_name.to_string(),
                    db_id,
                    table_id,
                },
                Some(table_id.to_string()),
                name.to_string(),
            )
        }
        "database" => {
            // db_name is empty string, name is real database name
            let catalog = ctx.get_catalog(catalog_name).await?;
            let db_id = catalog
                .get_database(&tenant, name)
                .await?
                .get_db_info()
                .database_id
                .db_id;
            if !visibility_checker.check_database_visibility(catalog_name, name, db_id) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: No privilege on database {} for user {}.",
                    name, current_user
                )));
            }
            (
                GrantObject::DatabaseById(catalog_name.to_string(), db_id),
                OwnershipObject::Database {
                    catalog_name: catalog_name.to_string(),
                    db_id,
                },
                Some(db_id.to_string()),
                name.to_string(),
            )
        }
        "udf" => {
            if !visibility_checker.check_udf_visibility(name) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege USAGE is required on udf {} for user {}.",
                    name, current_user
                )));
            }
            (
                GrantObject::UDF(name.to_string()),
                OwnershipObject::UDF {
                    name: name.to_string(),
                },
                None,
                name.to_string(),
            )
        }
        "stage" => {
            if !visibility_checker.check_stage_visibility(name) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege READ is required on stage {} for user {}. Or no need to show the stage privilege",
                    name, current_user
                )));
            }
            (
                GrantObject::Stage(name.to_string()),
                OwnershipObject::Stage {
                    name: name.to_string(),
                },
                None,
                name.to_string(),
            )
        }
        "warehouse" => {
            let warehouse_mgr = GlobalInstance::get::<Arc<dyn ResourcesManagement>>();
            if !warehouse_mgr.support_forward_warehouse_request() {
                return Err(ErrorCode::InvalidArgument("The 'SHOW GRANTS ON <warehouse_name>' only supported for warehouses managed by the system. Please verify that you are using a system-managed warehouse".to_string()));
            }
            let warehouses = warehouse_mgr.list_warehouses().await?;
            let mut id = String::new();
            for w in warehouses {
                if let WarehouseInfo::SystemManaged(rw) = w {
                    if rw.id == name {
                        id = rw.role_id.to_string();
                        break;
                    }
                }
            }
            if !visibility_checker.check_warehouse_visibility(&id) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: No privilege on warehouse {} for user {}.",
                    name, current_user
                )));
            }
            (
                GrantObject::Warehouse(id.to_string()),
                OwnershipObject::Warehouse { id: id.to_string() },
                Some(id),
                name.to_string(),
            )
        }
        "connection" => {
            if !visibility_checker.check_connection_visibility(name) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege ACCESS CONNECTION is required on connection {} for user {}",
                    name, current_user
                )));
            }
            (
                GrantObject::Connection(name.to_string()),
                OwnershipObject::Connection {
                    name: name.to_string(),
                },
                None,
                name.to_string(),
            )
        }
        "sequence" => {
            if !visibility_checker.check_seq_visibility(name) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege ACCESS SEQUENCE is required on sequence {} for user {}",
                    name, current_user
                )));
            }
            (
                GrantObject::Sequence(name.to_string()),
                OwnershipObject::Sequence {
                    name: name.to_string(),
                },
                None,
                name.to_string(),
            )
        }
        "procedure" => {
            let procedure_id = name.parse::<u64>()?;
            if !visibility_checker.check_procedure_visibility(&procedure_id) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege ACCESS PROCEDURE is required on procedure {} for user {}",
                    name, current_user
                )));
            }

            if let Some(p) = UserApiProvider::instance()
                .procedure_api(&tenant)
                .get_procedure_name_by_id(procedure_id)
                .await
                .map_err(meta_service_error)?
            {
                (
                    GrantObject::Procedure(procedure_id),
                    OwnershipObject::Procedure { procedure_id },
                    Some(procedure_id.to_string()),
                    p.to_string(),
                )
            } else {
                return Err(ErrorCode::UnknownProcedure(format!(
                    "Unknown procedure id: {}",
                    procedure_id
                )));
            }
        }
        "masking_policy" => {
            let policy_id = name.parse::<u64>()?;
            if !visibility_checker.check_masking_policy_visibility(&policy_id) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege APPLY MASKING POLICY or OWNERSHIP is required on masking policy {} for user {}",
                    name, current_user
                )));
            }
            let meta_api = UserApiProvider::instance().get_meta_store_client();
            if let Some(policy_name) = meta_api
                .get_data_mask_name_by_id(&ctx.get_tenant(), policy_id)
                .await
                .map_err(meta_service_error)?
            {
                (
                    GrantObject::MaskingPolicy(policy_id),
                    OwnershipObject::MaskingPolicy { policy_id },
                    Some(policy_id.to_string()),
                    policy_name,
                )
            } else {
                (
                    // Already get the policy id but old version policy does not have key DataMaskIdToNameIdent
                    // directly return policy id as name. Extremely low probability event
                    GrantObject::MaskingPolicy(policy_id),
                    OwnershipObject::MaskingPolicy { policy_id },
                    Some(policy_id.to_string()),
                    policy_id.to_string(),
                )
            }
        }
        "row_access_policy" => {
            let policy_id = name.parse::<u64>()?;
            if !visibility_checker.check_row_access_policy_visibility(&policy_id) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege APPLY ROW ACCESS POLICY or OWNERSHIP is required on row access policy {} for user {}",
                    name, current_user
                )));
            }
            let meta_api = UserApiProvider::instance().get_meta_store_client();
            if let Some(policy_name) = meta_api
                .get_row_access_policy_name_by_id(&ctx.get_tenant(), policy_id)
                .await
                .map_err(meta_service_error)?
            {
                (
                    GrantObject::RowAccessPolicy(policy_id),
                    OwnershipObject::RowAccessPolicy { policy_id },
                    Some(policy_id.to_string()),
                    policy_name,
                )
            } else {
                (
                    GrantObject::RowAccessPolicy(policy_id),
                    OwnershipObject::RowAccessPolicy { policy_id },
                    Some(policy_id.to_string()),
                    policy_id.to_string(),
                )
            }
        }
        _ => {
            return Err(ErrorCode::InvalidArgument(format!(
                "Expected 'table|database|udf|stage|warehouse|connection|sequence|procedure|masking_policy|row_access_policy', but got {:?}",
                grant_type
            )));
        }
    };

    let mut names = vec![];
    let mut privileges = vec![];
    for role in roles {
        for entry in role.grants.entries() {
            if entry.matches_entry(&object) {
                let privilege: UserPrivilegeSet = (*entry.privileges()).into();
                privileges.push(privilege.to_string());
                names.push(role.name.to_string());
            }
        }
    }

    if let Some(ownership) = user_api
        .role_api(&tenant)
        .get_ownership(&owner_object)
        .await?
    {
        privileges.push("OWNERSHIP".to_string());
        names.push(ownership.role);
    }

    let object_ids = vec![object_id; privileges.len()];
    let object_names = vec![object_name.as_str(); privileges.len()];
    let grant_tos: Vec<String> = vec!["ROLE".to_string(); privileges.len()];
    let grant_list = vec!["".to_string(); privileges.len()];
    Ok(Some(DataBlock::new_from_columns(vec![
        StringType::from_data(privileges),
        StringType::from_data(object_names),
        StringType::from_opt_data(object_ids),
        StringType::from_data(grant_tos),
        StringType::from_data(names),
        StringType::from_data(grant_list),
    ])))
}

impl TableFunction for ShowGrants {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
