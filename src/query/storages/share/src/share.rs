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

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::share::ShareDatabaseSpec;
use databend_common_meta_app::share::ShareSpec;
use databend_common_meta_app::share::ShareTableInfoMap;
use databend_common_meta_app::share::ShareTableSpec;
use log::error;
use opendal::Operator;

const SHARE_CONFIG_PREFIX: &str = "_share_config";

pub fn get_share_dir(tenant: &str, share_name: &str) -> String {
    format!("{}/{}/{}", SHARE_CONFIG_PREFIX, tenant, share_name)
}

pub fn get_share_spec_location(tenant: &str, share_name: &str) -> String {
    format!(
        "{}/{}/{}/share_specs.json",
        SHARE_CONFIG_PREFIX, tenant, share_name
    )
}

pub fn share_table_info_location(tenant: &str, share_name: &str) -> String {
    format!(
        "{}/{}/{}_table_info.json",
        SHARE_CONFIG_PREFIX, tenant, share_name
    )
}

pub fn new_share_table_info_location(tenant: &str, share_name: &str, table_name: &str) -> String {
    format!(
        "{}/{}/{}/{}_table_info.json",
        SHARE_CONFIG_PREFIX, tenant, share_name, table_name
    )
}

#[async_backtrace::framed]
pub async fn save_share_table_info(
    tenant: &str,
    operator: Operator,
    share_table_info: &[ShareTableInfoMap],
) -> Result<()> {
    for (share_name, share_table_info) in share_table_info {
        let location = share_table_info_location(tenant, share_name);
        match share_table_info {
            Some(table_info_map) => {
                operator
                    .write(&location, serde_json::to_vec(table_info_map)?)
                    .await?;
            }
            None => {
                operator.delete(&location).await?;
            }
        }
    }

    Ok(())
}

#[async_backtrace::framed]
pub async fn save_share_spec(
    tenant: &str,
    operator: Operator,
    share_specs: &[ShareSpec],
) -> Result<()> {
    for share_spec in share_specs {
        let share_name = &share_spec.name;
        let location = get_share_spec_location(tenant, &share_name);
        let share_spec_ext = ext::ShareSpecExt::from_share_spec(share_spec.clone(), &operator);
        let data = serde_json::to_string(&share_spec_ext)?;
        operator.write(&location, data).await?;
    }

    Ok(())
}

#[async_backtrace::framed]
pub async fn remove_share_table_info(
    tenant: &str,
    operator: Operator,
    share_name: &String,
    revoke_share_table: &[String],
) -> Result<()> {
    for share_table in revoke_share_table {
        let location = new_share_table_info_location(tenant, share_name, share_table);

        operator.delete(&location).await?;
    }

    Ok(())
}

#[async_backtrace::framed]
pub async fn update_share_table_info(
    tenant: &str,
    operator: Operator,
    share_name_vec: &[String],
    share_table_info: &TableInfo,
) -> Result<()> {
    for share_name in share_name_vec {
        let location = new_share_table_info_location(tenant, share_name, &share_table_info.name);

        if let Err(e) = operator
            .write(&location, serde_json::to_string(share_table_info)?)
            .await
        {
            error!(
                "update_share_table_info of share {} table {} error: {:?}",
                share_name, share_table_info.name, e
            );
        }
    }

    Ok(())
}

#[async_backtrace::framed]
pub async fn remove_share_dir(
    tenant: &str,
    operator: Operator,
    share_specs: &[ShareSpec],
) -> Result<()> {
    for share_spec in share_specs {
        let dir = get_share_dir(tenant, &share_spec.name);
        operator.remove_all(&dir).await?;
    }

    Ok(())
}

#[async_backtrace::framed]
pub async fn save_share_spec_old(
    tenant: &str,
    operator: Operator,
    spec_vec: Option<Vec<ShareSpec>>,
    share_table_info: Option<Vec<ShareTableInfoMap>>,
) -> Result<()> {
    println!("share spec_vec:{:?}\n", spec_vec);
    println!("share_table_info:{:?}\n", share_table_info);
    if let Some(share_spec) = spec_vec {
        for spec in share_spec {
            let share_name = spec.name.clone();
            let location = get_share_spec_location(tenant, &share_name);
            let share_spec_ext = ext::ShareSpecExt::from_share_spec(spec, &operator);
            let data = serde_json::to_string(&share_spec_ext)?;
            println!("data: {:?}", data);
            operator.write(&location, data).await?;
        }
    }

    // save share table info
    if let Some(share_table_info) = share_table_info {
        save_share_table_info(tenant, operator, &share_table_info).await?
    }

    Ok(())
}

mod ext {
    use databend_common_meta_app::share::ShareGrantObjectPrivilege;
    use databend_storages_common_table_meta::table::database_storage_prefix;
    use databend_storages_common_table_meta::table::table_storage_prefix;
    use enumflags2::BitFlags;

    use super::*;

    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
    struct WithLocation<T> {
        location: String,
        #[serde(flatten)]
        t: T,
    }

    /// An extended form of [ShareSpec], which decorates [ShareDatabaseSpec] and [ShareTableSpec]
    /// with location
    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
    pub(super) struct ShareSpecExt {
        name: String,
        share_id: u64,
        version: u64,
        database: Option<WithLocation<ShareDatabaseSpec>>,
        tables: Vec<WithLocation<ShareTableSpec>>,
        tenants: Vec<String>,
        db_privileges: Option<BitFlags<ShareGrantObjectPrivilege>>,
        comment: Option<String>,
        share_on: Option<DateTime<Utc>>,
    }

    impl ShareSpecExt {
        pub fn from_share_spec(spec: ShareSpec, operator: &Operator) -> Self {
            Self {
                name: spec.name,
                share_id: spec.share_id,
                version: spec.version,
                database: spec.database.map(|db_spec| WithLocation {
                    location: shared_database_prefix(operator, db_spec.id),
                    t: db_spec,
                }),
                tables: spec
                    .tables
                    .into_iter()
                    .map(|tbl_spec| WithLocation {
                        location: shared_table_prefix(
                            operator,
                            tbl_spec.database_id,
                            tbl_spec.table_id,
                        ),
                        t: tbl_spec,
                    })
                    .collect(),
                tenants: spec.tenants,
                db_privileges: spec.db_privileges,
                comment: spec.comment.clone(),
                share_on: spec.share_on,
            }
        }
    }

    /// Returns prefix path which covers all the data of give table.
    /// something like "query-storage-bd5efc6/tnc7yee14/501248/501263/", where
    ///   - "/query-storage-bd5efc6/tnc7yee14/" is the storage prefix
    ///   - "501248/" is the database id suffixed with '/'
    ///   - "501263/" is the table id  suffixed with '/'
    fn shared_table_prefix(operator: &Operator, database_id: u64, table_id: u64) -> String {
        let operator_meta_data = operator.info();
        let storage_prefix = operator_meta_data.root();
        let table_storage_prefix = table_storage_prefix(database_id, table_id);
        // storage_prefix has suffix character '/'
        format!("{}{}/", storage_prefix, table_storage_prefix)
    }

    /// Returns prefix path which covers all the data of give database.
    /// something like "query-storage-bd5efc6/tnc7yee14/501248/", where
    ///   - "/query-storage-bd5efc6/tnc7yee14/" is the storage prefix
    ///   - "501248/" is the database id suffixed with '/'
    fn shared_database_prefix(operator: &Operator, database_id: u64) -> String {
        let operator_meta_data = operator.info();
        let storage_prefix = operator_meta_data.root();
        let database_storage_prefix = database_storage_prefix(database_id);
        // storage_prefix has suffix character '/'
        format!("{}{}/", storage_prefix, database_storage_prefix)
    }
}
