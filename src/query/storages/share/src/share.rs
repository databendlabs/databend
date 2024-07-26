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
use databend_common_meta_app::share::ShareObject;
use databend_common_meta_app::share::ShareSpec;
use databend_common_meta_app::share::ShareTableSpec;
use log::error;
use opendal::Operator;

const SHARE_CONFIG_VERSION: u16 = 2;
const SHARE_CONFIG_PREFIX: &str = "_share_config";

fn share_dir_prefix() -> String {
    format!("{}/V{}", SHARE_CONFIG_PREFIX, SHARE_CONFIG_VERSION)
}

pub fn get_share_dir(tenant: &str, share_name: &str) -> String {
    format!("{}/{}/{}", share_dir_prefix(), tenant, share_name)
}

pub fn get_share_database_dir(tenant: &str, share_name: &str, db_id: u64) -> String {
    format!("{}/{}/{}/{}", share_dir_prefix(), tenant, share_name, db_id)
}

pub fn get_share_spec_location(tenant: &str, share_name: &str) -> String {
    format!(
        "{}/{}/{}/share_specs.json",
        share_dir_prefix(),
        tenant,
        share_name
    )
}

pub fn share_table_info_location(
    tenant: &str,
    share_name: &str,
    db_id: u64,
    table_id: u64,
) -> String {
    format!(
        "{}/{}/{}/{}/{}_table_info.json",
        share_dir_prefix(),
        tenant,
        share_name,
        db_id,
        table_id
    )
}

#[async_backtrace::framed]
pub async fn save_share_spec(
    tenant: &str,
    operator: Operator,
    share_specs: &[ShareSpec],
) -> Result<()> {
    for share_spec in share_specs {
        let share_name = &share_spec.name;
        let location = get_share_spec_location(tenant, share_name);
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
    share_name: &str,
    db_id: u64,
    share_table_id: u64,
) -> Result<()> {
    let location = share_table_info_location(tenant, share_name, db_id, share_table_id);

    operator.delete(&location).await?;

    Ok(())
}

#[async_backtrace::framed]
pub async fn remove_share_table_object(
    tenant: &str,
    operator: Operator,
    share_name: &str,
    revoke_share_object: &[ShareObject],
) -> Result<()> {
    for share_object in revoke_share_object {
        match share_object {
            ShareObject::Table(_share_table, db_id, table_id) => {
                let location = share_table_info_location(tenant, share_name, *db_id, *table_id);

                operator.delete(&location).await?;
            }
            ShareObject::Database(_db_name, db_id) => {
                let dir = get_share_database_dir(tenant, share_name, *db_id);
                operator.remove_all(&dir).await?;
            }
        }
    }

    Ok(())
}

#[async_backtrace::framed]
pub async fn update_share_table_info(
    tenant: &str,
    operator: Operator,
    share_name_vec: &[String],
    db_id: u64,
    share_table_info: &TableInfo,
) -> Result<()> {
    let data = serde_json::to_string(share_table_info)?;
    for share_name in share_name_vec {
        let location =
            share_table_info_location(tenant, share_name, db_id, share_table_info.ident.table_id);

        if let Err(e) = operator.write(&location, data.clone()).await {
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
pub async fn remove_share_db_dir(
    tenant: &str,
    operator: Operator,
    db_id: u64,
    share_specs: &[ShareSpec],
) -> Result<()> {
    for share_spec in share_specs {
        let dir = get_share_database_dir(tenant, &share_spec.name, db_id);
        operator.remove_all(&dir).await?;
    }

    Ok(())
}

mod ext {
    use databend_storages_common_table_meta::table::database_storage_prefix;
    use databend_storages_common_table_meta::table::table_storage_prefix;

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
        use_database: Option<WithLocation<ShareDatabaseSpec>>,
        reference_database: Vec<WithLocation<ShareDatabaseSpec>>,
        tables: Vec<WithLocation<ShareTableSpec>>,
        reference_tables: Vec<WithLocation<ShareTableSpec>>,
        tenants: Vec<String>,
        comment: Option<String>,
        create_on: DateTime<Utc>,
    }

    impl ShareSpecExt {
        pub fn from_share_spec(spec: ShareSpec, operator: &Operator) -> Self {
            Self {
                name: spec.name,
                share_id: spec.share_id,
                version: spec.version,
                use_database: spec.use_database.map(|db_spec| WithLocation {
                    location: shared_database_prefix(operator, db_spec.id),
                    t: db_spec,
                }),
                reference_database: spec
                    .reference_database
                    .into_iter()
                    .map(|db_spec| WithLocation {
                        location: shared_database_prefix(operator, db_spec.id),
                        t: db_spec,
                    })
                    .collect(),
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
                reference_tables: spec
                    .reference_tables
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
                comment: spec.comment.clone(),
                create_on: spec.create_on,
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
