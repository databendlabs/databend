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

use std::collections::BTreeMap;

use common_exception::Result;
use common_meta_app::share::ShareDatabaseSpec;
use common_meta_app::share::ShareSpec;
use common_meta_app::share::ShareTableSpec;
use opendal::Operator;

pub const SHARE_CONFIG_PREFIX: &str = "_share_config";

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareSpecVec {
    share_specs: BTreeMap<u64, ext::ShareSpecExt>,
}

pub async fn save_share_spec(
    tenant: String,
    share_id: u64,
    operator: Operator,
    spec: Option<ShareSpec>,
) -> Result<()> {
    let location = format!("{}/{}/share_specs.json", SHARE_CONFIG_PREFIX, tenant);

    if let Some(spec) = spec {
        // @lichuang
        // The same confusion (https://github.com/datafuselabs/databend/pull/7430/files#r961381306) here:
        // It seems that spec_vec is "not used", the removal operation have no observable side effects.
        let data = operator.object(&location).read().await?;
        let mut spec_vec: ShareSpecVec = serde_json::from_slice(&data)?;
        spec_vec.share_specs.remove(&share_id);

        let share_spec_ext = ext::ShareSpecExt::from_share_spec(spec, &operator);
        operator
            .object(&location)
            .write(serde_json::to_vec(&share_spec_ext)?)
            .await?;
    } else {
        operator.object(&location).delete().await?;
    }

    Ok(())
}

mod ext {
    use common_storages_util::table_storage_prefix::database_storage_prefix;
    use common_storages_util::table_storage_prefix::table_storage_prefix;

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
        database: Option<WithLocation<ShareDatabaseSpec>>,
        tables: Vec<WithLocation<ShareTableSpec>>,
        tenants: Vec<String>,
    }

    impl ShareSpecExt {
        pub fn from_share_spec(spec: ShareSpec, operator: &Operator) -> Self {
            Self {
                name: spec.name,
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
            }
        }
    }

    /// Returns prefix path which covers all the data of give table.
    /// something like "query-storage-bd5efc6/tnc7yee14/501248/501263/", where
    ///   - "/query-storage-bd5efc6/tnc7yee14/" is the storage prefix
    ///   - "501248/" is the database id suffixed with '/'
    ///   - "501263/" is the table id  suffixed with '/'
    fn shared_table_prefix(operator: &Operator, database_id: u64, table_id: u64) -> String {
        let operator_meta_data = operator.metadata();
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
        let operator_meta_data = operator.metadata();
        let storage_prefix = operator_meta_data.root();
        let database_storage_prefix = database_storage_prefix(database_id);
        // storage_prefix has suffix character '/'
        format!("{}{}/", storage_prefix, database_storage_prefix)
    }

    #[cfg(test)]
    mod tests {

        use opendal::services::fs;

        use super::*;

        #[test]
        fn test_serialize_share_spec_ext() -> Result<()> {
            let share_spec = ShareSpec {
                name: "test_share_name".to_string(),
                database: Some(ShareDatabaseSpec {
                    name: "share_database".to_string(),
                    id: 1,
                }),
                tables: vec![ShareTableSpec {
                    name: "share_table".to_string(),
                    database_id: 1,
                    table_id: 1,
                    version: 1,
                    presigned_url_timeout: "100s".to_string(),
                }],
                tenants: vec!["test_tenant".to_owned()],
            };
            let tmp_dir = tempfile::tempdir()?;
            let test_root = tmp_dir.path().join("test_cluster_id/test_tenant_id");
            let test_root_str = test_root.to_str().unwrap();
            let operator = {
                let mut builder = fs::Builder::default();
                builder.root(test_root_str);
                Operator::new(builder.build()?)
            };

            let share_spec_ext = ShareSpecExt::from_share_spec(share_spec, &operator);
            let spec_json_value = serde_json::to_value(&share_spec_ext).unwrap();

            use serde_json::json;

            let expected = json!({
              "name": "test_share_name",
              "database": {
                "location": format!("{}/1/", test_root_str),
                "name": "share_database",
                "id": 1
              },
              "tables": [
                {
                  "location": format!("{}/1/1/", test_root_str),
                  "name": "share_table",
                  "database_id": 1,
                  "table_id": 1,
                  "version": 1,
                  "presigned_url_timeout": "100s"
                }
              ],
              "tenants": [
                "test_tenant"
              ]
            });

            assert_eq!(expected, spec_json_value);
            Ok(())
        }
    }
}
