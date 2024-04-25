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

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::KeyCodec;

/// [`OwnershipObject`] is used to maintain the grant object that support rename by id. Using ID over name
/// have many benefits, it can avoid lost privileges after the object get renamed.
/// But Stage and UDF do not support the concept of renaming and do not have ids, so names can be used.
///
/// It could be a tenant's database, a tenant's table etc.
/// It is in form of `__fd_object_owners/<tenant>/<object>`.
/// where `<object>` could be:
/// - `database-by-id/<db_id>`
/// - `database-by-catalog-id/<catalog>/<db_id>`
/// - `table-by-id/<table_id>`
/// - `table-by-catalog-id/<catalog>/<table_id>`
/// - `stage-by-name/<stage_name>`
/// - `udf-by-name/<udf_name>`
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub enum OwnershipObject {
    /// used on the fuse databases
    Database {
        catalog_name: String,
        db_id: u64,
    },

    /// used on the fuse tables
    Table {
        catalog_name: String,
        db_id: u64,
        table_id: u64,
    },

    Stage {
        name: String,
    },

    UDF {
        name: String,
    },
}

impl OwnershipObject {
    // # Legacy issue: Catalog is not encoded into key.
    //
    // But at that time, only `"default"` catalog is used.
    // Thus we follow the same rule, if catalog is `"default"`, we don't encode it.
    //
    // This issue is introduced in https://github.com/drmingdrmer/databend/blob/7681763dc54306e55b5e0326af0510292d244be3/src/query/management/src/role/role_mgr.rs#L86
    const DEFAULT_CATALOG: &'static str = "default";
}

impl KeyCodec for OwnershipObject {
    fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
        match self {
            OwnershipObject::Database {
                catalog_name,
                db_id,
            } => {
                // Legacy issue: Catalog is not encoded into key.
                if catalog_name == Self::DEFAULT_CATALOG {
                    b.push_raw("database-by-id").push_u64(*db_id)
                } else {
                    b.push_raw("database-by-catalog-id")
                        .push_str(catalog_name)
                        .push_u64(*db_id)
                }
            }
            OwnershipObject::Table {
                catalog_name,
                db_id,
                table_id,
            } => {
                // TODO(flaneur): db_id is not encoded into key. Thus such key can not be decoded.
                let _ = db_id;

                // Legacy issue: Catalog is not encoded into key.
                if catalog_name == Self::DEFAULT_CATALOG {
                    b.push_raw("table-by-id").push_u64(*table_id)
                } else {
                    b.push_raw("table-by-catalog-id")
                        .push_str(catalog_name)
                        .push_u64(*table_id)
                }
            }
            OwnershipObject::Stage { name } => b.push_raw("stage-by-name").push_str(name),
            OwnershipObject::UDF { name } => b.push_raw("udf-by-name").push_str(name),
        }
    }

    fn decode_key(p: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
        let q = p.next_raw()?;
        match q {
            "database-by-id" => {
                let db_id = p.next_u64()?;
                Ok(OwnershipObject::Database {
                    catalog_name: Self::DEFAULT_CATALOG.to_string(),
                    db_id,
                })
            }
            "database-by-catalog-id" => {
                let catalog_name = p.next_str()?;
                let db_id = p.next_u64()?;
                Ok(OwnershipObject::Database {
                    catalog_name,
                    db_id,
                })
            }
            "table-by-id" => {
                let table_id = p.next_u64()?;
                Ok(OwnershipObject::Table {
                    catalog_name: Self::DEFAULT_CATALOG.to_string(),
                    db_id: 0,
                    table_id,
                })
            }
            "table-by-catalog-id" => {
                let catalog_name = p.next_str()?;
                let table_id = p.next_u64()?;
                Ok(OwnershipObject::Table {
                    catalog_name,
                    db_id: 0, // string key does not contain db_id
                    table_id,
                })
            }
            "stage-by-name" => {
                let name = p.next_str()?;
                Ok(OwnershipObject::Stage { name })
            }
            "udf-by-name" => {
                let name = p.next_str()?;
                Ok(OwnershipObject::UDF { name })
            }
            _ => Err(kvapi::KeyError::InvalidSegment {
                i: p.index(),
                expect: "database-by-id|database-by-catalog-id|table-by-id|table-by-catalog-id|stage-by-name|udf-by-name"
                    .to_string(),
                got: q.to_string(),
            }),
        }
    }
}
