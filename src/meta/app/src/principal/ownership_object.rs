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

use std::fmt;

use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::KeyCodec;

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
/// - `procedure-by-id/<procedure_id>`
/// - `masking-policy-by-id/<policy_id>`
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

    Warehouse {
        id: String,
    },

    Connection {
        name: String,
    },

    Sequence {
        name: String,
    },

    Procedure {
        procedure_id: u64,
    },

    MaskingPolicy {
        policy_id: u64,
    },

    RowAccessPolicy {
        policy_id: u64,
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

impl fmt::Display for OwnershipObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        match self {
            OwnershipObject::Database {
                catalog_name,
                db_id,
            } => write!(f, "'{}'.'{}'.*", catalog_name, db_id),
            OwnershipObject::Table {
                catalog_name,
                db_id,
                table_id,
            } => {
                write!(f, "'{}'.'{}'.'{}'", catalog_name, db_id, table_id)
            }
            OwnershipObject::UDF { name } => write!(f, "UDF {name}"),
            OwnershipObject::Stage { name } => write!(f, "STAGE {name}"),
            OwnershipObject::Warehouse { id } => write!(f, "WAREHOUSE {id}"),
            OwnershipObject::Connection { name } => write!(f, "CONNECTION {name}"),
            OwnershipObject::Sequence { name } => write!(f, "SEQUENCE {name}"),
            OwnershipObject::Procedure { procedure_id } => write!(f, "PROCEDURE {procedure_id}"),
            OwnershipObject::MaskingPolicy { policy_id } => {
                write!(f, "MASKING POLICY {policy_id}")
            }
            OwnershipObject::RowAccessPolicy { policy_id } => {
                write!(f, "ROW ACCESS POLICY {policy_id}")
            }
        }
    }
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
            OwnershipObject::Warehouse { id } => b.push_raw("warehouse-by-id").push_str(id),
            OwnershipObject::Connection { name } => b.push_raw("connection-by-name").push_str(name),
            OwnershipObject::Sequence { name } => b.push_raw("sequence-by-name").push_str(name),
            OwnershipObject::Procedure { procedure_id } => {
                b.push_raw("procedure-by-id").push_u64(*procedure_id)
            }
            OwnershipObject::MaskingPolicy { policy_id } => {
                b.push_raw("masking-policy-by-id").push_u64(*policy_id)
            }
            OwnershipObject::RowAccessPolicy { policy_id } => {
                b.push_raw("row-access-policy-by-id").push_u64(*policy_id)
            }
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
            "warehouse-by-id" => {
                let id = p.next_str()?;
                Ok(OwnershipObject::Warehouse { id })
            }
            "connection-by-name" => {
                let name = p.next_str()?;
                Ok(OwnershipObject::Connection { name })
            }
            "sequence-by-name" => {
                let name = p.next_str()?;
                Ok(OwnershipObject::Sequence { name })
            }
            "procedure-by-id" => {
                let procedure_id = p.next_u64()?;
                Ok(OwnershipObject::Procedure { procedure_id })
            }
            "masking-policy-by-id" => {
                let policy_id = p.next_u64()?;
                Ok(OwnershipObject::MaskingPolicy { policy_id })
            }
            "row-access-policy-by-id" => {
                let policy_id = p.next_u64()?;
                Ok(OwnershipObject::RowAccessPolicy { policy_id })
            }
            _ => Err(kvapi::KeyError::InvalidSegment {
                i: p.index(),
                expect: "database-by-id|database-by-catalog-id|table-by-id|table-by-catalog-id|stage-by-name|udf-by-name|warehouse-by-id|connection-by-name|masking-policy-by-id|row-access-policy-by-id"
                    .to_string(),
                got: q.to_string(),
            }),
        }
    }
}
