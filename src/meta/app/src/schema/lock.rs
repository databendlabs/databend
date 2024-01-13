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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::Result;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_kvapi::kvapi::KeyError;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct LockMeta {
    pub user: String,
    pub node: String,
    pub query_id: String,

    pub created_on: DateTime<Utc>,
    pub acquired_on: Option<DateTime<Utc>>,
    pub lock_type: LockType,
    pub extra_info: BTreeMap<String, String>,
}

#[derive(
    serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, num_derive::FromPrimitive,
)]
pub enum LockType {
    TABLE = 0,
}

impl LockType {
    pub fn revision_from_str(&self, s: &str) -> Result<u64, KeyError> {
        match self {
            LockType::TABLE => {
                let key = TableLockKey::from_str_key(s)?;
                Ok(key.revision)
            }
        }
    }

    pub fn key_from_str(&self, s: &str) -> Result<LockKey, KeyError> {
        match self {
            LockType::TABLE => {
                let key = TableLockKey::from_str_key(s)?;
                Ok(LockKey::Table {
                    table_id: key.table_id,
                })
            }
        }
    }
}

impl Display for LockType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LockType::TABLE => write!(f, "TABLE"),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum LockKey {
    /// table-level lock.
    Table { table_id: u64 },
}

impl LockKey {
    pub fn get_table_id(&self) -> u64 {
        match self {
            LockKey::Table { table_id } => *table_id,
        }
    }

    pub fn get_extra_info(&self) -> BTreeMap<String, String> {
        match self {
            LockKey::Table { .. } => BTreeMap::new(),
        }
    }

    pub fn lock_type(&self) -> LockType {
        match self {
            LockKey::Table { .. } => LockType::TABLE,
        }
    }

    pub fn gen_prefix(&self) -> String {
        match self {
            LockKey::Table { table_id } => format!("{}/{}", TableLockKey::PREFIX, table_id),
        }
    }

    pub fn gen_key(&self, revision: u64) -> impl Key {
        match self {
            LockKey::Table { table_id } => TableLockKey {
                table_id: *table_id,
                revision,
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LockInfo {
    pub key: LockKey,
    pub revision: u64,
    pub meta: LockMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ListLocksReq {
    pub prefixes: Vec<String>,
}

impl ListLocksReq {
    pub fn create() -> Self {
        let prefixes = vec![TableLockKey::PREFIX.to_string()];
        Self { prefixes }
    }

    pub fn create_with_table_ids(table_ids: Vec<u64>) -> Self {
        let mut prefixes = Vec::new();
        for table_id in table_ids {
            prefixes.push(format!("{}/{}", TableLockKey::PREFIX, table_id));
        }
        Self { prefixes }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ListLockRevReq {
    pub lock_key: LockKey,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateLockRevReq {
    pub lock_key: LockKey,
    pub expire_secs: u64,
    pub user: String,
    pub node: String,
    pub query_id: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateLockRevReply {
    pub revision: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ExtendLockRevReq {
    pub lock_key: LockKey,
    pub expire_secs: u64,
    pub revision: u64,
    pub acquire_lock: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeleteLockRevReq {
    pub lock_key: LockKey,
    pub revision: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TableLockKey {
    pub table_id: u64,
    pub revision: u64,
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::schema::TableLockKey;
    use crate::schema::PREFIX_TABLE_LOCK;

    /// __fd_table_lock/table_id/revision -> LockMeta
    impl kvapi::Key for TableLockKey {
        const PREFIX: &'static str = PREFIX_TABLE_LOCK;

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_u64(self.table_id)
                .push_u64(self.revision)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let table_id = p.next_u64()?;
            let revision = p.next_u64()?;
            p.done()?;

            Ok(TableLockKey { table_id, revision })
        }
    }
}
