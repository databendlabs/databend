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

use std::fmt::Display;
use std::fmt::Formatter;

use chrono::DateTime;
use chrono::Utc;
use common_exception::Result;
use common_meta_kvapi::kvapi::Key;
use common_meta_kvapi::kvapi::KeyError;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct LockMeta {
    pub level: LockLevel,
    pub user: String,
    pub node: String,
    pub session_id: String,

    pub created_on: DateTime<Utc>,
    pub acquired_on: Option<DateTime<Utc>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum LockLevel {
    /// table-level lock.
    Table,
}

impl LockLevel {
    pub fn prefix(&self, table_id: u64) -> String {
        match self {
            LockLevel::Table => format!("{}/{}", TableLockKey::PREFIX, table_id),
        }
    }

    pub fn revision(&self, s: &str) -> Result<u64, KeyError> {
        match self {
            LockLevel::Table => {
                let key = TableLockKey::from_str_key(s)?;
                Ok(key.revision)
            }
        }
    }

    pub fn gen_key(&self, table_id: u64, revision: u64) -> impl Key {
        match self {
            LockLevel::Table => TableLockKey { table_id, revision },
        }
    }
}

impl Display for LockLevel {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            LockLevel::Table => write!(f, "Table"),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ListLockRevReq {
    pub table_id: u64,
    pub level: LockLevel,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateLockRevReq {
    pub table_id: u64,
    pub expire_at: u64,

    pub level: LockLevel,
    pub user: String,
    pub node: String,
    pub session_id: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateLockRevReply {
    pub revision: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ExtendLockRevReq {
    pub table_id: u64,
    pub expire_at: u64,
    pub revision: u64,
    pub level: LockLevel,
    pub acquire_lock: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DeleteLockRevReq {
    pub table_id: u64,
    pub revision: u64,
    pub level: LockLevel,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TableLockKey {
    pub table_id: u64,
    pub revision: u64,
}

mod kvapi_key_impl {
    use common_meta_kvapi::kvapi;

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
