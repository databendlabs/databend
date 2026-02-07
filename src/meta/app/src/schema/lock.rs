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
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use databend_meta_kvapi::kvapi::DirName;

use crate::schema::TableLockIdent;
use crate::tenant::Tenant;

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

impl Display for LockType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            LockType::TABLE => write!(f, "TABLE"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockKey {
    /// table-level lock.
    Table { tenant: Tenant, table_id: u64 },
}

impl LockKey {
    pub fn get_table_id(&self) -> u64 {
        match self {
            LockKey::Table { table_id, .. } => *table_id,
        }
    }

    pub fn get_tenant(&self) -> &Tenant {
        match self {
            LockKey::Table { tenant, .. } => tenant,
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

    pub fn gen_prefix(&self) -> DirName<TableLockIdent> {
        match self {
            LockKey::Table { tenant, table_id } => {
                DirName::new(TableLockIdent::new(tenant.clone(), *table_id, 0))
            }
        }
    }

    pub fn gen_key(&self, revision: u64) -> TableLockIdent {
        match self {
            LockKey::Table { tenant, table_id } => TableLockIdent::new(tenant, *table_id, revision),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LockInfo {
    pub table_id: u64,
    pub revision: u64,
    pub meta: LockMeta,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListLocksReq {
    pub prefixes: Vec<DirName<TableLockIdent>>,
}

impl ListLocksReq {
    pub fn create(tenant: &Tenant) -> Self {
        let lock = TableLockIdent::new(tenant, 0, 0);
        let prefix = DirName::new_with_level(lock, 2);
        Self {
            prefixes: vec![prefix],
        }
    }

    pub fn create_with_table_ids(tenant: &Tenant, table_ids: Vec<u64>) -> Self {
        let mut prefixes = Vec::new();
        for table_id in table_ids {
            let lock = TableLockIdent::new(tenant, table_id, 0);
            let prefix = DirName::new(lock);
            prefixes.push(prefix);
        }
        Self { prefixes }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListLockRevReq {
    pub lock_key: LockKey,
}

impl ListLockRevReq {
    pub fn new(lock_key: LockKey) -> Self {
        Self { lock_key }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateLockRevReq {
    pub lock_key: LockKey,
    pub ttl: Duration,
    pub user: String,
    pub node: String,
    pub query_id: String,
}

impl CreateLockRevReq {
    pub fn new(
        lock_key: LockKey,
        user: String,
        node: String,
        query_id: String,
        expire_secs: Duration,
    ) -> Self {
        Self {
            lock_key,
            user,
            node,
            query_id,
            ttl: expire_secs,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateLockRevReply {
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtendLockRevReq {
    pub lock_key: LockKey,
    pub ttl: Duration,
    pub revision: u64,
    pub acquire_lock: bool,
}

impl ExtendLockRevReq {
    pub fn new(lock_key: LockKey, revision: u64, ttl: Duration, acquire_lock: bool) -> Self {
        Self {
            lock_key,
            revision,
            ttl,
            acquire_lock,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteLockRevReq {
    pub lock_key: LockKey,
    pub revision: u64,
}

impl DeleteLockRevReq {
    pub fn new(lock_key: LockKey, revision: u64) -> Self {
        Self { lock_key, revision }
    }
}
