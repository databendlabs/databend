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
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_kvapi::kvapi::KeyError;

use crate::tenant::Tenant;
use crate::tenant::ToTenant;

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
                    tenant: key.tenant,
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
            LockKey::Table { tenant, table_id } => {
                let ident = DirName::new(TableLockKey::new(tenant.clone(), *table_id, 0));
                ident.dir_name_with_slash()
            }
        }
    }

    pub fn gen_key(&self, revision: u64) -> TableLockKey {
        match self {
            LockKey::Table { tenant, table_id } => TableLockKey {
                tenant: tenant.clone(),
                table_id: *table_id,
                revision,
            },
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
    pub tenant: Tenant,
    pub table_ids: Vec<u64>,
}

impl ListLocksReq {
    pub fn create(tenant: impl ToTenant, table_ids: Vec<u64>) -> Self {
        Self {
            tenant: tenant.to_tenant(),
            table_ids,
        }
    }

    pub fn gen_prefixes(&self) -> Vec<String> {
        let mut prefixes = Vec::new();
        if self.table_ids.is_empty() {
            let lock = TableLockKey::new(self.tenant.clone(), 0, 0);
            let prefix = DirName::new_with_level(lock, 2).dir_name_with_slash();
            prefixes.push(prefix);
        } else {
            for table_id in &self.table_ids {
                let lock = TableLockKey::new(self.tenant.clone(), *table_id, 0);
                let prefix = DirName::new(lock).dir_name_with_slash();
                prefixes.push(prefix);
            }
        }
        prefixes
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListLockRevReq {
    pub lock_key: LockKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtendLockRevReq {
    pub lock_key: LockKey,
    pub expire_secs: u64,
    pub revision: u64,
    pub acquire_lock: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteLockRevReq {
    pub lock_key: LockKey,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableLockKey {
    pub tenant: Tenant,
    pub table_id: u64,
    pub revision: u64,
}

impl TableLockKey {
    pub fn new(tenant: impl ToTenant, table_id: u64, revision: u64) -> Self {
        Self {
            tenant: tenant.to_tenant(),
            table_id: table_id.into(),
            revision,
        }
    }

    pub fn tenant(&self) -> &Tenant {
        &self.tenant
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::schema::LockMeta;
    use crate::schema::TableId;
    use crate::schema::TableLockKey;
    use crate::tenant::Tenant;

    /// __fd_table_lock/<tenant>/table_id/revision -> LockMeta
    impl kvapi::Key for TableLockKey {
        const PREFIX: &'static str = "__fd_table_lock";

        type ValueType = LockMeta;

        fn parent(&self) -> Option<String> {
            Some(TableId::new(self.table_id).to_string_key())
        }

        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_str(self.tenant.tenant_name())
                .push_u64(self.table_id)
                .push_u64(self.revision)
        }

        fn decode_key(p: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let tenant = p.next_nonempty()?;
            let table_id = p.next_u64()?;
            let revision = p.next_u64()?;

            let tenant = Tenant::new_nonempty(tenant);
            Ok(Self {
                tenant,
                table_id,
                revision,
            })
        }
    }

    impl kvapi::Value for LockMeta {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
