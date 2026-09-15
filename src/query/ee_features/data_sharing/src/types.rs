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
use std::collections::BTreeSet;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::storage::StorageParams;

pub const SHARE_ENGINE: &str = "share";
pub const SHARE_PROVIDER_TABLE_ENGINE: &str = "FUSE";
pub const SHARE_OPT_PROVIDER_TENANT: &str = "provider_tenant";
pub const SHARE_OPT_SHARE_NAME: &str = "share_name";
pub const SHARE_OPT_SHARE_ID: &str = "share_id";
pub const SHARE_OPT_PROVIDER_DATABASE_ID: &str = "provider_database_id";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareDatabaseBinding {
    pub provider_tenant: String,
    pub share_name: String,
    pub share_id: u64,
    pub provider_database_id: u64,
}

impl ShareDatabaseBinding {
    pub fn to_engine_options(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                SHARE_OPT_PROVIDER_TENANT.to_string(),
                self.provider_tenant.clone(),
            ),
            (SHARE_OPT_SHARE_NAME.to_string(), self.share_name.clone()),
            (SHARE_OPT_SHARE_ID.to_string(), self.share_id.to_string()),
            (
                SHARE_OPT_PROVIDER_DATABASE_ID.to_string(),
                self.provider_database_id.to_string(),
            ),
        ])
    }

    pub fn from_engine_options(options: &BTreeMap<String, String>) -> Result<Self> {
        let required = |key: &str| {
            options.get(key).cloned().ok_or_else(|| {
                ErrorCode::BadArguments(format!("Missing shared database binding option '{key}'"))
            })
        };
        let parse_u64 = |key: &str| -> Result<u64> {
            required(key)?.parse::<u64>().map_err(|_| {
                ErrorCode::BadArguments(format!("Invalid shared database binding option '{key}'"))
            })
        };

        Ok(Self {
            provider_tenant: required(SHARE_OPT_PROVIDER_TENANT)?,
            share_name: required(SHARE_OPT_SHARE_NAME)?,
            share_id: parse_u64(SHARE_OPT_SHARE_ID)?,
            provider_database_id: parse_u64(SHARE_OPT_PROVIDER_DATABASE_ID)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareTableContext {
    pub binding: ShareDatabaseBinding,
    pub provider_table: String,
    pub provider_table_id: u64,
    pub connection: String,
    /// Provider storage location without credentials.
    ///
    /// `None` indicates a grant written before provider locations were
    /// persisted and retains the legacy local-config fallback.
    pub storage_params: Option<StorageParams>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareGrantDatabase {
    pub database: String,
    pub database_id: u64,
    pub database_meta_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareGrantTable {
    pub database: String,
    pub database_id: u64,
    pub database_meta_seq: u64,
    pub table: String,
    pub table_id: u64,
    pub table_meta_seq: u64,
    /// Provider storage location. The manager removes credentials before
    /// persisting it in the share grant.
    pub storage_params: StorageParams,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProviderObjectIds {
    pub database_id: u64,
    pub table_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ShareRevokeTarget {
    Database {
        database_id: u64,
        database_meta_seq: u64,
        requires_object_privilege: bool,
    },
    Table {
        database_id: u64,
        table_id: u64,
        database_meta_seq: u64,
        table_meta_seq: u64,
        requires_object_privilege: bool,
    },
}

impl ShareRevokeTarget {
    pub fn database_id(&self) -> u64 {
        match self {
            Self::Database { database_id, .. } | Self::Table { database_id, .. } => *database_id,
        }
    }

    pub fn table_id(&self) -> Option<u64> {
        match self {
            Self::Database { .. } => None,
            Self::Table { table_id, .. } => Some(*table_id),
        }
    }

    pub fn requires_object_privilege(&self) -> bool {
        match self {
            Self::Database {
                requires_object_privilege,
                ..
            }
            | Self::Table {
                requires_object_privilege,
                ..
            } => *requires_object_privilege,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SetShareConnection {
    Unchanged,
    ValidatedCurrent {
        connection: String,
    },
    Replace {
        connection: String,
        validated_table_ids: BTreeSet<u64>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetShareRequest {
    pub accounts: Option<Vec<String>>,
    pub comment: Option<String>,
    pub connection: SetShareConnection,
    pub if_exists: bool,
}

impl SetShareRequest {
    pub fn properties(comment: Option<String>, if_exists: bool) -> Self {
        Self {
            accounts: None,
            comment,
            connection: SetShareConnection::Unchanged,
            if_exists,
        }
    }

    pub fn accounts(
        accounts: Vec<String>,
        comment: Option<String>,
        connection: String,
        if_exists: bool,
    ) -> Self {
        Self {
            accounts: Some(accounts),
            comment,
            connection: SetShareConnection::ValidatedCurrent { connection },
            if_exists,
        }
    }

    pub fn connection(
        accounts: Option<Vec<String>>,
        comment: Option<String>,
        connection: String,
        validated_table_ids: BTreeSet<u64>,
        if_exists: bool,
    ) -> Self {
        Self {
            accounts,
            comment,
            connection: SetShareConnection::Replace {
                connection,
                validated_table_ids,
            },
            if_exists,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareShowEntry {
    pub created_on: String,
    pub kind: String,
    pub owner_account: String,
    pub name: String,
    pub database_name: String,
    pub to: String,
    pub owner: String,
    pub comment: String,
    pub listing_global_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareDescEntry {
    pub kind: String,
    pub name: String,
    pub shared_on: String,
}
