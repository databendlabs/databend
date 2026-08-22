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
use std::sync::Arc;

use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::fetch_id;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::reply::unpack_txn_reply;
use databend_common_meta_api::txn_cond_eq_seq;
use databend_common_meta_api::txn_del;
use databend_common_meta_api::txn_put_pb;
use databend_common_meta_app::data_id::DataId;
use databend_common_meta_app::data_share::DataShareDatabaseGrant;
use databend_common_meta_app::data_share::DataShareMeta;
use databend_common_meta_app::data_share::DataShareTableGrant;
use databend_common_meta_app::data_share::ShareIdIdent;
use databend_common_meta_app::data_share::ShareNameIdent;
use databend_common_meta_app::data_share::ShareNameResource;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseIdToName;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi;
use databend_meta_client::kvapi::DirName;
use databend_meta_client::kvapi::ListOptions;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::TxnCondition;
use databend_meta_client::types::TxnRequest;
use futures::TryStreamExt;
use log::debug;

use crate::meta_service_error;

mod presentation;
mod storage;

pub use presentation::ShareDescEntry;
pub use presentation::ShareShowEntry;
use presentation::like_match;
pub use storage::resolve_share_storage_params;

const TXN_MAX_RETRY_TIMES: usize = 20;

pub const SHARE_ENGINE: &str = "share";
pub const SHARE_PROVIDER_TABLE_ENGINE: &str = "FUSE";
pub const SHARE_OPT_PROVIDER_TENANT: &str = "provider_tenant";
pub const SHARE_OPT_SHARE_NAME: &str = "share_name";
pub const SHARE_OPT_SHARE_ID: &str = "share_id";
pub const SHARE_OPT_PROVIDER_DATABASE_ID: &str = "provider_database_id";

pub(crate) fn ensure_provider_table_can_be_shared(meta: &TableMeta) -> Result<()> {
    if !meta
        .engine
        .eq_ignore_ascii_case(SHARE_PROVIDER_TABLE_ENGINE)
    {
        return Err(ErrorCode::InvalidOperation(format!(
            "Shared table only supports Fuse provider tables, got '{}'",
            meta.engine
        )));
    }

    let has_masking_policy = !meta.column_mask_policy_columns_ids.is_empty()
        || meta
            .column_mask_policy
            .as_ref()
            .is_some_and(|policies| !policies.is_empty());
    let has_row_access_policy = meta.row_access_policy_columns_ids.is_some()
        || meta
            .row_access_policy
            .as_ref()
            .is_some_and(|policy| !policy.is_empty());
    if has_masking_policy || has_row_access_policy {
        return Err(ErrorCode::InvalidOperation(
            "Shared tables do not support provider row access or masking policies",
        ));
    }
    Ok(())
}

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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProviderObjectIds {
    pub database_id: u64,
    pub table_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ShareRevokeTarget {
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

    fn txn_conditions(&self) -> Vec<TxnCondition> {
        match self {
            Self::Database {
                database_id,
                database_meta_seq,
                ..
            } => vec![txn_cond_eq_seq(
                &DatabaseId::new(*database_id),
                *database_meta_seq,
            )],
            Self::Table {
                database_id,
                table_id,
                database_meta_seq,
                table_meta_seq,
                ..
            } => vec![
                txn_cond_eq_seq(&DatabaseId::new(*database_id), *database_meta_seq),
                txn_cond_eq_seq(&TableId::new(*table_id), *table_meta_seq),
            ],
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
    accounts: Option<Vec<String>>,
    comment: Option<String>,
    connection: SetShareConnection,
    if_exists: bool,
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

/// Concrete Meta-backed data sharing manager.
///
/// Share updates use CAS on both the name and metadata records, so multiple query
/// nodes can safely update the same share without a process-local service layer.
pub struct ShareMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
}

impl ShareMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>) -> Self {
        Self { kv_api }
    }

    #[fastrace::trace]
    pub async fn create_share(
        &self,
        provider: &Tenant,
        create_option: CreateOption,
        share: &str,
        connection: Option<String>,
        comment: Option<String>,
    ) -> Result<()> {
        let name_ident = ShareNameIdent::new(provider.clone(), share);

        for attempt in 1..=TXN_MAX_RETRY_TIMES {
            let current = self
                .kv_api
                .get_pb(&name_ident)
                .await
                .map_err(meta_service_error)?;

            if current.is_some() {
                match create_option {
                    CreateOption::Create => return Err(share_exists(provider, share)),
                    CreateOption::CreateIfNotExists => return Ok(()),
                    CreateOption::CreateOrReplace => {}
                }
            }

            let share_id = fetch_id(self.kv_api.as_ref(), IdGenerator::share_id())
                .await
                .map_err(meta_service_error)?;
            let id_ident = ShareIdIdent::new(share_id);
            let meta = DataShareMeta {
                provider: provider.tenant_name().to_string(),
                name: share.to_string(),
                created_on: Utc::now(),
                comment: comment.clone(),
                accounts: BTreeSet::new(),
                database: None,
                tables: BTreeMap::new(),
                connection: connection.clone(),
            };

            let mut txn = TxnRequest::default();
            txn.condition
                .push(txn_cond_eq_seq(&name_ident, seq(&current)));
            if let Some(old) = &current {
                let old_id_ident = ShareIdIdent::new(*old.data);
                let old_meta = self
                    .kv_api
                    .get_pb(&old_id_ident)
                    .await
                    .map_err(meta_service_error)?;
                txn.condition
                    .push(txn_cond_eq_seq(&old_id_ident, seq(&old_meta)));
                txn.if_then.push(txn_del(&old_id_ident));
            }
            txn.if_then.push(txn_put_pb(
                &name_ident,
                &DataId::<ShareNameResource>::new(share_id),
            ));
            txn.if_then.push(txn_put_pb(&id_ident, &meta));

            if self.send_txn(txn).await? {
                return Ok(());
            }
            log_txn_retry("create share", provider, share, attempt);
        }

        Err(txn_retry_error("create share"))
    }

    #[fastrace::trace]
    pub async fn drop_share(&self, provider: &Tenant, share: &str) -> Result<()> {
        let name_ident = ShareNameIdent::new(provider.clone(), share);

        for attempt in 1..=TXN_MAX_RETRY_TIMES {
            let Some(id) = self
                .kv_api
                .get_pb(&name_ident)
                .await
                .map_err(meta_service_error)?
            else {
                return Err(unknown_share(provider.tenant_name(), share));
            };
            let id_ident = ShareIdIdent::new(*id.data);
            let meta = self
                .kv_api
                .get_pb(&id_ident)
                .await
                .map_err(meta_service_error)?;

            let mut txn = TxnRequest::default();
            txn.condition.push(txn_cond_eq_seq(&name_ident, id.seq));
            txn.condition.push(txn_cond_eq_seq(&id_ident, seq(&meta)));
            txn.if_then.push(txn_del(&name_ident));
            txn.if_then.push(txn_del(&id_ident));
            if self.send_txn(txn).await? {
                return Ok(());
            }
            log_txn_retry("drop share", provider, share, attempt);
        }

        Err(txn_retry_error("drop share"))
    }

    pub async fn add_accounts(
        &self,
        provider: &Tenant,
        share: &str,
        accounts: Vec<String>,
        expected_connection: String,
        if_exists: bool,
    ) -> Result<()> {
        self.update_share(provider, share, if_exists, move |meta| {
            ensure_connection_unchanged(meta, Some(&expected_connection))?;
            meta.accounts.extend(accounts.clone());
            Ok(())
        })
        .await
    }

    pub async fn remove_accounts(
        &self,
        provider: &Tenant,
        share: &str,
        accounts: Vec<String>,
        expected_connection: Option<String>,
        if_exists: bool,
    ) -> Result<()> {
        self.update_share(provider, share, if_exists, move |meta| {
            ensure_connection_unchanged(meta, expected_connection.as_ref())?;
            for account in &accounts {
                meta.accounts.remove(account);
            }
            Ok(())
        })
        .await
    }

    pub async fn set_share(
        &self,
        provider: &Tenant,
        share: &str,
        request: SetShareRequest,
    ) -> Result<()> {
        self.update_share(provider, share, request.if_exists, move |meta| {
            if let Some(accounts) = &request.accounts {
                meta.accounts = accounts.iter().cloned().collect();
            }

            match &request.connection {
                SetShareConnection::Unchanged => {}
                SetShareConnection::ValidatedCurrent { connection } => {
                    ensure_connection_unchanged(meta, Some(connection))?;
                }
                SetShareConnection::Replace {
                    connection,
                    validated_table_ids,
                } => {
                    let current_table_ids = meta.tables.keys().copied().collect::<BTreeSet<_>>();
                    if current_table_ids != *validated_table_ids {
                        return Err(ErrorCode::InvalidOperation(
                            "Share grants changed while validating the replacement connection; retry ALTER SHARE",
                        ));
                    }
                    meta.connection = Some(connection.clone());
                }
            }

            if request.comment.is_some() {
                meta.comment = request.comment.clone();
            }
            Ok(())
        })
        .await
    }

    pub async fn grant_database(
        &self,
        provider: &Tenant,
        share: &str,
        grant: ShareGrantDatabase,
    ) -> Result<()> {
        let name_ident = ShareNameIdent::new(provider.clone(), share);
        for attempt in 1..=TXN_MAX_RETRY_TIMES {
            let Some(id) = self
                .kv_api
                .get_pb(&name_ident)
                .await
                .map_err(meta_service_error)?
            else {
                return Err(unknown_share(provider.tenant_name(), share));
            };
            let id_ident = ShareIdIdent::new(*id.data);
            let Some(mut meta) = self
                .kv_api
                .get_pb(&id_ident)
                .await
                .map_err(meta_service_error)?
            else {
                continue;
            };

            if let Some(existing) = &meta.data.database
                && existing.database_id != grant.database_id
            {
                let existing_database = self
                    .kv_api
                    .get_pb(&DatabaseId::new(existing.database_id))
                    .await
                    .map_err(meta_service_error)?;
                if existing_database
                    .as_ref()
                    .is_some_and(|database| database.data.drop_on.is_none())
                {
                    return Err(ErrorCode::BadArguments(format!(
                        "Share '{}' already has USAGE on another database",
                        share
                    )));
                }
                meta.data.tables.clear();
            }

            meta.data.database = Some(DataShareDatabaseGrant {
                database_id: grant.database_id,
                shared_on: Utc::now(),
            });

            let txn = TxnRequest::new(
                vec![
                    txn_cond_eq_seq(&name_ident, id.seq),
                    txn_cond_eq_seq(&id_ident, meta.seq),
                    txn_cond_eq_seq(&DatabaseId::new(grant.database_id), grant.database_meta_seq),
                ],
                vec![txn_put_pb(&id_ident, &meta.data)],
            );
            if self.send_txn(txn).await? {
                return Ok(());
            }
            log_txn_retry("grant database", provider, share, attempt);
        }
        Err(txn_retry_error("grant database"))
    }

    pub(crate) async fn prepare_revoke_database(
        &self,
        provider: &Tenant,
        share: &str,
        current_database_id: Option<u64>,
    ) -> Result<Option<ShareRevokeTarget>> {
        let Some(database_id) = current_database_id else {
            return Ok(None);
        };
        let (_, meta) = self.get_share(provider.tenant_name(), share).await?;
        let Some(grant) = meta
            .database
            .filter(|grant| grant.database_id == database_id)
        else {
            return Ok(None);
        };
        let database_meta = self
            .kv_api
            .get_pb(&DatabaseId::new(grant.database_id))
            .await
            .map_err(meta_service_error)?;
        let requires_object_privilege = database_meta
            .as_ref()
            .is_some_and(|meta| meta.data.drop_on.is_none());
        Ok(Some(ShareRevokeTarget::Database {
            database_id: grant.database_id,
            database_meta_seq: seq(&database_meta),
            requires_object_privilege,
        }))
    }

    pub async fn grant_table(
        &self,
        provider: &Tenant,
        share: &str,
        grant: ShareGrantTable,
        expected_connection: String,
    ) -> Result<()> {
        let conditions = vec![
            txn_cond_eq_seq(&DatabaseId::new(grant.database_id), grant.database_meta_seq),
            txn_cond_eq_seq(&TableId::new(grant.table_id), grant.table_meta_seq),
        ];
        self.update_share_with_conditions(provider, share, false, conditions, move |meta| {
            let Some(database) = &meta.database else {
                return Err(ErrorCode::BadArguments(format!(
                    "Grant USAGE on a database to share '{}' before granting tables",
                    share
                )));
            };
            if database.database_id != grant.database_id {
                return Err(ErrorCode::BadArguments(format!(
                    "Cannot grant table '{}' from a different database to share '{}'",
                    grant.table, share
                )));
            }
            if meta.connection.as_deref() != Some(expected_connection.as_str()) {
                return Err(ErrorCode::InvalidOperation(format!(
                    "Share '{}' connection changed while validating the table grant; retry GRANT",
                    share
                )));
            }
            meta.tables.insert(grant.table_id, DataShareTableGrant {
                shared_on: Utc::now(),
            });
            Ok(())
        })
        .await
    }

    pub(crate) async fn prepare_revoke_table(
        &self,
        provider: &Tenant,
        share: &str,
        current_object_ids: Option<ProviderObjectIds>,
    ) -> Result<Option<ShareRevokeTarget>> {
        let Some(current_object_ids) = current_object_ids else {
            return Ok(None);
        };
        let (_, meta) = self.get_share(provider.tenant_name(), share).await?;
        let Some(database_grant) = meta.database else {
            return Ok(None);
        };
        if database_grant.database_id != current_object_ids.database_id {
            return Ok(None);
        }
        if !meta.tables.contains_key(&current_object_ids.table_id) {
            return Ok(None);
        }

        let database_meta = self
            .kv_api
            .get_pb(&DatabaseId::new(database_grant.database_id))
            .await
            .map_err(meta_service_error)?;
        let table_meta = self
            .kv_api
            .get_pb(&TableId::new(current_object_ids.table_id))
            .await
            .map_err(meta_service_error)?;
        let requires_object_privilege = database_meta
            .as_ref()
            .is_some_and(|meta| meta.data.drop_on.is_none())
            && table_meta
                .as_ref()
                .is_some_and(|meta| meta.data.drop_on.is_none());
        Ok(Some(ShareRevokeTarget::Table {
            database_id: database_grant.database_id,
            table_id: current_object_ids.table_id,
            database_meta_seq: seq(&database_meta),
            table_meta_seq: seq(&table_meta),
            requires_object_privilege,
        }))
    }

    pub(crate) async fn revoke_share_object(
        &self,
        provider: &Tenant,
        share: &str,
        target: ShareRevokeTarget,
    ) -> Result<()> {
        let conditions = target.txn_conditions();
        self.update_share_with_conditions(provider, share, false, conditions, move |meta| {
            match &target {
                ShareRevokeTarget::Database { database_id, .. } => {
                    if meta
                        .database
                        .as_ref()
                        .is_some_and(|grant| grant.database_id == *database_id)
                    {
                        meta.database = None;
                        meta.tables.clear();
                    }
                }
                ShareRevokeTarget::Table { table_id, .. } => {
                    meta.tables.remove(table_id);
                }
            }
            Ok(())
        })
        .await
    }

    pub async fn show_shares(
        &self,
        tenant: &Tenant,
        like_pattern: Option<&str>,
        limit: Option<u64>,
    ) -> Result<Vec<ShareShowEntry>> {
        let metas = self.list_all().await?;
        let mut rows = Vec::new();
        for meta in metas {
            let kind = if meta.provider == tenant.tenant_name() {
                "OUTBOUND"
            } else if meta.accounts.contains(tenant.tenant_name()) {
                "INBOUND"
            } else {
                continue;
            };
            if like_pattern.is_some_and(|pattern| !like_match(pattern, &meta.name)) {
                continue;
            }
            let mut accounts = meta.accounts.iter().cloned().collect::<Vec<_>>();
            accounts.sort();
            let database_name = self.current_database_name(&meta).await?;
            rows.push(ShareShowEntry {
                created_on: meta.created_on.to_rfc3339(),
                kind: kind.to_string(),
                owner_account: meta.provider,
                name: meta.name,
                database_name,
                to: if kind == "OUTBOUND" {
                    accounts.join(", ")
                } else {
                    String::new()
                },
                owner: String::new(),
                comment: meta.comment.unwrap_or_default(),
                listing_global_name: String::new(),
            });
        }
        rows.sort_by(|a, b| {
            a.kind
                .cmp(&b.kind)
                .then(a.owner_account.cmp(&b.owner_account))
                .then(a.name.cmp(&b.name))
        });
        if let Some(limit) = limit {
            rows.truncate(limit as usize);
        }
        Ok(rows)
    }

    pub async fn describe_share(
        &self,
        tenant: &Tenant,
        provider_tenant: Option<&str>,
        share: &str,
    ) -> Result<Vec<ShareDescEntry>> {
        let provider = provider_tenant.unwrap_or_else(|| tenant.tenant_name());
        let (_, meta) = self.get_share(provider, share).await?;
        if provider != tenant.tenant_name() {
            ensure_account(&meta, tenant.tenant_name())?;
        }
        let database_name = self.current_database_name(&meta).await?;
        let mut rows = Vec::new();
        if let Some(database) = &meta.database {
            rows.push(ShareDescEntry {
                kind: "DATABASE".to_string(),
                name: database_name.clone(),
                shared_on: database.shared_on.to_rfc3339(),
            });
        }
        for (table_id, table) in &meta.tables {
            let table_name = self.current_table_name(*table_id).await?;
            rows.push(ShareDescEntry {
                kind: "TABLE".to_string(),
                name: format!("{}.{}", database_name, table_name),
                shared_on: table.shared_on.to_rfc3339(),
            });
        }
        Ok(rows)
    }

    pub async fn exists(&self, provider: &Tenant, share: &str) -> Result<bool> {
        let name_ident = ShareNameIdent::new(provider.clone(), share);
        Ok(self
            .kv_api
            .get_pb(&name_ident)
            .await
            .map_err(meta_service_error)?
            .is_some())
    }

    pub async fn get_connection_name(&self, provider: &Tenant, share: &str) -> Result<String> {
        let (_, meta) = self.get_share(provider.tenant_name(), share).await?;
        meta.connection.ok_or_else(|| {
            ErrorCode::InvalidOperation(format!("Share '{}' has no connection", share))
        })
    }

    pub async fn get_connection_name_if_exists(
        &self,
        provider: &Tenant,
        share: &str,
    ) -> Result<Option<String>> {
        let name_ident = ShareNameIdent::new(provider.clone(), share);
        let Some(id) = self
            .kv_api
            .get_pb(&name_ident)
            .await
            .map_err(meta_service_error)?
        else {
            return Ok(None);
        };
        let meta = self
            .kv_api
            .get_pb(&ShareIdIdent::new(*id.data))
            .await
            .map_err(meta_service_error)?;
        Ok(meta.and_then(|meta| meta.data.connection))
    }

    pub async fn get_granted_table_ids(
        &self,
        provider: &Tenant,
        share: &str,
    ) -> Result<BTreeSet<u64>> {
        let (_, meta) = self.get_share(provider.tenant_name(), share).await?;
        Ok(meta.tables.keys().copied().collect())
    }

    pub async fn bind_share_database(
        &self,
        consumer: &Tenant,
        provider_tenant: &str,
        share: &str,
    ) -> Result<ShareDatabaseBinding> {
        let (share_id, meta) = self.get_share(provider_tenant, share).await?;
        ensure_account(&meta, consumer.tenant_name())?;
        let database = meta.database.ok_or_else(|| {
            ErrorCode::BadArguments(format!(
                "Share '{}.{}' does not grant a database",
                provider_tenant, share
            ))
        })?;
        self.ensure_provider_database_active(database.database_id)
            .await?;
        Ok(ShareDatabaseBinding {
            provider_tenant: provider_tenant.to_string(),
            share_name: share.to_string(),
            share_id,
            provider_database_id: database.database_id,
        })
    }

    pub async fn resolve_shared_table(
        &self,
        consumer: &Tenant,
        binding: &ShareDatabaseBinding,
        table: &str,
    ) -> Result<ShareTableContext> {
        let meta = self.validate_binding(consumer, binding).await?;
        let grant = self
            .find_granted_table(&meta, table)
            .await?
            .ok_or_else(|| {
                ErrorCode::UnknownTable(format!(
                    "Table '{}' is not granted by share '{}.{}'",
                    table, binding.provider_tenant, binding.share_name
                ))
            })?;
        self.table_context(binding, &meta, grant.0, grant.1)
    }

    pub async fn list_shared_tables(
        &self,
        consumer: &Tenant,
        binding: &ShareDatabaseBinding,
    ) -> Result<Vec<ShareTableContext>> {
        let meta = self.validate_binding(consumer, binding).await?;
        let mut tables = Vec::with_capacity(meta.tables.len());
        for table_id in meta.tables.keys().copied() {
            let table_name = self.current_table_name(table_id).await?;
            if table_name.is_empty() {
                continue;
            }
            tables.push(self.table_context(binding, &meta, table_name, table_id)?);
        }
        Ok(tables)
    }

    async fn validate_binding(
        &self,
        consumer: &Tenant,
        binding: &ShareDatabaseBinding,
    ) -> Result<DataShareMeta> {
        let (share_id, meta) = self
            .get_share(&binding.provider_tenant, &binding.share_name)
            .await?;
        if share_id != binding.share_id {
            return Err(stale_binding("share was dropped or recreated"));
        }
        ensure_account(&meta, consumer.tenant_name())?;
        let database = meta.database.as_ref().ok_or_else(|| {
            ErrorCode::InvalidOperation(format!(
                "Share '{}.{}' no longer grants a database",
                binding.provider_tenant, binding.share_name
            ))
        })?;
        if database.database_id != binding.provider_database_id {
            return Err(stale_binding("provider database grant changed"));
        }
        self.ensure_provider_database_active(binding.provider_database_id)
            .await?;
        Ok(meta)
    }

    async fn ensure_provider_database_active(&self, database_id: u64) -> Result<()> {
        let database = self
            .kv_api
            .get_pb(&DatabaseId::new(database_id))
            .await
            .map_err(meta_service_error)?;
        match database {
            Some(database) if database.data.drop_on.is_none() => Ok(()),
            Some(_) => Err(stale_binding("provider database was dropped")),
            None => Err(stale_binding("provider database metadata was removed")),
        }
    }

    fn table_context(
        &self,
        binding: &ShareDatabaseBinding,
        meta: &DataShareMeta,
        table_name: impl Into<String>,
        table_id: u64,
    ) -> Result<ShareTableContext> {
        let connection = meta.connection.clone().ok_or_else(|| {
            ErrorCode::InvalidOperation(format!(
                "Share '{}.{}' has no connection",
                binding.provider_tenant, binding.share_name
            ))
        })?;
        Ok(ShareTableContext {
            binding: binding.clone(),
            provider_table: table_name.into(),
            provider_table_id: table_id,
            connection,
        })
    }

    async fn find_granted_table(
        &self,
        meta: &DataShareMeta,
        table_name: &str,
    ) -> Result<Option<(String, u64)>> {
        for table_id in meta.tables.keys().copied() {
            let current_name = self.current_table_name(table_id).await?;
            if current_name == table_name {
                return Ok(Some((current_name, table_id)));
            }
        }
        Ok(None)
    }

    async fn current_database_name(&self, meta: &DataShareMeta) -> Result<String> {
        let Some(database) = &meta.database else {
            return Ok(String::new());
        };
        Ok(self
            .kv_api
            .get_pb(&DatabaseIdToName::new(database.database_id))
            .await
            .map_err(meta_service_error)?
            .map(|name| name.data.database_name().to_string())
            .unwrap_or_default())
    }

    async fn current_table_name(&self, table_id: u64) -> Result<String> {
        Ok(self
            .kv_api
            .get_pb(&TableIdToName { table_id })
            .await
            .map_err(meta_service_error)?
            .map(|name| name.data.table_name)
            .unwrap_or_default())
    }

    async fn get_share(&self, provider: &str, share: &str) -> Result<(u64, DataShareMeta)> {
        let name_ident = ShareNameIdent::new(Tenant::new_literal(provider), share);
        let id = self
            .kv_api
            .get_pb(&name_ident)
            .await
            .map_err(meta_service_error)?
            .ok_or_else(|| unknown_share(provider, share))?;
        let meta = self
            .kv_api
            .get_pb(&ShareIdIdent::new(*id.data))
            .await
            .map_err(meta_service_error)?
            .ok_or_else(|| unknown_share(provider, share))?;
        Ok((*id.data, meta.data))
    }

    async fn update_share<F>(
        &self,
        provider: &Tenant,
        share: &str,
        if_exists: bool,
        update: F,
    ) -> Result<()>
    where
        F: Fn(&mut DataShareMeta) -> Result<()>,
    {
        self.update_share_with_conditions(provider, share, if_exists, Vec::new(), update)
            .await
    }

    #[fastrace::trace]
    async fn update_share_with_conditions<F>(
        &self,
        provider: &Tenant,
        share: &str,
        if_exists: bool,
        extra_conditions: Vec<TxnCondition>,
        update: F,
    ) -> Result<()>
    where
        F: Fn(&mut DataShareMeta) -> Result<()>,
    {
        let name_ident = ShareNameIdent::new(provider.clone(), share);
        for attempt in 1..=TXN_MAX_RETRY_TIMES {
            let Some(id) = self
                .kv_api
                .get_pb(&name_ident)
                .await
                .map_err(meta_service_error)?
            else {
                return if if_exists {
                    Ok(())
                } else {
                    Err(unknown_share(provider.tenant_name(), share))
                };
            };
            let id_ident = ShareIdIdent::new(*id.data);
            let Some(mut meta) = self
                .kv_api
                .get_pb(&id_ident)
                .await
                .map_err(meta_service_error)?
            else {
                continue;
            };
            update(&mut meta.data)?;

            let mut conditions = vec![
                txn_cond_eq_seq(&name_ident, id.seq),
                txn_cond_eq_seq(&id_ident, meta.seq),
            ];
            conditions.extend(extra_conditions.iter().cloned());
            let txn = TxnRequest::new(conditions, vec![txn_put_pb(&id_ident, &meta.data)]);
            if self.send_txn(txn).await? {
                return Ok(());
            }
            log_txn_retry("update share", provider, share, attempt);
        }
        Err(txn_retry_error("update share"))
    }

    async fn list_all(&self) -> Result<Vec<DataShareMeta>> {
        let dir = DirName::new(ShareIdIdent::new(0));
        let stream = self
            .kv_api
            .list_pb_values(ListOptions::unlimited(&dir))
            .await
            .map_err(meta_service_error)?;
        stream.try_collect().await.map_err(meta_service_error)
    }

    async fn send_txn(&self, txn: TxnRequest) -> Result<bool> {
        let reply = self
            .kv_api
            .transaction(txn)
            .await
            .map_err(meta_service_error)?;
        Ok(unpack_txn_reply(reply).0)
    }
}

fn seq<T>(value: &Option<SeqV<T>>) -> u64 {
    value.as_ref().map(|v| v.seq).unwrap_or(0)
}

fn ensure_account(meta: &DataShareMeta, account: &str) -> Result<()> {
    if !meta.accounts.contains(account) {
        return Err(ErrorCode::InvalidOperation(format!(
            "Account '{}' is not authorized for share id",
            account
        )));
    }
    Ok(())
}

fn ensure_connection_unchanged(
    meta: &DataShareMeta,
    expected_connection: Option<&String>,
) -> Result<()> {
    if meta.connection.as_ref() != expected_connection {
        return Err(ErrorCode::InvalidOperation(
            "Share connection changed while authorizing the account update; retry ALTER SHARE",
        ));
    }
    Ok(())
}

fn share_exists(provider: &Tenant, share: &str) -> ErrorCode {
    ErrorCode::BadArguments(format!(
        "Share '{}.{}' already exists",
        provider.tenant_name(),
        share
    ))
}

fn unknown_share(provider: &str, share: &str) -> ErrorCode {
    ErrorCode::BadArguments(format!("Unknown share '{}.{}'", provider, share))
}

fn stale_binding(reason: &str) -> ErrorCode {
    ErrorCode::InvalidOperation(format!(
        "Shared database binding is no longer valid: {reason}"
    ))
}

fn txn_retry_error(operation: &str) -> ErrorCode {
    ErrorCode::MetaServiceError(format!(
        "Failed to {operation} after {TXN_MAX_RETRY_TIMES} concurrent retries"
    ))
}

fn log_txn_retry(operation: &str, provider: &Tenant, share: &str, attempt: usize) {
    debug!(
        "Data share metadata transaction conflict: operation={}, provider={}, share={}, attempt={}/{}",
        operation,
        provider.tenant_name(),
        share,
        attempt,
        TXN_MAX_RETRY_TIMES
    );
}

#[cfg(test)]
mod tests {
    use databend_common_meta_api::kv_pb_api::UpsertPB;
    use databend_common_meta_app::schema::DBIdTableName;
    use databend_common_meta_app::schema::DatabaseIdToName;
    use databend_common_meta_app::schema::DatabaseMeta;
    use databend_common_meta_app::schema::SecurityPolicyColumnMap;
    use databend_common_meta_app::schema::TableIdToName;
    use databend_common_meta_app::schema::TableMeta;
    use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdentRaw;
    use databend_common_meta_store::MetaStore;
    use databend_meta_runtime::DatabendRuntime;

    use super::*;

    fn tenant(name: &str) -> Tenant {
        Tenant::new_literal(name)
    }

    async fn manager() -> (MetaStore, ShareMgr) {
        let store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let manager = ShareMgr::create(Arc::new(store.clone()));
        (store, manager)
    }

    async fn database_grant(
        manager: &ShareMgr,
        database: &str,
        database_id: u64,
    ) -> ShareGrantDatabase {
        let meta = manager
            .kv_api
            .get_pb(&DatabaseId::new(database_id))
            .await
            .unwrap();
        ShareGrantDatabase {
            database: database.to_string(),
            database_id,
            database_meta_seq: seq(&meta),
        }
    }

    async fn table_grant(
        manager: &ShareMgr,
        database: &str,
        database_id: u64,
        table: &str,
        table_id: u64,
    ) -> ShareGrantTable {
        let database_meta = manager
            .kv_api
            .get_pb(&DatabaseId::new(database_id))
            .await
            .unwrap();
        let table_meta = manager
            .kv_api
            .get_pb(&TableId::new(table_id))
            .await
            .unwrap();
        ShareGrantTable {
            database: database.to_string(),
            database_id,
            database_meta_seq: seq(&database_meta),
            table: table.to_string(),
            table_id,
            table_meta_seq: seq(&table_meta),
        }
    }

    async fn seed_database_name(manager: &ShareMgr, database_id: u64, database: &str) {
        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(
                DatabaseIdToName::new(database_id),
                DatabaseNameIdentRaw::new("provider", database),
            ))
            .await
            .unwrap();
    }

    async fn seed_table_name(manager: &ShareMgr, database_id: u64, table_id: u64, table: &str) {
        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(
                TableIdToName { table_id },
                DBIdTableName::new(database_id, table),
            ))
            .await
            .unwrap();
    }

    async fn create_granted_share(manager: &ShareMgr) -> ShareDatabaseBinding {
        let provider = tenant("provider");
        let consumer = tenant("consumer");
        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(
                DatabaseId::new(11),
                DatabaseMeta::default(),
            ))
            .await
            .unwrap();
        seed_database_name(manager, 11, "db").await;
        seed_table_name(manager, 11, 101, "orders").await;
        manager
            .create_share(
                &provider,
                CreateOption::Create,
                "sales",
                Some("share_conn".to_string()),
                None,
            )
            .await
            .unwrap();
        manager
            .grant_database(&provider, "sales", database_grant(manager, "db", 11).await)
            .await
            .unwrap();
        manager
            .grant_table(
                &provider,
                "sales",
                table_grant(manager, "db", 11, "orders", 101).await,
                "share_conn".to_string(),
            )
            .await
            .unwrap();
        manager
            .add_accounts(
                &provider,
                "sales",
                vec![consumer.tenant_name().to_string()],
                "share_conn".to_string(),
                false,
            )
            .await
            .unwrap();
        manager
            .bind_share_database(&consumer, "provider", "sales")
            .await
            .unwrap()
    }

    async fn revoke_database(
        manager: &ShareMgr,
        provider: &Tenant,
        share: &str,
        current_database_id: Option<u64>,
    ) {
        if let Some(target) = manager
            .prepare_revoke_database(provider, share, current_database_id)
            .await
            .unwrap()
        {
            manager
                .revoke_share_object(provider, share, target)
                .await
                .unwrap();
        }
    }

    async fn revoke_table(
        manager: &ShareMgr,
        provider: &Tenant,
        share: &str,
        current_object_ids: Option<ProviderObjectIds>,
    ) {
        if let Some(target) = manager
            .prepare_revoke_table(provider, share, current_object_ids)
            .await
            .unwrap()
        {
            manager
                .revoke_share_object(provider, share, target)
                .await
                .unwrap();
        }
    }

    #[test]
    fn binding_options_round_trip() {
        let binding = ShareDatabaseBinding {
            provider_tenant: "provider".to_string(),
            share_name: "sales".to_string(),
            share_id: 7,
            provider_database_id: 9,
        };
        let options = binding.to_engine_options();
        assert_eq!(
            binding,
            ShareDatabaseBinding::from_engine_options(&options).unwrap()
        );
    }

    #[test]
    fn provider_table_security_policies_are_rejected() {
        let mut table_meta = TableMeta {
            engine: SHARE_PROVIDER_TABLE_ENGINE.to_string(),
            row_access_policy_columns_ids: Some(SecurityPolicyColumnMap::new(1, vec![0])),
            ..Default::default()
        };
        assert!(ensure_provider_table_can_be_shared(&table_meta).is_err());

        table_meta.row_access_policy_columns_ids = None;
        table_meta
            .column_mask_policy_columns_ids
            .insert(0, SecurityPolicyColumnMap::new(2, vec![0]));
        assert!(ensure_provider_table_can_be_shared(&table_meta).is_err());

        table_meta.column_mask_policy_columns_ids.clear();
        table_meta.row_access_policy = Some("legacy_row_policy".to_string());
        assert!(ensure_provider_table_can_be_shared(&table_meta).is_err());

        table_meta.row_access_policy = None;
        table_meta.column_mask_policy = Some(BTreeMap::from([(
            "secret".to_string(),
            "legacy_mask_policy".to_string(),
        )]));
        assert!(ensure_provider_table_can_be_shared(&table_meta).is_err());

        table_meta.column_mask_policy = None;
        assert!(ensure_provider_table_can_be_shared(&table_meta).is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn meta_share_is_visible_from_another_manager() {
        let (store, first) = manager().await;
        let binding = create_granted_share(&first).await;
        let second = ShareMgr::create(Arc::new(store));

        let table = second
            .resolve_shared_table(&tenant("consumer"), &binding, "orders")
            .await
            .unwrap();
        assert_eq!(101, table.provider_table_id);
        assert_eq!("share_conn", table.connection);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dropped_provider_database_invalidates_existing_binding() {
        let (_store, manager) = manager().await;
        let binding = create_granted_share(&manager).await;
        let dropped_database = DatabaseMeta {
            drop_on: Some(Utc::now()),
            ..Default::default()
        };
        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(
                DatabaseId::new(binding.provider_database_id),
                dropped_database,
            ))
            .await
            .unwrap();

        let err = manager
            .resolve_shared_table(&tenant("consumer"), &binding, "orders")
            .await
            .unwrap_err();
        assert!(err.message().contains("provider database was dropped"));

        // Recreating a same-named database produces a new id and must not revive
        // a binding that stores the dropped database id.
        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(
                DatabaseId::new(binding.provider_database_id + 1),
                DatabaseMeta::default(),
            ))
            .await
            .unwrap();
        assert!(
            manager
                .resolve_shared_table(&tenant("consumer"), &binding, "orders")
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remove_and_readd_account_restores_existing_binding() {
        let (_store, manager) = manager().await;
        let binding = create_granted_share(&manager).await;
        let provider = tenant("provider");
        let consumer = tenant("consumer");

        manager
            .remove_accounts(
                &provider,
                "sales",
                vec![consumer.tenant_name().to_string()],
                Some("share_conn".to_string()),
                false,
            )
            .await
            .unwrap();
        assert!(
            manager
                .resolve_shared_table(&consumer, &binding, "orders")
                .await
                .is_err()
        );

        manager
            .add_accounts(
                &provider,
                "sales",
                vec![consumer.tenant_name().to_string()],
                "share_conn".to_string(),
                false,
            )
            .await
            .unwrap();
        manager
            .resolve_shared_table(&consumer, &binding, "orders")
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recreate_share_invalidates_existing_binding() {
        let (_store, manager) = manager().await;
        let old_binding = create_granted_share(&manager).await;
        let provider = tenant("provider");
        let consumer = tenant("consumer");

        manager.drop_share(&provider, "sales").await.unwrap();
        let new_binding = create_granted_share(&manager).await;
        assert_ne!(old_binding.share_id, new_binding.share_id);
        assert!(
            manager
                .resolve_shared_table(&consumer, &old_binding, "orders")
                .await
                .is_err()
        );
        manager
            .resolve_shared_table(&consumer, &new_binding, "orders")
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_options_preserve_or_replace_meta() {
        let (_store, manager) = manager().await;
        let provider = tenant("provider");

        manager
            .create_share(
                &provider,
                CreateOption::Create,
                "sales",
                Some("original_conn".to_string()),
                Some("original".to_string()),
            )
            .await
            .unwrap();
        let (original_id, _) = manager.get_share("provider", "sales").await.unwrap();

        assert!(
            manager
                .create_share(&provider, CreateOption::Create, "sales", None, None)
                .await
                .is_err()
        );
        manager
            .create_share(
                &provider,
                CreateOption::CreateIfNotExists,
                "sales",
                Some("ignored_conn".to_string()),
                Some("ignored".to_string()),
            )
            .await
            .unwrap();
        let (same_id, same_meta) = manager.get_share("provider", "sales").await.unwrap();
        assert_eq!(original_id, same_id);
        assert_eq!(Some("original_conn".to_string()), same_meta.connection);
        assert_eq!(Some("original".to_string()), same_meta.comment);

        manager
            .create_share(
                &provider,
                CreateOption::CreateOrReplace,
                "sales",
                Some("replacement_conn".to_string()),
                Some("replacement".to_string()),
            )
            .await
            .unwrap();
        let (new_id, new_meta) = manager.get_share("provider", "sales").await.unwrap();
        assert_ne!(original_id, new_id);
        assert_eq!(Some("replacement_conn".to_string()), new_meta.connection);
        assert_eq!(Some("replacement".to_string()), new_meta.comment);
        assert!(new_meta.accounts.is_empty());
        assert!(new_meta.database.is_none());
        assert!(new_meta.tables.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn grant_and_account_constraints_are_meta_backed() {
        let (_store, manager) = manager().await;
        let binding = create_granted_share(&manager).await;
        let provider = tenant("provider");
        let consumer = tenant("consumer");

        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(
                DatabaseId::new(22),
                DatabaseMeta::default(),
            ))
            .await
            .unwrap();
        assert!(
            manager
                .grant_database(
                    &provider,
                    "sales",
                    database_grant(&manager, "other", 22).await,
                )
                .await
                .is_err()
        );
        assert!(
            manager
                .grant_table(
                    &provider,
                    "sales",
                    table_grant(&manager, "other", 22, "other_table", 202).await,
                    "share_conn".to_string(),
                )
                .await
                .is_err()
        );

        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(DatabaseId::new(11), DatabaseMeta {
                drop_on: Some(Utc::now()),
                ..Default::default()
            }))
            .await
            .unwrap();
        manager
            .grant_database(
                &provider,
                "sales",
                database_grant(&manager, "other", 22).await,
            )
            .await
            .unwrap();
        let (_, meta) = manager.get_share("provider", "sales").await.unwrap();
        assert_eq!(
            Some(22),
            meta.database.as_ref().map(|grant| grant.database_id)
        );
        assert!(meta.tables.is_empty());
        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(DatabaseId::new(22), DatabaseMeta {
                drop_on: Some(Utc::now()),
                ..Default::default()
            }))
            .await
            .unwrap();
        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(
                DatabaseId::new(11),
                DatabaseMeta::default(),
            ))
            .await
            .unwrap();
        manager
            .grant_database(&provider, "sales", database_grant(&manager, "db", 11).await)
            .await
            .unwrap();
        manager
            .grant_table(
                &provider,
                "sales",
                table_grant(&manager, "db", 11, "orders", 101).await,
                "share_conn".to_string(),
            )
            .await
            .unwrap();

        manager
            .add_accounts(
                &provider,
                "sales",
                vec!["consumer".to_string()],
                "share_conn".to_string(),
                false,
            )
            .await
            .unwrap();
        manager
            .remove_accounts(
                &provider,
                "sales",
                vec!["other".to_string()],
                Some("share_conn".to_string()),
                false,
            )
            .await
            .unwrap();
        manager
            .resolve_shared_table(&consumer, &binding, "orders")
            .await
            .unwrap();

        manager
            .add_accounts(
                &provider,
                "missing",
                vec!["consumer".to_string()],
                "unused".to_string(),
                true,
            )
            .await
            .unwrap();
        assert!(
            manager
                .add_accounts(
                    &provider,
                    "missing",
                    vec!["consumer".to_string()],
                    "unused".to_string(),
                    false,
                )
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connection_reference_survives_revoke_and_can_be_replaced() {
        let (_store, manager) = manager().await;
        let binding = create_granted_share(&manager).await;
        let provider = tenant("provider");
        let consumer = tenant("consumer");

        revoke_table(
            &manager,
            &provider,
            "sales",
            Some(ProviderObjectIds {
                database_id: 11,
                table_id: 101,
            }),
        )
        .await;
        assert!(
            manager
                .resolve_shared_table(&consumer, &binding, "orders")
                .await
                .is_err()
        );
        assert_eq!(
            Some("share_conn".to_string()),
            manager
                .get_share("provider", "sales")
                .await
                .unwrap()
                .1
                .connection
        );

        manager
            .grant_table(
                &provider,
                "sales",
                table_grant(&manager, "db", 11, "items", 102).await,
                "share_conn".to_string(),
            )
            .await
            .unwrap();
        seed_table_name(&manager, 11, 102, "items").await;
        manager
            .set_share(
                &provider,
                "sales",
                SetShareRequest::connection(
                    None,
                    None,
                    "replacement_conn".to_string(),
                    BTreeSet::from([102]),
                    false,
                ),
            )
            .await
            .unwrap();
        let table = manager
            .resolve_shared_table(&consumer, &binding, "items")
            .await
            .unwrap();
        assert_eq!("replacement_conn", table.connection);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn revoke_matches_stable_object_ids_after_rename() {
        let (_store, manager) = manager().await;
        let binding = create_granted_share(&manager).await;
        let provider = tenant("provider");
        let consumer = tenant("consumer");

        revoke_database(&manager, &provider, "sales", Some(11)).await;
        let (_, meta) = manager.get_share("provider", "sales").await.unwrap();
        assert!(meta.database.is_none());
        assert!(meta.tables.is_empty());

        manager
            .grant_database(
                &provider,
                "sales",
                database_grant(&manager, "renamed_db", 11).await,
            )
            .await
            .unwrap();
        seed_database_name(&manager, 11, "renamed_db").await;
        manager
            .grant_table(
                &provider,
                "sales",
                table_grant(&manager, "renamed_db", 11, "old_table_name", 101).await,
                "share_conn".to_string(),
            )
            .await
            .unwrap();
        seed_table_name(&manager, 11, 101, "renamed_table").await;
        manager
            .grant_table(
                &provider,
                "sales",
                table_grant(&manager, "renamed_db", 11, "renamed_table", 101).await,
                "share_conn".to_string(),
            )
            .await
            .unwrap();
        let (_, meta) = manager.get_share("provider", "sales").await.unwrap();
        assert!(meta.tables.contains_key(&101));
        assert_eq!(
            "renamed_table",
            manager
                .resolve_shared_table(&consumer, &binding, "renamed_table")
                .await
                .unwrap()
                .provider_table
        );
        revoke_table(
            &manager,
            &provider,
            "sales",
            Some(ProviderObjectIds {
                database_id: 11,
                table_id: 101,
            }),
        )
        .await;

        let (_, meta) = manager.get_share("provider", "sales").await.unwrap();
        assert!(meta.tables.is_empty());

        revoke_database(&manager, &provider, "sales", None).await;
        let (_, meta) = manager.get_share("provider", "sales").await.unwrap();
        assert!(meta.database.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mutations_reject_a_connection_changed_after_validation() {
        let (_store, manager) = manager().await;
        create_granted_share(&manager).await;
        let provider = tenant("provider");

        assert!(
            manager
                .grant_table(
                    &provider,
                    "sales",
                    table_grant(&manager, "db", 11, "items", 102).await,
                    "replacement_conn".to_string(),
                )
                .await
                .is_err()
        );
        assert!(
            manager
                .add_accounts(
                    &provider,
                    "sales",
                    vec!["new_consumer".to_string()],
                    "replacement_conn".to_string(),
                    false,
                )
                .await
                .is_err()
        );
        assert!(
            manager
                .set_share(
                    &provider,
                    "sales",
                    SetShareRequest::accounts(
                        vec!["new_consumer".to_string()],
                        None,
                        "replacement_conn".to_string(),
                        false,
                    ),
                )
                .await
                .is_err()
        );

        let (_, meta) = manager.get_share("provider", "sales").await.unwrap();
        assert!(!meta.tables.contains_key(&102));
        assert!(!meta.accounts.contains("new_consumer"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replacing_connection_rejects_changed_grant_set() {
        let (_store, manager) = manager().await;
        create_granted_share(&manager).await;
        let provider = tenant("provider");

        let result = manager
            .set_share(
                &provider,
                "sales",
                SetShareRequest::connection(
                    None,
                    None,
                    "replacement_conn".to_string(),
                    BTreeSet::from([999]),
                    false,
                ),
            )
            .await;
        assert!(result.is_err());
        assert_eq!(
            Some("share_conn".to_string()),
            manager
                .get_share("provider", "sales")
                .await
                .unwrap()
                .1
                .connection
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepared_revoke_does_not_remove_a_different_grant() {
        let (_store, manager) = manager().await;
        create_granted_share(&manager).await;
        let provider = tenant("provider");
        let target = manager
            .prepare_revoke_table(
                &provider,
                "sales",
                Some(ProviderObjectIds {
                    database_id: 11,
                    table_id: 101,
                }),
            )
            .await
            .unwrap()
            .unwrap();

        manager
            .grant_table(
                &provider,
                "sales",
                table_grant(&manager, "db", 11, "orders", 102).await,
                "share_conn".to_string(),
            )
            .await
            .unwrap();
        manager
            .revoke_share_object(&provider, "sales", target)
            .await
            .unwrap();

        let (_, meta) = manager.get_share("provider", "sales").await.unwrap();
        assert!(meta.tables.contains_key(&102));
        assert!(!meta.tables.contains_key(&101));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepared_grant_rejects_object_state_changes() {
        let (_store, manager) = manager().await;
        let provider = tenant("provider");
        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(
                DatabaseId::new(11),
                DatabaseMeta::default(),
            ))
            .await
            .unwrap();
        manager
            .create_share(
                &provider,
                CreateOption::Create,
                "sales",
                Some("share_conn".to_string()),
                None,
            )
            .await
            .unwrap();

        let stale_database_grant = database_grant(&manager, "db", 11).await;
        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(
                DatabaseId::new(11),
                DatabaseMeta::default(),
            ))
            .await
            .unwrap();
        assert!(
            manager
                .grant_database(&provider, "sales", stale_database_grant)
                .await
                .is_err()
        );
        assert!(
            manager
                .get_share("provider", "sales")
                .await
                .unwrap()
                .1
                .database
                .is_none()
        );

        manager
            .grant_database(&provider, "sales", database_grant(&manager, "db", 11).await)
            .await
            .unwrap();
        let stale_table_grant = table_grant(&manager, "db", 11, "orders", 101).await;
        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(TableId::new(101), TableMeta::default()))
            .await
            .unwrap();
        assert!(
            manager
                .grant_table(
                    &provider,
                    "sales",
                    stale_table_grant,
                    "share_conn".to_string(),
                )
                .await
                .is_err()
        );
        assert!(
            manager
                .get_share("provider", "sales")
                .await
                .unwrap()
                .1
                .tables
                .is_empty()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepared_revoke_rejects_object_state_changes() {
        let (_store, manager) = manager().await;
        create_granted_share(&manager).await;
        let provider = tenant("provider");
        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(TableId::new(101), TableMeta::default()))
            .await
            .unwrap();
        let target = manager
            .prepare_revoke_table(
                &provider,
                "sales",
                Some(ProviderObjectIds {
                    database_id: 11,
                    table_id: 101,
                }),
            )
            .await
            .unwrap()
            .unwrap();
        let dropped_table = TableMeta {
            drop_on: Some(Utc::now()),
            ..Default::default()
        };
        manager
            .kv_api
            .upsert_pb(&UpsertPB::update(TableId::new(101), dropped_table))
            .await
            .unwrap();

        assert!(
            manager
                .revoke_share_object(&provider, "sales", target)
                .await
                .is_err()
        );
        let (_, meta) = manager.get_share("provider", "sales").await.unwrap();
        assert!(meta.tables.contains_key(&101));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_managers_do_not_lose_account_updates() {
        let store = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let first = ShareMgr::create(Arc::new(store.clone()));
        let second = ShareMgr::create(Arc::new(store));
        let provider = tenant("provider");
        first
            .create_share(
                &provider,
                CreateOption::Create,
                "sales",
                Some("share_conn".to_string()),
                None,
            )
            .await
            .unwrap();

        let (first_result, second_result) = tokio::join!(
            first.add_accounts(
                &provider,
                "sales",
                vec!["consumer_a".to_string()],
                "share_conn".to_string(),
                false,
            ),
            second.add_accounts(
                &provider,
                "sales",
                vec!["consumer_b".to_string()],
                "share_conn".to_string(),
                false,
            ),
        );
        first_result.unwrap();
        second_result.unwrap();

        let (_, meta) = first.get_share("provider", "sales").await.unwrap();
        assert_eq!(
            BTreeSet::from(["consumer_a".to_string(), "consumer_b".to_string()]),
            meta.accounts
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn show_and_describe_share_use_persisted_meta() {
        let (_store, manager) = manager().await;
        create_granted_share(&manager).await;
        let provider = tenant("provider");
        let consumer = tenant("consumer");

        let outbound = manager
            .show_shares(&provider, Some("sale%"), Some(1))
            .await
            .unwrap();
        assert_eq!(1, outbound.len());
        assert_eq!("OUTBOUND", outbound[0].kind);
        assert_eq!("consumer", outbound[0].to);
        assert_eq!("db", outbound[0].database_name);

        let inbound = manager.show_shares(&consumer, None, None).await.unwrap();
        assert_eq!(1, inbound.len());
        assert_eq!("INBOUND", inbound[0].kind);
        assert_eq!("provider", inbound[0].owner_account);

        let described = manager
            .describe_share(&consumer, Some("provider"), "sales")
            .await
            .unwrap();
        assert_eq!(2, described.len());
        assert_eq!("DATABASE", described[0].kind);
        assert_eq!("db", described[0].name);
        assert_eq!("TABLE", described[1].kind);
        assert_eq!("db.orders", described[1].name);
    }
}
