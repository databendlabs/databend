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
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::generate_like_pattern;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;

pub const SHARE_ENGINE: &str = "share";
pub const SHARE_OPT_PROVIDER_TENANT: &str = "provider_tenant";
pub const SHARE_OPT_SHARE_NAME: &str = "share_name";
pub const SHARE_OPT_SHARE_ID: &str = "share_id";
pub const SHARE_OPT_ACCOUNT_GENERATION: &str = "account_generation";
pub const SHARE_OPT_PROVIDER_DATABASE: &str = "provider_database";
pub const SHARE_OPT_PROVIDER_DATABASE_ID: &str = "provider_database_id";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareDatabaseBinding {
    pub provider_tenant: String,
    pub share_name: String,
    pub share_id: u64,
    pub account_generation: u64,
    pub provider_database: String,
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
                SHARE_OPT_ACCOUNT_GENERATION.to_string(),
                self.account_generation.to_string(),
            ),
            (
                SHARE_OPT_PROVIDER_DATABASE.to_string(),
                self.provider_database.clone(),
            ),
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
            account_generation: parse_u64(SHARE_OPT_ACCOUNT_GENERATION)?,
            provider_database: required(SHARE_OPT_PROVIDER_DATABASE)?,
            provider_database_id: parse_u64(SHARE_OPT_PROVIDER_DATABASE_ID)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareTableContext {
    pub provider_tenant: String,
    pub share_name: String,
    pub share_id: u64,
    pub provider_database: String,
    pub provider_database_id: u64,
    pub provider_table: String,
    pub provider_table_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareCredential {
    pub storage_params: StorageParams,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareGrantDatabase {
    pub database: String,
    pub database_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShareGrantTable {
    pub database: String,
    pub database_id: u64,
    pub table: String,
    pub table_id: u64,
    pub storage_params: StorageParams,
}

#[async_trait]
pub trait ShareService: Send + Sync {
    async fn create_share(
        &self,
        provider: &Tenant,
        create_option: CreateOption,
        share: &str,
        comment: Option<String>,
    ) -> Result<()>;

    async fn drop_share(&self, provider: &Tenant, share: &str) -> Result<()>;

    async fn add_accounts(
        &self,
        provider: &Tenant,
        share: &str,
        accounts: Vec<String>,
        if_exists: bool,
    ) -> Result<()>;

    async fn remove_accounts(
        &self,
        provider: &Tenant,
        share: &str,
        accounts: Vec<String>,
        if_exists: bool,
    ) -> Result<()>;

    async fn set_share(
        &self,
        provider: &Tenant,
        share: &str,
        accounts: Option<Vec<String>>,
        comment: Option<String>,
        if_exists: bool,
    ) -> Result<()>;

    async fn grant_database(
        &self,
        provider: &Tenant,
        share: &str,
        grant: ShareGrantDatabase,
    ) -> Result<()>;

    async fn revoke_database(&self, provider: &Tenant, share: &str, database: &str) -> Result<()>;

    async fn grant_table(
        &self,
        provider: &Tenant,
        share: &str,
        grant: ShareGrantTable,
    ) -> Result<()>;

    async fn revoke_table(
        &self,
        provider: &Tenant,
        share: &str,
        database: &str,
        table: &str,
    ) -> Result<()>;

    async fn show_shares(
        &self,
        tenant: &Tenant,
        like: Option<&str>,
        limit: Option<u64>,
    ) -> Result<Vec<ShareShowEntry>>;

    async fn describe_share(
        &self,
        tenant: &Tenant,
        provider_tenant: Option<&str>,
        share: &str,
    ) -> Result<Vec<ShareDescEntry>>;

    async fn bind_share_database(
        &self,
        consumer: &Tenant,
        provider_tenant: &str,
        share: &str,
    ) -> Result<ShareDatabaseBinding>;

    async fn resolve_shared_table(
        &self,
        consumer: &Tenant,
        binding: &ShareDatabaseBinding,
        table: &str,
    ) -> Result<ShareTableContext>;

    async fn list_shared_tables(
        &self,
        consumer: &Tenant,
        binding: &ShareDatabaseBinding,
    ) -> Result<Vec<ShareTableContext>>;

    async fn get_share_credential(
        &self,
        provider_tenant: &str,
        share_id: u64,
    ) -> Result<ShareCredential>;
}

pub fn share_service() -> Arc<dyn ShareService> {
    static INSTANCE: LazyLock<Arc<dyn ShareService>> =
        LazyLock::new(|| Arc::new(MockShareService::default()));
    INSTANCE.clone()
}

#[derive(Default)]
struct MockShareService {
    state: Mutex<MockShareState>,
}

#[derive(Default)]
struct MockShareState {
    next_share_id: u64,
    shares: HashMap<(String, String), MockShareMeta>,
}

struct MockShareMeta {
    share_id: u64,
    created_on: String,
    comment: Option<String>,
    active_accounts: HashSet<String>,
    account_generations: HashMap<String, u64>,
    database: Option<MockDatabaseGrant>,
    tables: HashMap<(String, String), MockTableGrant>,
    credential_storage_params: Option<StorageParams>,
}

#[derive(Clone)]
struct MockDatabaseGrant {
    database: String,
    database_id: u64,
    shared_on: String,
}

#[derive(Clone)]
struct MockTableGrant {
    database: String,
    database_id: u64,
    table: String,
    table_id: u64,
    storage_params: StorageParams,
    shared_on: String,
}

#[async_trait]
impl ShareService for MockShareService {
    async fn create_share(
        &self,
        provider: &Tenant,
        create_option: CreateOption,
        share: &str,
        comment: Option<String>,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let key = share_key(provider.tenant_name(), share);

        if state.shares.contains_key(&key) {
            match create_option {
                CreateOption::Create => {
                    return Err(ErrorCode::BadArguments(format!(
                        "Share '{}' already exists",
                        share
                    )));
                }
                CreateOption::CreateIfNotExists => return Ok(()),
                CreateOption::CreateOrReplace => {
                    state.shares.remove(&key);
                }
            }
        }

        state.next_share_id += 1;
        let share_id = state.next_share_id;
        state.shares.insert(key, MockShareMeta {
            share_id,
            created_on: now(),
            comment,
            active_accounts: HashSet::new(),
            account_generations: HashMap::new(),
            database: None,
            tables: HashMap::new(),
            credential_storage_params: None,
        });
        Ok(())
    }

    async fn drop_share(&self, provider: &Tenant, share: &str) -> Result<()> {
        let mut state = self.lock_state()?;
        let key = share_key(provider.tenant_name(), share);
        if state.shares.remove(&key).is_none() {
            return Err(unknown_share(provider.tenant_name(), share));
        }
        Ok(())
    }

    async fn add_accounts(
        &self,
        provider: &Tenant,
        share: &str,
        accounts: Vec<String>,
        if_exists: bool,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let meta = state.share_mut(provider.tenant_name(), share, if_exists)?;
        let Some(meta) = meta else {
            return Ok(());
        };
        for account in accounts {
            activate_account(meta, account);
        }
        Ok(())
    }

    async fn remove_accounts(
        &self,
        provider: &Tenant,
        share: &str,
        accounts: Vec<String>,
        if_exists: bool,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let meta = state.share_mut(provider.tenant_name(), share, if_exists)?;
        let Some(meta) = meta else {
            return Ok(());
        };

        for account in accounts {
            if meta.active_accounts.remove(&account) {
                bump_generation(meta, &account);
            }
        }
        Ok(())
    }

    async fn set_share(
        &self,
        provider: &Tenant,
        share: &str,
        accounts: Option<Vec<String>>,
        comment: Option<String>,
        if_exists: bool,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let meta = state.share_mut(provider.tenant_name(), share, if_exists)?;
        let Some(meta) = meta else {
            return Ok(());
        };

        if let Some(accounts) = accounts {
            let new_accounts = accounts.into_iter().collect::<HashSet<_>>();
            let old_accounts = meta.active_accounts.clone();
            for account in old_accounts.difference(&new_accounts) {
                meta.active_accounts.remove(account);
                bump_generation(meta, account);
            }
            for account in new_accounts {
                if !meta.active_accounts.contains(&account) {
                    activate_account(meta, account);
                }
            }
        }

        if comment.is_some() {
            meta.comment = comment;
        }
        Ok(())
    }

    async fn grant_database(
        &self,
        provider: &Tenant,
        share: &str,
        grant: ShareGrantDatabase,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let meta = state
            .share_mut(provider.tenant_name(), share, false)?
            .unwrap();

        if let Some(existing) = &meta.database {
            if existing.database_id != grant.database_id {
                return Err(ErrorCode::BadArguments(format!(
                    "Share '{}' already has USAGE on database '{}'",
                    share, existing.database
                )));
            }
        }

        meta.database = Some(MockDatabaseGrant {
            database: grant.database,
            database_id: grant.database_id,
            shared_on: now(),
        });
        Ok(())
    }

    async fn revoke_database(&self, provider: &Tenant, share: &str, database: &str) -> Result<()> {
        let mut state = self.lock_state()?;
        let meta = state
            .share_mut(provider.tenant_name(), share, false)?
            .unwrap();
        if meta
            .database
            .as_ref()
            .map(|grant| grant.database == database)
            .unwrap_or(false)
        {
            meta.database = None;
            meta.tables.clear();
            meta.credential_storage_params = None;
        }
        Ok(())
    }

    async fn grant_table(
        &self,
        provider: &Tenant,
        share: &str,
        grant: ShareGrantTable,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let meta = state
            .share_mut(provider.tenant_name(), share, false)?
            .unwrap();

        let Some(database) = &meta.database else {
            return Err(ErrorCode::BadArguments(format!(
                "Grant USAGE on database '{}' to share '{}' before granting tables",
                grant.database, share
            )));
        };

        if database.database_id != grant.database_id {
            return Err(ErrorCode::BadArguments(format!(
                "Cannot grant table '{}.{}' from a different database to share '{}'",
                grant.database, grant.table, share
            )));
        }

        let key = (grant.database.clone(), grant.table.clone());
        if meta.tables.iter().any(|(table_key, table)| {
            table_key != &key && table.storage_params != grant.storage_params
        }) {
            return Err(ErrorCode::InvalidOperation(format!(
                "Share '{}' credential must cover all granted tables",
                share
            )));
        }

        meta.tables.insert(key, MockTableGrant {
            database: grant.database,
            database_id: grant.database_id,
            table: grant.table,
            table_id: grant.table_id,
            storage_params: grant.storage_params,
            shared_on: now(),
        });
        refresh_credential_storage_params(meta);
        Ok(())
    }

    async fn revoke_table(
        &self,
        provider: &Tenant,
        share: &str,
        database: &str,
        table: &str,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        let meta = state
            .share_mut(provider.tenant_name(), share, false)?
            .unwrap();
        meta.tables
            .remove(&(database.to_string(), table.to_string()));
        refresh_credential_storage_params(meta);
        Ok(())
    }

    async fn show_shares(
        &self,
        tenant: &Tenant,
        like: Option<&str>,
        limit: Option<u64>,
    ) -> Result<Vec<ShareShowEntry>> {
        let state = self.lock_state()?;
        let tenant_name = tenant.tenant_name();
        let mut rows = Vec::new();

        for ((provider, share), meta) in &state.shares {
            if provider == tenant_name {
                rows.push(show_entry("OUTBOUND", provider, share, meta));
            } else if meta.active_accounts.contains(tenant_name) {
                rows.push(show_entry("INBOUND", provider, share, meta));
            }
        }

        rows.sort_by(|a, b| {
            a.kind
                .cmp(&b.kind)
                .then(a.owner_account.cmp(&b.owner_account))
                .then(a.name.cmp(&b.name))
        });
        if let Some(pattern) = like {
            rows.retain(|row| like_match(pattern, &row.name));
        }
        if let Some(limit) = limit {
            rows.truncate(limit as usize);
        }
        Ok(rows)
    }

    async fn describe_share(
        &self,
        tenant: &Tenant,
        provider_tenant: Option<&str>,
        share: &str,
    ) -> Result<Vec<ShareDescEntry>> {
        let state = self.lock_state()?;
        let provider = provider_tenant.unwrap_or_else(|| tenant.tenant_name());
        let meta = state.share(provider, share)?;

        if provider != tenant.tenant_name() {
            ensure_account(meta, tenant.tenant_name())?;
        }

        Ok(desc_entries(meta))
    }

    async fn bind_share_database(
        &self,
        consumer: &Tenant,
        provider_tenant: &str,
        share: &str,
    ) -> Result<ShareDatabaseBinding> {
        let state = self.lock_state()?;
        let meta = state.share(provider_tenant, share)?;
        let account_generation = ensure_account(meta, consumer.tenant_name())?;
        let database = meta.database.as_ref().ok_or_else(|| {
            ErrorCode::BadArguments(format!(
                "Share '{}.{}' does not grant a database",
                provider_tenant, share
            ))
        })?;

        Ok(ShareDatabaseBinding {
            provider_tenant: provider_tenant.to_string(),
            share_name: share.to_string(),
            share_id: meta.share_id,
            account_generation,
            provider_database: database.database.clone(),
            provider_database_id: database.database_id,
        })
    }

    async fn resolve_shared_table(
        &self,
        consumer: &Tenant,
        binding: &ShareDatabaseBinding,
        table: &str,
    ) -> Result<ShareTableContext> {
        let state = self.lock_state()?;
        let meta = state.share(&binding.provider_tenant, &binding.share_name)?;
        validate_binding(consumer, binding, meta)?;
        let table = meta
            .tables
            .get(&(binding.provider_database.clone(), table.to_string()))
            .ok_or_else(|| {
                ErrorCode::UnknownTable(format!(
                    "Table '{}' is not granted by share '{}.{}'",
                    table, binding.provider_tenant, binding.share_name
                ))
            })?;
        Ok(table_context(binding, table))
    }

    async fn list_shared_tables(
        &self,
        consumer: &Tenant,
        binding: &ShareDatabaseBinding,
    ) -> Result<Vec<ShareTableContext>> {
        let state = self.lock_state()?;
        let meta = state.share(&binding.provider_tenant, &binding.share_name)?;
        validate_binding(consumer, binding, meta)?;
        let mut tables = meta
            .tables
            .values()
            .map(|table| table_context(binding, table))
            .collect::<Vec<_>>();
        tables.sort_by(|a, b| a.provider_table.cmp(&b.provider_table));
        Ok(tables)
    }

    async fn get_share_credential(
        &self,
        provider_tenant: &str,
        share_id: u64,
    ) -> Result<ShareCredential> {
        let state = self.lock_state()?;
        let share = state
            .shares
            .iter()
            .find_map(|((provider, _), meta)| {
                (provider == provider_tenant && meta.share_id == share_id).then_some(meta)
            })
            .ok_or_else(|| {
                ErrorCode::InvalidOperation(format!(
                    "Share credential is unavailable for provider '{}' share id {}",
                    provider_tenant, share_id
                ))
            })?;

        let storage_params = share.credential_storage_params.clone().ok_or_else(|| {
            ErrorCode::InvalidOperation(format!(
                "Share credential is unavailable for provider '{}' share id {}: no provider table has been granted",
                provider_tenant, share_id
            ))
        })?;

        Ok(ShareCredential { storage_params })
    }
}

impl MockShareService {
    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, MockShareState>> {
        self.state
            .lock()
            .map_err(|_| ErrorCode::Internal("Mock share service lock is poisoned"))
    }
}

impl MockShareState {
    fn share(&self, provider: &str, share: &str) -> Result<&MockShareMeta> {
        self.shares
            .get(&share_key(provider, share))
            .ok_or_else(|| unknown_share(provider, share))
    }

    fn share_mut(
        &mut self,
        provider: &str,
        share: &str,
        if_exists: bool,
    ) -> Result<Option<&mut MockShareMeta>> {
        match self.shares.get_mut(&share_key(provider, share)) {
            Some(meta) => Ok(Some(meta)),
            None if if_exists => Ok(None),
            None => Err(unknown_share(provider, share)),
        }
    }
}

fn share_key(provider: &str, share: &str) -> (String, String) {
    (provider.to_string(), share.to_string())
}

fn unknown_share(provider: &str, share: &str) -> ErrorCode {
    ErrorCode::BadArguments(format!("Unknown share '{}.{}'", provider, share))
}

fn now() -> String {
    Utc::now().to_rfc3339()
}

fn bump_generation(meta: &mut MockShareMeta, account: &str) -> u64 {
    let generation = meta
        .account_generations
        .entry(account.to_string())
        .or_insert(0);
    *generation += 1;
    *generation
}

fn activate_account(meta: &mut MockShareMeta, account: String) {
    if meta.active_accounts.insert(account.clone()) {
        bump_generation(meta, &account);
    }
}

fn refresh_credential_storage_params(meta: &mut MockShareMeta) {
    meta.credential_storage_params = meta
        .tables
        .values()
        .next()
        .map(|table| table.storage_params.clone());
}

fn ensure_account(meta: &MockShareMeta, account: &str) -> Result<u64> {
    if !meta.active_accounts.contains(account) {
        return Err(ErrorCode::InvalidOperation(format!(
            "Account '{}' is not authorized for share id {}",
            account, meta.share_id
        )));
    }
    Ok(*meta.account_generations.get(account).unwrap_or(&0))
}

fn validate_binding(
    consumer: &Tenant,
    binding: &ShareDatabaseBinding,
    meta: &MockShareMeta,
) -> Result<()> {
    if meta.share_id != binding.share_id {
        return Err(stale_binding("share was dropped or recreated"));
    }

    let account_generation = ensure_account(meta, consumer.tenant_name())?;
    if account_generation != binding.account_generation {
        return Err(stale_binding("consumer account was removed or re-added"));
    }

    let database = meta.database.as_ref().ok_or_else(|| {
        ErrorCode::InvalidOperation(format!(
            "Share '{}.{}' no longer grants a database",
            binding.provider_tenant, binding.share_name
        ))
    })?;
    if database.database_id != binding.provider_database_id {
        return Err(stale_binding("provider database grant changed"));
    }
    Ok(())
}

fn stale_binding(reason: &str) -> ErrorCode {
    ErrorCode::InvalidOperation(format!(
        "Shared database binding is no longer valid: {reason}"
    ))
}

fn table_context(binding: &ShareDatabaseBinding, table: &MockTableGrant) -> ShareTableContext {
    ShareTableContext {
        provider_tenant: binding.provider_tenant.clone(),
        share_name: binding.share_name.clone(),
        share_id: binding.share_id,
        provider_database: table.database.clone(),
        provider_database_id: table.database_id,
        provider_table: table.table.clone(),
        provider_table_id: table.table_id,
    }
}

fn show_entry(kind: &str, provider: &str, share: &str, meta: &MockShareMeta) -> ShareShowEntry {
    let database_name = meta
        .database
        .as_ref()
        .map(|db| db.database.clone())
        .unwrap_or_default();
    let mut accounts = meta.active_accounts.iter().cloned().collect::<Vec<_>>();
    accounts.sort();

    ShareShowEntry {
        created_on: meta.created_on.clone(),
        kind: kind.to_string(),
        owner_account: provider.to_string(),
        name: share.to_string(),
        database_name,
        to: if kind == "OUTBOUND" {
            accounts.join(", ")
        } else {
            String::new()
        },
        owner: String::new(),
        comment: meta.comment.clone().unwrap_or_default(),
        listing_global_name: String::new(),
    }
}

fn desc_entries(meta: &MockShareMeta) -> Vec<ShareDescEntry> {
    let mut rows = Vec::new();
    if let Some(database) = &meta.database {
        rows.push(ShareDescEntry {
            kind: "DATABASE".to_string(),
            name: database.database.clone(),
            shared_on: database.shared_on.clone(),
        });
    }

    let mut tables = meta.tables.values().collect::<Vec<_>>();
    tables.sort_by(|a, b| a.table.cmp(&b.table));
    for table in tables {
        rows.push(ShareDescEntry {
            kind: "TABLE".to_string(),
            name: format!("{}.{}", table.database, table.table),
            shared_on: table.shared_on.clone(),
        });
    }
    rows
}

fn like_match(pattern: &str, value: &str) -> bool {
    generate_like_pattern(pattern.as_bytes(), value.len()).compare(value.as_bytes())
}

#[cfg(test)]
mod tests {
    use databend_common_meta_app::schema::CreateOption;
    use databend_common_meta_app::storage::StorageFsConfig;
    use databend_common_meta_app::tenant::Tenant;

    use super::*;

    fn tenant(name: &str) -> Tenant {
        Tenant::new_literal(name)
    }

    async fn create_granted_share(service: &MockShareService, share: &str) -> ShareDatabaseBinding {
        let provider = tenant("provider");
        let consumer = tenant("consumer");
        service
            .create_share(&provider, CreateOption::Create, share, None)
            .await
            .unwrap();
        service
            .grant_database(&provider, share, ShareGrantDatabase {
                database: "db1".to_string(),
                database_id: 11,
            })
            .await
            .unwrap();
        service
            .grant_table(&provider, share, ShareGrantTable {
                database: "db1".to_string(),
                database_id: 11,
                table: "t1".to_string(),
                table_id: 101,
                storage_params: StorageParams::Memory,
            })
            .await
            .unwrap();
        service
            .add_accounts(&provider, share, vec!["consumer".to_string()], false)
            .await
            .unwrap();
        service
            .bind_share_database(&consumer, "provider", share)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn mock_share_recreate_invalidates_old_binding() {
        let service = MockShareService::default();
        let provider = tenant("provider");
        let consumer = tenant("consumer");
        let old_binding = create_granted_share(&service, "s1").await;

        service.drop_share(&provider, "s1").await.unwrap();
        assert!(
            service
                .resolve_shared_table(&consumer, &old_binding, "t1")
                .await
                .is_err()
        );

        let new_binding = create_granted_share(&service, "s1").await;
        assert_ne!(old_binding.share_id, new_binding.share_id);
        assert!(
            service
                .resolve_shared_table(&consumer, &old_binding, "t1")
                .await
                .is_err()
        );
        service
            .resolve_shared_table(&consumer, &new_binding, "t1")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn mock_share_remove_readd_invalidates_old_binding() {
        let service = MockShareService::default();
        let provider = tenant("provider");
        let consumer = tenant("consumer");
        let old_binding = create_granted_share(&service, "s1").await;

        service
            .remove_accounts(&provider, "s1", vec!["consumer".to_string()], false)
            .await
            .unwrap();
        assert!(
            service
                .resolve_shared_table(&consumer, &old_binding, "t1")
                .await
                .is_err()
        );

        service
            .add_accounts(&provider, "s1", vec!["consumer".to_string()], false)
            .await
            .unwrap();
        assert!(
            service
                .resolve_shared_table(&consumer, &old_binding, "t1")
                .await
                .is_err()
        );

        let new_binding = service
            .bind_share_database(&consumer, "provider", "s1")
            .await
            .unwrap();
        assert_ne!(
            old_binding.account_generation,
            new_binding.account_generation
        );
        service
            .resolve_shared_table(&consumer, &new_binding, "t1")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn mock_share_rejects_second_database_and_cross_database_table() {
        let service = MockShareService::default();
        let provider = tenant("provider");
        create_granted_share(&service, "s1").await;

        assert!(
            service
                .grant_database(&provider, "s1", ShareGrantDatabase {
                    database: "db2".to_string(),
                    database_id: 22,
                },)
                .await
                .is_err()
        );

        assert!(
            service
                .grant_table(&provider, "s1", ShareGrantTable {
                    database: "db2".to_string(),
                    database_id: 22,
                    table: "t2".to_string(),
                    table_id: 202,
                    storage_params: StorageParams::Memory,
                },)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn mock_share_add_existing_account_is_noop() {
        let service = MockShareService::default();
        let provider = tenant("provider");
        let consumer = tenant("consumer");
        let binding = create_granted_share(&service, "s1").await;

        service
            .add_accounts(&provider, "s1", vec!["consumer".to_string()], false)
            .await
            .unwrap();

        let rebound = service
            .bind_share_database(&consumer, "provider", "s1")
            .await
            .unwrap();
        assert_eq!(binding.account_generation, rebound.account_generation);
        service
            .resolve_shared_table(&consumer, &binding, "t1")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn mock_share_add_if_exists_missing_share_is_noop() {
        let service = MockShareService::default();
        let provider = tenant("provider");

        service
            .add_accounts(&provider, "missing", vec!["consumer".to_string()], true)
            .await
            .unwrap();
        assert!(
            service
                .add_accounts(&provider, "missing", vec!["consumer".to_string()], false)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn mock_share_remove_inactive_account_is_noop() {
        let service = MockShareService::default();
        let provider = tenant("provider");
        let consumer = tenant("consumer");
        let binding = create_granted_share(&service, "s1").await;

        service
            .remove_accounts(&provider, "s1", vec!["other".to_string()], false)
            .await
            .unwrap();

        let rebound = service
            .bind_share_database(&consumer, "provider", "s1")
            .await
            .unwrap();
        assert_eq!(binding.account_generation, rebound.account_generation);
        service
            .resolve_shared_table(&consumer, &binding, "t1")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn mock_share_rejects_multiple_table_storage_params() {
        let service = MockShareService::default();
        let provider = tenant("provider");
        create_granted_share(&service, "s1").await;

        assert!(
            service
                .grant_table(&provider, "s1", ShareGrantTable {
                    database: "db1".to_string(),
                    database_id: 11,
                    table: "t2".to_string(),
                    table_id: 102,
                    storage_params: StorageParams::Fs(StorageFsConfig::default()),
                })
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn mock_share_revoke_resets_credential_storage_params() {
        let service = MockShareService::default();
        let provider = tenant("provider");
        create_granted_share(&service, "s1").await;

        service
            .revoke_table(&provider, "s1", "db1", "t1")
            .await
            .unwrap();
        assert!(service.get_share_credential("provider", 1).await.is_err());

        let storage_params = StorageParams::Fs(StorageFsConfig::default());
        service
            .grant_table(&provider, "s1", ShareGrantTable {
                database: "db1".to_string(),
                database_id: 11,
                table: "t2".to_string(),
                table_id: 102,
                storage_params: storage_params.clone(),
            })
            .await
            .unwrap();

        let credential = service.get_share_credential("provider", 1).await.unwrap();
        assert_eq!(credential.storage_params, storage_params);
    }

    #[test]
    fn show_shares_like_supports_sql_wildcards() {
        assert!(like_match("share_e2_", "share_e2e"));
        assert!(like_match("share%", "share_e2e"));
        assert!(!like_match("share_e2_", "share_e2"));
    }
}
