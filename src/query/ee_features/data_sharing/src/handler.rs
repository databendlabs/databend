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

use std::collections::BTreeSet;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::types::MetaError;

use crate::types::*;

pub type ShareMetaStore = Arc<dyn KVApi<Error = MetaError>>;

/// Enterprise implementation of sharing operations, using the caller's metastore.
#[async_trait::async_trait]
pub trait DataSharingHandler: Send + Sync {
    fn ensure_provider_table_can_be_shared(&self, meta: &TableMeta) -> Result<()>;
    async fn create_share(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        create_option: CreateOption,
        share: &str,
        connection: Option<String>,
        comment: Option<String>,
    ) -> Result<()>;

    async fn drop_share(&self, meta: ShareMetaStore, provider: &Tenant, share: &str) -> Result<()>;

    async fn add_accounts(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        accounts: Vec<String>,
        expected_connection: String,
        if_exists: bool,
    ) -> Result<()>;

    async fn remove_accounts(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        accounts: Vec<String>,
        expected_connection: Option<String>,
        if_exists: bool,
    ) -> Result<()>;

    async fn set_share(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        request: SetShareRequest,
    ) -> Result<()>;

    async fn grant_database(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        grant: ShareGrantDatabase,
    ) -> Result<()>;

    async fn prepare_revoke_database(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        current_database_id: Option<u64>,
    ) -> Result<Option<ShareRevokeTarget>>;

    async fn grant_table(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        grant: ShareGrantTable,
        expected_connection: String,
    ) -> Result<()>;

    async fn prepare_revoke_table(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        current_object_ids: Option<ProviderObjectIds>,
    ) -> Result<Option<ShareRevokeTarget>>;

    async fn revoke_share_object(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        target: ShareRevokeTarget,
    ) -> Result<()>;

    async fn show_shares(
        &self,
        meta: ShareMetaStore,
        tenant: &Tenant,
        like_pattern: Option<&str>,
        limit: Option<u64>,
    ) -> Result<Vec<ShareShowEntry>>;

    async fn describe_share(
        &self,
        meta: ShareMetaStore,
        tenant: &Tenant,
        provider_tenant: Option<&str>,
        share: &str,
    ) -> Result<Vec<ShareDescEntry>>;

    async fn exists(&self, meta: ShareMetaStore, provider: &Tenant, share: &str) -> Result<bool>;

    async fn get_connection_name(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
    ) -> Result<String>;

    async fn get_connection_name_if_exists(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
    ) -> Result<Option<String>>;

    async fn get_granted_table_ids(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
    ) -> Result<BTreeSet<u64>>;

    async fn bind_share_database(
        &self,
        meta: ShareMetaStore,
        consumer: &Tenant,
        provider_tenant: &str,
        share: &str,
    ) -> Result<ShareDatabaseBinding>;

    async fn resolve_shared_table(
        &self,
        meta: ShareMetaStore,
        consumer: &Tenant,
        binding: &ShareDatabaseBinding,
        table: &str,
    ) -> Result<ShareTableContext>;

    async fn list_shared_tables(
        &self,
        meta: ShareMetaStore,
        consumer: &Tenant,
        binding: &ShareDatabaseBinding,
    ) -> Result<Vec<ShareTableContext>>;

    async fn resolve_share_storage_params(
        &self,
        provider: &Tenant,
        connection_name: &str,
        provider_storage: StorageParams,
    ) -> Result<StorageParams>;

    async fn get_shared_table_info(
        &self,
        meta: ShareMetaStore,
        database: &str,
        context: ShareTableContext,
    ) -> Result<TableInfo>;
}

pub struct DataSharingHandlerWrapper {
    handler: Arc<dyn DataSharingHandler>,
    meta: ShareMetaStore,
}

impl DataSharingHandlerWrapper {
    pub fn ensure_provider_table_can_be_shared(&self, meta: &TableMeta) -> Result<()> {
        self.handler.ensure_provider_table_can_be_shared(meta)
    }

    pub async fn create_share(
        &self,
        provider: &Tenant,
        create_option: CreateOption,
        share: &str,
        connection: Option<String>,
        comment: Option<String>,
    ) -> Result<()> {
        self.handler
            .create_share(
                self.meta.clone(),
                provider,
                create_option,
                share,
                connection,
                comment,
            )
            .await
    }

    pub async fn drop_share(&self, provider: &Tenant, share: &str) -> Result<()> {
        self.handler
            .drop_share(self.meta.clone(), provider, share)
            .await
    }

    pub async fn add_accounts(
        &self,
        provider: &Tenant,
        share: &str,
        accounts: Vec<String>,
        expected_connection: String,
        if_exists: bool,
    ) -> Result<()> {
        self.handler
            .add_accounts(
                self.meta.clone(),
                provider,
                share,
                accounts,
                expected_connection,
                if_exists,
            )
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
        self.handler
            .remove_accounts(
                self.meta.clone(),
                provider,
                share,
                accounts,
                expected_connection,
                if_exists,
            )
            .await
    }

    pub async fn set_share(
        &self,
        provider: &Tenant,
        share: &str,
        request: SetShareRequest,
    ) -> Result<()> {
        self.handler
            .set_share(self.meta.clone(), provider, share, request)
            .await
    }

    pub async fn grant_database(
        &self,
        provider: &Tenant,
        share: &str,
        grant: ShareGrantDatabase,
    ) -> Result<()> {
        self.handler
            .grant_database(self.meta.clone(), provider, share, grant)
            .await
    }

    pub async fn prepare_revoke_database(
        &self,
        provider: &Tenant,
        share: &str,
        current_database_id: Option<u64>,
    ) -> Result<Option<ShareRevokeTarget>> {
        self.handler
            .prepare_revoke_database(self.meta.clone(), provider, share, current_database_id)
            .await
    }

    pub async fn grant_table(
        &self,
        provider: &Tenant,
        share: &str,
        grant: ShareGrantTable,
        expected_connection: String,
    ) -> Result<()> {
        self.handler
            .grant_table(
                self.meta.clone(),
                provider,
                share,
                grant,
                expected_connection,
            )
            .await
    }

    pub async fn prepare_revoke_table(
        &self,
        provider: &Tenant,
        share: &str,
        current_object_ids: Option<ProviderObjectIds>,
    ) -> Result<Option<ShareRevokeTarget>> {
        self.handler
            .prepare_revoke_table(self.meta.clone(), provider, share, current_object_ids)
            .await
    }

    pub async fn revoke_share_object(
        &self,
        provider: &Tenant,
        share: &str,
        target: ShareRevokeTarget,
    ) -> Result<()> {
        self.handler
            .revoke_share_object(self.meta.clone(), provider, share, target)
            .await
    }

    pub async fn show_shares(
        &self,
        tenant: &Tenant,
        like_pattern: Option<&str>,
        limit: Option<u64>,
    ) -> Result<Vec<ShareShowEntry>> {
        self.handler
            .show_shares(self.meta.clone(), tenant, like_pattern, limit)
            .await
    }

    pub async fn describe_share(
        &self,
        tenant: &Tenant,
        provider_tenant: Option<&str>,
        share: &str,
    ) -> Result<Vec<ShareDescEntry>> {
        self.handler
            .describe_share(self.meta.clone(), tenant, provider_tenant, share)
            .await
    }

    pub async fn exists(&self, provider: &Tenant, share: &str) -> Result<bool> {
        self.handler
            .exists(self.meta.clone(), provider, share)
            .await
    }

    pub async fn get_connection_name(&self, provider: &Tenant, share: &str) -> Result<String> {
        self.handler
            .get_connection_name(self.meta.clone(), provider, share)
            .await
    }

    pub async fn get_connection_name_if_exists(
        &self,
        provider: &Tenant,
        share: &str,
    ) -> Result<Option<String>> {
        self.handler
            .get_connection_name_if_exists(self.meta.clone(), provider, share)
            .await
    }

    pub async fn get_granted_table_ids(
        &self,
        provider: &Tenant,
        share: &str,
    ) -> Result<BTreeSet<u64>> {
        self.handler
            .get_granted_table_ids(self.meta.clone(), provider, share)
            .await
    }

    pub async fn bind_share_database(
        &self,
        consumer: &Tenant,
        provider_tenant: &str,
        share: &str,
    ) -> Result<ShareDatabaseBinding> {
        self.handler
            .bind_share_database(self.meta.clone(), consumer, provider_tenant, share)
            .await
    }

    pub async fn resolve_shared_table(
        &self,
        consumer: &Tenant,
        binding: &ShareDatabaseBinding,
        table: &str,
    ) -> Result<ShareTableContext> {
        self.handler
            .resolve_shared_table(self.meta.clone(), consumer, binding, table)
            .await
    }

    pub async fn list_shared_tables(
        &self,
        consumer: &Tenant,
        binding: &ShareDatabaseBinding,
    ) -> Result<Vec<ShareTableContext>> {
        self.handler
            .list_shared_tables(self.meta.clone(), consumer, binding)
            .await
    }

    pub async fn resolve_share_storage_params(
        &self,
        provider: &Tenant,
        connection_name: &str,
        provider_storage: StorageParams,
    ) -> Result<StorageParams> {
        self.handler
            .resolve_share_storage_params(provider, connection_name, provider_storage)
            .await
    }

    pub async fn get_shared_table_info(
        &self,
        database: &str,
        context: ShareTableContext,
    ) -> Result<TableInfo> {
        self.handler
            .get_shared_table_info(self.meta.clone(), database, context)
            .await
    }
}

/// Check the license before accessing the EE-only global handler, including in OSS.
pub fn get_data_sharing_handler(
    meta: ShareMetaStore,
    license_key: String,
) -> Result<DataSharingHandlerWrapper> {
    LicenseManagerSwitch::instance().check_enterprise_enabled(license_key, Feature::DataSharing)?;
    Ok(DataSharingHandlerWrapper {
        handler: GlobalInstance::get(),
        meta,
    })
}
