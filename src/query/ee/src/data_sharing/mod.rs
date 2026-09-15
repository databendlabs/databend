// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
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
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use databend_enterprise_data_sharing::*;

mod manager;
mod presentation;
mod shared_table;
mod storage;

use manager::ShareMgr;

pub struct RealDataSharingHandler;

impl RealDataSharingHandler {
    pub fn init() -> Result<()> {
        GlobalInstance::set(Arc::new(Self) as Arc<dyn DataSharingHandler>);
        Ok(())
    }
}

#[async_trait::async_trait]
impl DataSharingHandler for RealDataSharingHandler {
    fn ensure_provider_table_can_be_shared(&self, meta: &TableMeta) -> Result<()> {
        manager::ensure_provider_table_can_be_shared(meta)
    }

    async fn create_share(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        create_option: CreateOption,
        share: &str,
        connection: Option<String>,
        comment: Option<String>,
    ) -> Result<()> {
        ShareMgr::create(meta)
            .create_share(provider, create_option, share, connection, comment)
            .await
    }

    async fn drop_share(&self, meta: ShareMetaStore, provider: &Tenant, share: &str) -> Result<()> {
        ShareMgr::create(meta).drop_share(provider, share).await
    }

    async fn add_accounts(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        accounts: Vec<String>,
        expected_connection: String,
        if_exists: bool,
    ) -> Result<()> {
        ShareMgr::create(meta)
            .add_accounts(provider, share, accounts, expected_connection, if_exists)
            .await
    }

    async fn remove_accounts(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        accounts: Vec<String>,
        expected_connection: Option<String>,
        if_exists: bool,
    ) -> Result<()> {
        ShareMgr::create(meta)
            .remove_accounts(provider, share, accounts, expected_connection, if_exists)
            .await
    }

    async fn set_share(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        request: SetShareRequest,
    ) -> Result<()> {
        ShareMgr::create(meta)
            .set_share(provider, share, request)
            .await
    }

    async fn grant_database(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        grant: ShareGrantDatabase,
    ) -> Result<()> {
        ShareMgr::create(meta)
            .grant_database(provider, share, grant)
            .await
    }

    async fn prepare_revoke_database(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        current_database_id: Option<u64>,
    ) -> Result<Option<ShareRevokeTarget>> {
        ShareMgr::create(meta)
            .prepare_revoke_database(provider, share, current_database_id)
            .await
    }

    async fn grant_table(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        grant: ShareGrantTable,
        expected_connection: String,
    ) -> Result<()> {
        ShareMgr::create(meta)
            .grant_table(provider, share, grant, expected_connection)
            .await
    }

    async fn prepare_revoke_table(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        current_object_ids: Option<ProviderObjectIds>,
    ) -> Result<Option<ShareRevokeTarget>> {
        ShareMgr::create(meta)
            .prepare_revoke_table(provider, share, current_object_ids)
            .await
    }

    async fn revoke_share_object(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
        target: ShareRevokeTarget,
    ) -> Result<()> {
        ShareMgr::create(meta)
            .revoke_share_object(provider, share, target)
            .await
    }

    async fn show_shares(
        &self,
        meta: ShareMetaStore,
        tenant: &Tenant,
        like_pattern: Option<&str>,
        limit: Option<u64>,
    ) -> Result<Vec<ShareShowEntry>> {
        ShareMgr::create(meta)
            .show_shares(tenant, like_pattern, limit)
            .await
    }

    async fn describe_share(
        &self,
        meta: ShareMetaStore,
        tenant: &Tenant,
        provider_tenant: Option<&str>,
        share: &str,
    ) -> Result<Vec<ShareDescEntry>> {
        ShareMgr::create(meta)
            .describe_share(tenant, provider_tenant, share)
            .await
    }

    async fn exists(&self, meta: ShareMetaStore, provider: &Tenant, share: &str) -> Result<bool> {
        ShareMgr::create(meta).exists(provider, share).await
    }

    async fn get_connection_name(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
    ) -> Result<String> {
        ShareMgr::create(meta)
            .get_connection_name(provider, share)
            .await
    }

    async fn get_connection_name_if_exists(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
    ) -> Result<Option<String>> {
        ShareMgr::create(meta)
            .get_connection_name_if_exists(provider, share)
            .await
    }

    async fn get_granted_table_ids(
        &self,
        meta: ShareMetaStore,
        provider: &Tenant,
        share: &str,
    ) -> Result<BTreeSet<u64>> {
        ShareMgr::create(meta)
            .get_granted_table_ids(provider, share)
            .await
    }

    async fn bind_share_database(
        &self,
        meta: ShareMetaStore,
        consumer: &Tenant,
        provider_tenant: &str,
        share: &str,
    ) -> Result<ShareDatabaseBinding> {
        ShareMgr::create(meta)
            .bind_share_database(consumer, provider_tenant, share)
            .await
    }

    async fn resolve_shared_table(
        &self,
        meta: ShareMetaStore,
        consumer: &Tenant,
        binding: &ShareDatabaseBinding,
        table: &str,
    ) -> Result<ShareTableContext> {
        ShareMgr::create(meta)
            .resolve_shared_table(consumer, binding, table)
            .await
    }

    async fn list_shared_tables(
        &self,
        meta: ShareMetaStore,
        consumer: &Tenant,
        binding: &ShareDatabaseBinding,
    ) -> Result<Vec<ShareTableContext>> {
        ShareMgr::create(meta)
            .list_shared_tables(consumer, binding)
            .await
    }

    async fn resolve_share_storage_params(
        &self,
        provider: &Tenant,
        connection_name: &str,
        provider_storage: StorageParams,
    ) -> Result<StorageParams> {
        storage::resolve_share_storage_params(provider, connection_name, provider_storage).await
    }

    async fn get_shared_table_info(
        &self,
        meta: ShareMetaStore,
        database: &str,
        context: ShareTableContext,
    ) -> Result<TableInfo> {
        shared_table::get_shared_table_info(meta, database, context).await
    }
}
