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

use std::sync::Arc;

use databend_common_base::base::BuildInfoRef;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_settings::Settings;

use crate::databases::Database;
use crate::databases::DatabaseContext;
use crate::share::DataSharingHandlerWrapper;
use crate::share::ShareDatabaseBinding;
use crate::share::ShareTableContext;
use crate::share::get_data_sharing_handler;

#[derive(Clone)]
pub struct SharedDatabase {
    ctx: DatabaseContext,
    db_info: DatabaseInfo,
    version: BuildInfoRef,
}

impl SharedDatabase {
    pub const NAME: &'static str = "SHARE";

    pub fn try_create(
        ctx: DatabaseContext,
        db_info: DatabaseInfo,
        version: BuildInfoRef,
    ) -> Result<Box<dyn Database>> {
        ShareDatabaseBinding::from_engine_options(&db_info.meta.engine_options)?;
        Ok(Box::new(Self {
            ctx,
            db_info,
            version,
        }))
    }

    fn binding(&self) -> Result<ShareDatabaseBinding> {
        ShareDatabaseBinding::from_engine_options(&self.db_info.meta.engine_options)
    }

    async fn share_handler(&self) -> Result<DataSharingHandlerWrapper> {
        // Database APIs have no query context. Load the consumer's effective
        // license using the same config/global/embedded precedence as a session.
        let settings = Settings::create(self.get_tenant().clone());
        settings.load_changes().await?;
        get_data_sharing_handler(
            Arc::new(self.ctx.meta.clone()),
            settings.get_enterprise_license(self.version),
        )
    }

    async fn shared_table(
        &self,
        manager: &DataSharingHandlerWrapper,
        context: ShareTableContext,
    ) -> Result<Arc<dyn Table>> {
        let table_info = manager
            .get_shared_table_info(self.get_db_name(), context)
            .await?;
        self.ctx
            .storage_factory
            .get_table(&table_info, self.ctx.disable_table_info_refresh)
    }

    async fn get_table_with_handler(
        &self,
        manager: &DataSharingHandlerWrapper,
        binding: &ShareDatabaseBinding,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let context = manager
            .resolve_shared_table(self.get_tenant(), binding, table_name)
            .await?;
        self.shared_table(manager, context).await
    }
}

#[async_trait::async_trait]
impl Database for SharedDatabase {
    fn name(&self) -> &str {
        self.db_info.name_ident.database_name()
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.db_info
    }

    async fn get_table(&self, table_name: &str) -> Result<Arc<dyn Table>> {
        let binding = self.binding()?;
        let manager = self.share_handler().await?;
        self.get_table_with_handler(&manager, &binding, table_name)
            .await
    }

    async fn mget_tables(&self, table_names: &[String]) -> Result<Vec<Arc<dyn Table>>> {
        let binding = self.binding()?;
        let manager = self.share_handler().await?;
        let mut tables = Vec::with_capacity(table_names.len());
        for table_name in table_names {
            match self
                .get_table_with_handler(&manager, &binding, table_name)
                .await
            {
                Ok(table) => tables.push(table),
                Err(err) if err.code() == ErrorCode::UnknownTable("").code() => {}
                Err(err) => return Err(err),
            }
        }
        Ok(tables)
    }

    async fn list_tables(&self) -> Result<Vec<Arc<dyn Table>>> {
        let binding = self.binding()?;
        let manager = self.share_handler().await?;
        let contexts = manager
            .list_shared_tables(self.get_tenant(), &binding)
            .await?;
        let mut tables = Vec::with_capacity(contexts.len());
        for context in contexts {
            tables.push(self.shared_table(&manager, context).await?);
        }
        Ok(tables)
    }

    async fn list_tables_names(&self) -> Result<Vec<String>> {
        let binding = self.binding()?;
        let manager = self.share_handler().await?;
        let contexts = manager
            .list_shared_tables(self.get_tenant(), &binding)
            .await?;
        Ok(contexts
            .into_iter()
            .map(|context| context.provider_table)
            .collect())
    }
}
