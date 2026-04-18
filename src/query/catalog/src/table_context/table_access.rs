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

use databend_common_exception::Result;
use databend_common_meta_app::tenant::Tenant;
use databend_common_pipeline::core::LockGuard;
use databend_common_storage::DataOperator;

use crate::catalog::Catalog;
use crate::lock::LockTableOption;
use crate::plan::DataSourcePlan;
use crate::table::Table;

#[async_trait::async_trait]
pub trait TableContextTableAccess: Send + Sync {
    /// Build a table instance the plan wants to operate on.
    ///
    /// A plan just contains raw information about a table or table function.
    /// This method builds a `dyn Table`, which provides table specific io methods the plan needs.
    fn build_table_from_source_plan(&self, plan: &DataSourcePlan) -> Result<Arc<dyn Table>>;

    async fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>>;

    fn get_default_catalog(&self) -> Result<Arc<dyn Catalog>>;

    fn get_current_catalog(&self) -> String;

    fn get_current_database(&self) -> String;

    fn get_tenant(&self) -> Tenant;

    async fn get_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        self.get_table_with_branch(catalog, database, table, None)
            .await
    }

    async fn get_zero_table(&self) -> Result<Arc<dyn Table>> {
        let catalog = self.get_catalog("default").await?;
        catalog
            .get_table(&self.get_tenant(), "system", "zero")
            .await
    }

    /// Get the storage data accessor operator from the session manager.
    /// Note that this is the application level data accessor, which may be different from
    /// the table level data accessor (e.g., table with customized storage parameters).
    fn get_application_level_data_operator(&self) -> Result<DataOperator>;

    async fn get_table_with_branch(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        branch: Option<&str>,
    ) -> Result<Arc<dyn Table>>;

    async fn resolve_data_source(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        branch: Option<&str>,
        max_batch_size: Option<u64>,
    ) -> Result<Arc<dyn Table>>;

    async fn acquire_table_lock(
        self: Arc<Self>,
        catalog_name: &str,
        db_name: &str,
        tbl_name: &str,
        lock_opt: &LockTableOption,
    ) -> Result<Option<Arc<LockGuard>>>;

    fn get_temp_table_prefix(&self) -> Result<String>;

    fn is_temp_table(&self, catalog_name: &str, database_name: &str, table_name: &str) -> bool;
}
