// Copyright 2021 Datafuse Labs.
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
use std::time::Duration;

use common_base::BlockingWait;
use common_base::Runtime;
use common_exception::Result;
use common_meta_api::MetaApi;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateTableReply;
use common_meta_types::DatabaseInfo;
use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

use crate::catalogs::backends::MetaApiSync;

/// A `MetaApiSync` impl, backed with another `MetaApi`.
#[derive(Clone)]
pub struct MetaSync {
    rt: Arc<Runtime>,
    timeout: Option<Duration>,
    pub inner: Arc<dyn MetaApi>,
}

impl MetaSync {
    pub fn create(inner: Arc<dyn MetaApi>, timeout: Option<Duration>) -> MetaSync {
        let rt = Runtime::with_worker_threads(1).expect("remote catalogs initialization failure");
        MetaSync {
            rt: Arc::new(rt),
            timeout,
            inner,
        }
    }
}

impl MetaApiSync for MetaSync {
    fn create_database(&self, plan: CreateDatabasePlan) -> Result<CreateDatabaseReply> {
        let x = self.inner.clone();
        (async move { x.create_database(plan).await }).wait_in(&self.rt, self.timeout)?
    }

    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        let x = self.inner.clone();
        (async move { x.drop_database(plan).await }).wait_in(&self.rt, self.timeout)?
    }

    fn get_database(&self, db_name: &str) -> Result<Arc<DatabaseInfo>> {
        let x = self.inner.clone();
        let db_name = db_name.to_owned();
        (async move { x.get_database(&db_name).await }).wait_in(&self.rt, self.timeout)?
    }

    fn get_databases(&self) -> Result<Vec<Arc<DatabaseInfo>>> {
        let x = self.inner.clone();
        (async move { x.get_databases().await }).wait_in(&self.rt, self.timeout)?
    }

    fn create_table(&self, plan: CreateTablePlan) -> Result<CreateTableReply> {
        // TODO validate plan by table engine first
        let x = self.inner.clone();
        (async move { x.create_table(plan).await }).wait_in(&self.rt, self.timeout)?
    }

    fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        let x = self.inner.clone();
        (async move { x.drop_table(plan).await }).wait_in(&self.rt, self.timeout)?
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableInfo>> {
        let x = self.inner.clone();
        let table_name = table_name.to_string();
        let db_name = db_name.to_string();
        (async move { x.get_table(&db_name, &table_name).await }).wait_in(&self.rt, self.timeout)?
    }

    fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<TableInfo>>> {
        let x = self.inner.clone();
        let db_name = db_name.to_owned();
        (async move { x.get_tables(&db_name).await }).wait_in(&self.rt, self.timeout)?
    }

    fn get_table_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)> {
        let x = self.inner.clone();
        (async move { x.get_table_by_id(table_id).await }).wait_in(&self.rt, self.timeout)?
    }

    fn upsert_table_option(
        &self,
        table_id: MetaId,
        table_version: MetaVersion,
        table_option_key: String,
        table_option_value: String,
    ) -> Result<UpsertTableOptionReply> {
        let x = self.inner.clone();
        (async move {
            x.upsert_table_option(
                table_id,
                table_version,
                table_option_key,
                table_option_value,
            )
            .await
        })
        .wait_in(&self.rt, self.timeout)?
    }

    fn name(&self) -> String {
        format!("meta-sync({})", self.inner.name())
    }
}
