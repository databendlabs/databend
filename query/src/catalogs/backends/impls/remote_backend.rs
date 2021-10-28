// Copyright 2020 Datafuse Labs.
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
//

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use common_exception::Result;
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

use crate::catalogs::backends::impls::MetaCached;
use crate::catalogs::backends::impls::MetaRemote;
use crate::catalogs::backends::impls::MetaSync;
use crate::catalogs::backends::MetaApiSync;
use crate::common::MetaClientProvider;

/// Meta store that provides **sync** API, equipped with cache, backed by a cluster of remote Meta server.
///
/// The component hierarchy is layered as:
/// ```text
///                                        RPC
/// MetaSync -> MetaCached -> MetaRemote -------> Meta server      Meta server
///                                               raft <---------> raft <----..
///                                               MetaEmbedded     MetaEmbedded
/// ```
#[derive(Clone)]
pub struct MetaRemoteSync {
    pub meta_sync: MetaSync,
}

impl MetaRemoteSync {
    pub fn create(apis_provider: Arc<MetaClientProvider>) -> MetaRemoteSync {
        // TODO(xp): config rpc timeout
        // TODO(xp): config blocking timeout

        let meta_remote = MetaRemote::create(apis_provider);
        let meta_cached = MetaCached::create(Arc::new(meta_remote));
        let meta_sync = MetaSync::create(Arc::new(meta_cached), Some(Duration::from_millis(5000)));

        MetaRemoteSync { meta_sync }
    }
}

impl Deref for MetaRemoteSync {
    type Target = MetaSync;

    fn deref(&self) -> &Self::Target {
        &self.meta_sync
    }
}

/// So that every type that deref to a `T: MetaApiSync` is impl with `MetaApiSync`
impl<T: MetaApiSync, U: Deref<Target = T> + Send + Sync> MetaApiSync for U {
    fn create_database(&self, plan: CreateDatabasePlan) -> Result<CreateDatabaseReply> {
        self.deref().create_database(plan)
    }

    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        self.deref().drop_database(plan)
    }

    fn get_database(&self, db_name: &str) -> Result<Arc<DatabaseInfo>> {
        self.deref().get_database(db_name)
    }

    fn get_databases(&self) -> Result<Vec<Arc<DatabaseInfo>>> {
        self.deref().get_databases()
    }

    fn create_table(&self, plan: CreateTablePlan) -> Result<CreateTableReply> {
        self.deref().create_table(plan)
    }

    fn drop_table(&self, plan: DropTablePlan) -> Result<()> {
        self.deref().drop_table(plan)
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<TableInfo>> {
        self.deref().get_table(db_name, table_name)
    }

    fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<TableInfo>>> {
        self.deref().get_tables(db_name)
    }

    fn get_table_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)> {
        self.deref().get_table_by_id(table_id)
    }

    fn upsert_table_option(
        &self,
        table_id: MetaId,
        table_version: MetaVersion,
        table_option_key: String,
        table_option_value: String,
    ) -> Result<UpsertTableOptionReply> {
        self.deref().upsert_table_option(
            table_id,
            table_version,
            table_option_key,
            table_option_value,
        )
    }

    fn name(&self) -> String {
        self.deref().name()
    }
}
