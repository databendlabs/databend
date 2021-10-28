//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::HashMap;
use std::sync::Arc;

use common_context::TableDataContext;
use common_dal::InMemoryData;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::DatabaseInfo;
use common_meta_types::MetaId;
use common_meta_types::MetaVersion;
use common_meta_types::TableInfo;
use common_meta_types::UpsertTableOptionReply;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;

use crate::catalogs::backends::MetaApiSync;
use crate::catalogs::backends::MetaEmbeddedSync;
use crate::catalogs::backends::MetaRemoteSync;
use crate::catalogs::catalog::Catalog;
use crate::catalogs::Database;
use crate::catalogs::Table;
use crate::common::MetaClientProvider;
use crate::configs::Config;
use crate::datasources::database::default::default_database::DefaultDatabase;
use crate::datasources::table::register_prelude_tbl_engines;
use crate::datasources::table_engine_registry::TableEngineRegistry;

/// Catalog based on MetaStore
/// - System Database NOT included
/// - Meta data of databases are saved in meta store
/// - Instances of `Database` are created by using database factories according to the engine
/// - Database engines are free to save table meta in metastore or not
pub struct MetaStoreCatalog {
    table_engine_registry: Arc<TableEngineRegistry>,

    meta: Arc<dyn MetaApiSync>,

    /// The data layer that supports MemoryTable or else.
    ///
    /// TODO(xp): Introduce this field to release `Database` from managing MemoryTable data blocks.
    ///           This should still be considered as a temp solution.
    ///           There should be a dedicate component to serve this duty.
    ///           Maybe as part of `Session`.
    in_memory_data: Arc<RwLock<InMemoryData<u64>>>,

    // this is not for performance:
    // some tables are stateful, cached in database, thus, instances of Database have to be kept as well.
    //
    // if we drop Database Trait, and create tables by using catalog directly, things may be easier
    db_instances: RwLock<HashMap<String, Arc<dyn Database>>>,
}

impl MetaStoreCatalog {
    pub fn try_create_with_config(conf: Config) -> Result<Self> {
        let local_mode = conf.meta.meta_address.is_empty();
        let meta: Arc<dyn MetaApiSync> = if local_mode {
            Arc::new(MetaEmbeddedSync::create())
        } else {
            let store_client_provider = Arc::new(MetaClientProvider::new(&conf));
            Arc::new(MetaRemoteSync::create(store_client_provider))
        };

        let plan = CreateDatabasePlan {
            if_not_exists: true,
            db: "default".to_string(),
            options: Default::default(),
        };
        meta.create_database(plan)?;

        let table_engine_registry = Arc::new(TableEngineRegistry::new());

        register_prelude_tbl_engines(&table_engine_registry)?;

        let cat = MetaStoreCatalog {
            table_engine_registry,
            meta,
            in_memory_data: Default::default(),
            db_instances: RwLock::new(HashMap::new()),
        };

        Ok(cat)
    }

    fn build_db_instance(&self, db_info: &Arc<DatabaseInfo>) -> Result<Arc<dyn Database>> {
        let db = DefaultDatabase::new(&db_info.db);

        let db = Arc::new(db);

        let name = db_info.db.clone();
        self.db_instances.write().insert(name, db.clone());
        Ok(db)
    }
}

impl Catalog for MetaStoreCatalog {
    fn get_databases(&self) -> Result<Vec<Arc<dyn Database>>> {
        let dbs = self.meta.get_databases()?;
        dbs.iter().try_fold(vec![], |mut acc, item| {
            let db = self.build_db_instance(item)?;
            acc.push(db);
            Ok(acc)
        })
    }

    fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        {
            if let Some(db) = self.db_instances.read().get(db_name) {
                return Ok(db.clone());
            }
        }
        let db_info = self.meta.get_database(db_name)?;
        self.build_db_instance(&db_info)
    }

    fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>> {
        let table_info = self.meta.get_table(db_name, table_name)?;
        self.build_table(table_info.as_ref())
    }

    fn get_tables(&self, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        let table_infos = self.meta.get_tables(db_name)?;

        table_infos.iter().try_fold(vec![], |mut acc, item| {
            let tbl = self.build_table(item.as_ref())?;
            acc.push(tbl);
            Ok(acc)
        })
    }

    fn get_table_by_id(&self, table_id: MetaId) -> Result<Arc<dyn Table>> {
        let table_info = self.meta.get_table_by_id(table_id)?;
        self.build_table(table_info.as_ref())
    }

    fn upsert_table_option(
        &self,
        table_id: MetaId,
        table_version: MetaVersion,
        table_option_key: String,
        table_option_value: String,
    ) -> Result<UpsertTableOptionReply> {
        self.meta.upsert_table_option(
            table_id,
            table_version,
            table_option_key,
            table_option_value,
        )
    }

    fn create_table(&self, plan: CreateTablePlan) -> common_exception::Result<()> {
        // TODO validate table parameters by using TableFactory
        self.meta.create_table(plan)?;
        Ok(())
    }

    fn drop_table(&self, plan: DropTablePlan) -> common_exception::Result<()> {
        self.meta.drop_table(plan)
    }

    fn create_database(&self, plan: CreateDatabasePlan) -> Result<CreateDatabaseReply> {
        self.meta.create_database(plan)
    }

    fn drop_database(&self, plan: DropDatabasePlan) -> Result<()> {
        let name = plan.db.clone();
        self.meta.drop_database(plan)?;
        self.db_instances.write().remove(&name);
        Ok(())
    }

    fn build_table(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let engine = table_info.engine();
        let factory = self
            .table_engine_registry
            .get_table_factory(engine)
            .ok_or_else(|| {
                ErrorCode::UnknownTableEngine(format!("unknown table engine {}", engine))
            })?;

        let tbl: Arc<dyn Table> = factory
            .try_create(
                table_info.clone(),
                Arc::new(TableDataContext {
                    in_memory_data: self.in_memory_data.clone(),
                }),
            )?
            .into();

        Ok(tbl)
    }
}
