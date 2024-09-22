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

use dashmap::DashMap;
pub use databend_common_catalog::catalog::StorageDescription;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_storages_delta::DeltaTable;
use databend_common_storages_iceberg::IcebergTable;
use databend_common_storages_memory::MemoryTable;
use databend_common_storages_null::NullTable;
use databend_common_storages_random::RandomTable;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_storages_view::view_table::ViewTable;

use crate::fuse::FuseTable;
use crate::Table;

// default schema refreshing timeout is 5 seconds.
const DEFAULT_SCHEMA_REFRESHING_TIMEOUT_MS: Duration = Duration::from_secs(5);

pub trait StorageCreator: Send + Sync {
    fn try_create(&self, table_info: TableInfo) -> Result<Box<dyn Table>>;
}

impl<T> StorageCreator for T
where
    T: Fn(TableInfo) -> Result<Box<dyn Table>>,
    T: Send + Sync,
{
    fn try_create(&self, table_info: TableInfo) -> Result<Box<dyn Table>> {
        self(table_info)
    }
}

use std::future::Future;
use std::time::Duration;

use dashmap::mapref::one::Ref;

#[async_trait::async_trait]
impl<F, Fut> TableInfoRefresher for F
where
    F: Fn(Arc<TableInfo>) -> Fut,
    Fut: Future<Output = Result<Arc<TableInfo>>> + Send,
    F: Send + Sync,
{
    async fn refresh(&self, table_info: Arc<TableInfo>) -> Result<Arc<TableInfo>> {
        self(table_info).await
    }
}

pub trait StorageDescriptor: Send + Sync {
    fn description(&self) -> StorageDescription;
}

impl<T> StorageDescriptor for T
where
    T: Fn() -> StorageDescription,
    T: Send + Sync,
{
    fn description(&self) -> StorageDescription {
        self()
    }
}

// Table engines that need to refresh schema should provide implementation of this
// trait. For example, FUSE table engine that attaches to a remote storage may
// need to refresh schema to get the latest schema.
#[async_trait::async_trait]
pub trait TableInfoRefresher: Send + Sync {
    async fn refresh(&self, table_info: Arc<TableInfo>) -> Result<Arc<TableInfo>>;
}

pub struct Storage {
    creator: Arc<dyn StorageCreator>,
    descriptor: Arc<dyn StorageDescriptor>,
    table_info_refresher: Option<Arc<dyn TableInfoRefresher>>,
}

#[derive(Default)]
pub struct StorageFactory {
    storages: DashMap<String, Storage>,
    schema_refreshing_timeout: Duration,
}

impl StorageFactory {
    pub fn create(conf: InnerConfig) -> Self {
        let creators: DashMap<String, Storage> = Default::default();

        // Register memory table engine.
        if conf.query.table_engine_memory_enabled {
            creators.insert("MEMORY".to_string(), Storage {
                creator: Arc::new(MemoryTable::try_create),
                descriptor: Arc::new(MemoryTable::description),
                table_info_refresher: None,
            });
        }

        // Register NULL table engine.
        creators.insert("NULL".to_string(), Storage {
            creator: Arc::new(NullTable::try_create),
            descriptor: Arc::new(NullTable::description),
            table_info_refresher: None,
        });

        // Register FUSE table engine.
        creators.insert("FUSE".to_string(), Storage {
            creator: Arc::new(FuseTable::try_create),
            descriptor: Arc::new(FuseTable::description),
            table_info_refresher: Some(Arc::new(FuseTable::refresh_schema)),
        });

        // Register VIEW table engine
        creators.insert("VIEW".to_string(), Storage {
            creator: Arc::new(ViewTable::try_create),
            descriptor: Arc::new(ViewTable::description),
            table_info_refresher: None,
        });

        // Register RANDOM table engine
        creators.insert("RANDOM".to_string(), Storage {
            creator: Arc::new(RandomTable::try_create),
            descriptor: Arc::new(RandomTable::description),
            table_info_refresher: None,
        });

        // Register STREAM table engine
        creators.insert("STREAM".to_string(), Storage {
            creator: Arc::new(StreamTable::try_create),
            descriptor: Arc::new(StreamTable::description),
            table_info_refresher: None,
        });

        // Register ICEBERG table engine
        creators.insert("ICEBERG".to_string(), Storage {
            creator: Arc::new(IcebergTable::try_create),
            descriptor: Arc::new(IcebergTable::description),
            table_info_refresher: None,
        });

        // Register DELTA table engine
        creators.insert("DELTA".to_string(), Storage {
            creator: Arc::new(DeltaTable::try_create),
            descriptor: Arc::new(DeltaTable::description),
            table_info_refresher: None,
        });

        StorageFactory {
            storages: creators,
            schema_refreshing_timeout: DEFAULT_SCHEMA_REFRESHING_TIMEOUT_MS,
        }
    }

    pub fn get_table(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let factory = self.get_storage_factory(table_info)?;
        let table: Arc<dyn Table> = factory.creator.try_create(table_info.clone())?.into();
        Ok(table)
    }

    pub async fn refresh_table_info(&self, table_info: Arc<TableInfo>) -> Result<Arc<TableInfo>> {
        let factory = self.get_storage_factory(table_info.as_ref())?;
        match factory.table_info_refresher.as_ref() {
            None => Ok(table_info),
            Some(refresher) => {
                let table_description = table_info.desc.clone();
                tokio::time::timeout(
                    self.schema_refreshing_timeout,
                    refresher.refresh(table_info),
                )
                .await
                .map_err(|elapsed| {
                    ErrorCode::RefreshTableInfoFailure(format!(
                        "failed to refresh table meta {} in time. Elapsed: {}",
                        table_description, elapsed
                    ))
                })
                .map_err(|e| {
                    ErrorCode::RefreshTableInfoFailure(format!(
                        "failed to refresh table meta {} : {}",
                        table_description, e
                    ))
                })?
            }
        }
    }

    fn get_storage_factory(&self, table_info: &TableInfo) -> Result<Ref<String, Storage>> {
        let engine = table_info.engine().to_uppercase();
        self.storages.get(&engine).ok_or_else(|| {
            ErrorCode::UnknownTableEngine(format!("Unknown table engine {}", engine))
        })
    }

    pub fn get_storage_descriptors(&self) -> Vec<StorageDescription> {
        let mut descriptors = Vec::with_capacity(self.storages.len());
        let it = self.storages.iter();
        for entry in it {
            descriptors.push(entry.value().descriptor.description())
        }
        descriptors
    }
}
