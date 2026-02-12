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

use std::collections::HashMap;
use std::sync::Arc;

pub use databend_common_catalog::catalog::StorageDescription;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::storage::S3StorageClass;
use databend_common_storages_basic::MemoryTable;
use databend_common_storages_basic::NullTable;
use databend_common_storages_basic::RandomTable;
use databend_common_storages_basic::RecursiveCteMemoryTable;
use databend_common_storages_basic::view_table::ViewTable;
use databend_common_storages_delta::DeltaTable;
use databend_common_storages_iceberg::IcebergTable;
use databend_common_storages_stream::stream_table::StreamTable;

use crate::Table;
use crate::fuse::FuseTable;

const RECURSIVE_CTE_OPT_KEY: &str = "recursive_cte";

pub trait StorageCreator: Send + Sync {
    fn try_create(&self, table_info: TableInfo, disable_refresh: bool) -> Result<Box<dyn Table>>;
}

impl<T> StorageCreator for T
where
    T: Fn(TableInfo) -> Result<Box<dyn Table>>,
    T: Send + Sync,
{
    fn try_create(&self, table_info: TableInfo, _need_refresh: bool) -> Result<Box<dyn Table>> {
        self(table_info)
    }
}

#[derive(Default, Clone)]
pub struct FuseTableCreator {
    s3_storage_class_spec: Option<S3StorageClass>,
}

impl FuseTableCreator {
    fn with_storage_class_spec(storage_class_spec: S3StorageClass) -> FuseTableCreator {
        Self {
            s3_storage_class_spec: Some(storage_class_spec),
        }
    }
}

impl StorageCreator for FuseTableCreator {
    fn try_create(&self, table_info: TableInfo, disable_refresh: bool) -> Result<Box<dyn Table>> {
        let tbl = FuseTable::try_create(table_info, self.s3_storage_class_spec, disable_refresh)?;
        Ok(tbl)
    }
}

#[derive(Clone)]
struct MemoryStorageCreator;

impl StorageCreator for MemoryStorageCreator {
    fn try_create(&self, table_info: TableInfo, _disable_refresh: bool) -> Result<Box<dyn Table>> {
        if table_info.options().contains_key(RECURSIVE_CTE_OPT_KEY) {
            RecursiveCteMemoryTable::try_create(table_info)
        } else {
            MemoryTable::try_create(table_info)
        }
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

#[derive(Clone)]
pub struct Storage {
    creator: Arc<dyn StorageCreator>,
    descriptor: Arc<dyn StorageDescriptor>,
}

#[derive(Default, Clone)]
pub struct StorageFactory {
    storages: HashMap<String, Storage>,
}

impl StorageFactory {
    pub fn with_storage_class_specs(&self, storage_class: S3StorageClass) -> Self {
        let mut new_creators = self.storages.clone();
        new_creators.insert("FUSE".to_string(), Storage {
            creator: Arc::new(FuseTableCreator::with_storage_class_spec(storage_class)),
            descriptor: Arc::new(FuseTable::description),
        });

        Self {
            storages: new_creators,
        }
    }
    pub fn create(conf: InnerConfig) -> Self {
        let mut creators: HashMap<String, Storage> = Default::default();

        // Register memory table engine.
        if conf.query.common.table_engine_memory_enabled {
            creators.insert("MEMORY".to_string(), Storage {
                creator: Arc::new(MemoryStorageCreator),
                descriptor: Arc::new(MemoryTable::description),
            });
        }

        // Register NULL table engine.
        creators.insert("NULL".to_string(), Storage {
            creator: Arc::new(NullTable::try_create),
            descriptor: Arc::new(NullTable::description),
        });

        // Register FUSE table engine.
        creators.insert("FUSE".to_string(), Storage {
            // TODO should get default storage specs
            creator: Arc::new(FuseTableCreator::default()),
            descriptor: Arc::new(FuseTable::description),
        });

        // Register VIEW table engine
        creators.insert("VIEW".to_string(), Storage {
            creator: Arc::new(ViewTable::try_create),
            descriptor: Arc::new(ViewTable::description),
        });

        // Register RANDOM table engine
        creators.insert("RANDOM".to_string(), Storage {
            creator: Arc::new(RandomTable::try_create),
            descriptor: Arc::new(RandomTable::description),
        });

        // Register STREAM table engine
        creators.insert("STREAM".to_string(), Storage {
            creator: Arc::new(StreamTable::try_create),
            descriptor: Arc::new(StreamTable::description),
        });

        // Register ICEBERG table engine
        creators.insert("ICEBERG".to_string(), Storage {
            creator: Arc::new(IcebergTable::try_create),
            descriptor: Arc::new(IcebergTable::description),
        });

        // Register DELTA table engine
        creators.insert("DELTA".to_string(), Storage {
            creator: Arc::new(DeltaTable::try_create),
            descriptor: Arc::new(DeltaTable::description),
        });

        StorageFactory { storages: creators }
    }

    pub fn get_table(
        &self,
        table_info: &TableInfo,
        disable_refresh: bool,
    ) -> Result<Arc<dyn Table>> {
        let factory = self.get_storage_factory(table_info)?;
        let table: Arc<dyn Table> = factory
            .creator
            .try_create(table_info.clone(), disable_refresh)?
            .into();
        Ok(table)
    }

    fn get_storage_factory(&self, table_info: &TableInfo) -> Result<&Storage> {
        let engine = table_info.engine().to_uppercase();
        self.storages.get(&engine).ok_or_else(|| {
            ErrorCode::UnknownTableEngine(format!("Unknown table engine {}", engine))
        })
    }

    pub fn get_storage_descriptors(&self) -> Vec<StorageDescription> {
        let mut descriptors = Vec::with_capacity(self.storages.len());
        let it = self.storages.values();
        for entry in it {
            descriptors.push(entry.descriptor.description())
        }
        descriptors
    }
}
