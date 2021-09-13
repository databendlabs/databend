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

use common_exception::Result;

use crate::datasources::database::local::CsvTable;
use crate::datasources::database::local::MemoryTable;
use crate::datasources::database::local::NullTable;
use crate::datasources::database::local::ParquetTable;
use crate::datasources::engines::table_engine_registry::TableEngineRegistry;
use crate::datasources::engines::table_factory::RemoteTableFactory;
use crate::datasources::table::fuse::FuseTable;

pub fn register_prelude(registry: &TableEngineRegistry) -> Result<()> {
    registry.register("CSV", std::sync::Arc::new(CsvTable::try_create))?;
    registry.register("PARQUET", std::sync::Arc::new(ParquetTable::try_create))?;
    registry.register("NULL", std::sync::Arc::new(NullTable::try_create))?;
    registry.register("MEMORY", std::sync::Arc::new(MemoryTable::try_create))?;
    registry.register("FUSE", std::sync::Arc::new(FuseTable::try_create))?;
    registry.register("REMOTE", std::sync::Arc::new(RemoteTableFactory {}))?;
    Ok(())
}
