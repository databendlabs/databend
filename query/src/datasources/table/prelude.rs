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

use crate::datasources::database::github;
use crate::datasources::database::github::RepoCommentsTable;
use crate::datasources::database::github::RepoInfoTable;
use crate::datasources::database::github::RepoIssuesTable;
use crate::datasources::database::github::RepoPrsTable;
use crate::configs::Config;
use crate::datasources::table::csv::csv_table::CsvTable;
use crate::datasources::table::fuse::FuseTable;
use crate::datasources::table::memory::memory_table::MemoryTable;
use crate::datasources::table::null::null_table::NullTable;
use crate::datasources::table::parquet::parquet_table::ParquetTable;
use crate::datasources::TableEngineRegistry;

pub fn register_prelude_tbl_engines(registry: &TableEngineRegistry) -> Result<()> {
    // common table engine
    registry.register("CSV", std::sync::Arc::new(CsvTable::try_create))?;
    registry.register("PARQUET", std::sync::Arc::new(ParquetTable::try_create))?;
    registry.register("NULL", std::sync::Arc::new(NullTable::try_create))?;
    registry.register("MEMORY", std::sync::Arc::new(MemoryTable::try_create))?;
pub fn register_prelude_tbl_engines(registry: &TableEngineRegistry, conf: Config) -> Result<()> {
    if conf.query.table_engine_csv_enabled {
        registry.register("CSV", std::sync::Arc::new(CsvTable::try_create))?;
    }
    if conf.query.table_engine_parquet_enabled {
        registry.register("PARQUET", std::sync::Arc::new(ParquetTable::try_create))?;
    }
    if conf.query.table_engine_memory_enabled {
        registry.register("MEMORY", std::sync::Arc::new(MemoryTable::try_create))?;
    }
    registry.register("NULL", std::sync::Arc::new(NullTable::try_create))?;
    registry.register("FUSE", std::sync::Arc::new(FuseTable::try_create))?;

    // github database table engine
    registry.register(
        github::GITHUB_REPO_INFO_ENGINE,
        std::sync::Arc::new(RepoInfoTable::try_create),
    )?;
    registry.register(
        github::GITHUB_REPO_ISSUES_ENGINE,
        std::sync::Arc::new(RepoIssuesTable::try_create),
    )?;
    registry.register(
        github::GITHUB_REPO_PRS_ENGINE,
        std::sync::Arc::new(RepoPrsTable::try_create),
    )?;
    registry.register(
        github::GITHUB_REPO_COMMENTS_ENGINE,
        std::sync::Arc::new(RepoCommentsTable::try_create),
    )?;
    Ok(())
}
