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

pub use catalog::Catalog;
use common_datavalues::DataSchemaRef;
use common_planners::TableOptions;
pub use database::Database;
pub use impls::util::in_memory_metas::InMemoryMetas;
pub use meta_id_ranges::*;
//pub use meta_backend::MetaBackend;
pub use table::Table;
pub use table::TablePtr;
pub use table_function::TableFunction;
//pub use database_engine::DatabaseEngine;
pub use table_meta::Meta;
pub use table_meta::TableFunctionMeta;
pub use table_meta::TableMeta;

pub use crate::datasources::database_engine::DatabaseEngine;

mod catalog;
mod database;
mod meta_id_ranges;
mod table;
mod table_function;
mod table_meta;

pub mod impls;
pub mod meta_backend;

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub db: String,
    pub table_id: u64,
    pub name: String,
    pub schema: DataSchemaRef,
    pub engine: String,
    pub table_option: TableOptions,
}
