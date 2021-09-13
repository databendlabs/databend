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

//pub use local_database::LocalDatabase;
//pub use local_databases::LocalDatabases;
//pub use local_meta_backend::LocalMetaBackend;

pub use crate::datasources::table::csv_table::CsvTable;
pub use crate::datasources::table::csv_table_stream::CsvTableStream;
pub use crate::datasources::table::memory_table::MemoryTable;
pub use crate::datasources::table::memory_table_stream::MemoryTableStream;
pub use crate::datasources::table::null_table::NullTable;
pub use crate::datasources::table::parquet_table::ParquetTable;

//#[cfg(test)]
//mod local_meta_backend_test;
//
//mod local_database;
//mod local_databases;
//mod local_meta_backend;
