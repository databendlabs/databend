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

mod columns_table;
mod key_column_usage_table;
mod keywords_table;
mod schemata_table;
mod statistics_table;
mod tables_table;
mod views_table;

pub use columns_table::ColumnsTable;
pub use key_column_usage_table::KeyColumnUsageTable;
pub use keywords_table::KeywordsTable;
pub use schemata_table::SchemataTable;
pub use statistics_table::StatisticsTable;
pub use tables_table::TablesTable;
pub use views_table::ViewsTable;
