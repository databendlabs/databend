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

use chrono::DateTime;
use chrono::Utc;
use poem::error::Result as PoemResult;
use poem::web::Path;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct SearchTablesResponse {
    pub tables: Vec<TableInfo>,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Default)]
pub struct TableInfo {
    pub catalog: String,
    pub database: String,
    pub database_id: u64,
    pub name: String,
    pub table_id: u64,
    pub total_columns: u64,
    pub engine: String,
    pub engine_full: String,
    pub cluster_by: String,
    pub is_transient: bool,
    pub is_attach: bool,
    pub created_on: DateTime<Utc>,
    pub dropped_on: Option<DateTime<Utc>>,
    pub updated_on: DateTime<Utc>,
    pub num_rows: u64,
    pub data_size: u64,
    pub data_compressed_size: u64,
    pub index_size: u64,
    pub number_of_segments: u64,
    pub number_of_blocks: u64,
    pub owner: String,
    pub comment: String,
    pub table_type: String,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn search_tables_handler() -> PoemResult<SearchTablesResponse> {
    todo!()
}
