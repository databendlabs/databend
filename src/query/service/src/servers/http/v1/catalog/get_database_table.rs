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
use jwt_simple::prelude::Serialize;
use poem::error::Result as PoemResult;
use poem::web::Path;

#[derive(Serialize, Eq, PartialEq, Debug, Default)]
pub struct GetDatabaseTableResponse {
    pub table: TableDetails,
    pub warnings: Vec<String>,
}

#[derive(Serialize, Eq, PartialEq, Debug, Default)]
pub struct TableDetails {
    pub name: String,
    pub table_type: String,
    pub database: String,
    pub catalog: String,
    pub owner: String,
    pub engine: String,
    pub cluster_by: String,
    pub create_time: DateTime<Utc>,
    pub num_rows: u64,
    pub data_size: u64,
    pub data_compressed_size: u64,
    pub index_size: u64,
    pub create_query: String,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn get_database_table_handler(
    Path((database, table)): Path<(String, String)>,
) -> PoemResult<GetDatabaseTableResponse> {
    todo!()
}
