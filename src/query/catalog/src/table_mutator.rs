//  Copyright 2022 Datafuse Labs.
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

use common_exception::Result;
use common_meta_app::schema::TableInfo;

#[async_trait::async_trait]
pub trait TableMutator: Send + Sync {
    async fn blocks_select(&mut self) -> Result<bool>;
    async fn try_commit(&self, catalog_name: &str, table_info: &TableInfo) -> Result<()>;
}
