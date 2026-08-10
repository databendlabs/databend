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

use databend_common_exception::Result;

use crate::table::Table;

#[async_trait::async_trait]
pub trait TableContextCte: Send + Sync {
    fn add_m_cte_temp_table(&self, database_name: &str, table_name: &str);

    async fn drop_m_cte_temp_table(&self) -> Result<()>;

    fn add_recursive_cte_temp_table(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
    );

    async fn drop_recursive_cte_temp_table(&self) -> Result<()>;
}

pub trait TableContextStream: Send + Sync {
    fn add_streams_ref(&self, _catalog: &str, _database: &str, _stream: &str, _consume: bool) {
        unimplemented!()
    }

    fn get_consume_streams(&self, _query: bool) -> Result<Vec<Arc<dyn Table>>> {
        unimplemented!()
    }
}
