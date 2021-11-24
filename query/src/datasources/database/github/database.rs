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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::CreateTableReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::GetTableReq;
use common_meta_types::ListTableReq;
use common_meta_types::TableInfo;

use crate::catalogs::Database;
use crate::catalogs::Table;
use crate::datasources::context::DataSourceContext;

#[derive(Clone)]
pub struct GithubDatabase {
    db_name: String,
    ctx: DataSourceContext,
}

impl GithubDatabase {
    pub fn try_create(db_name: &str, ctx: DataSourceContext) -> Result<Box<dyn Database>> {
        Ok(Box::new(Self {
            db_name: db_name.to_string(),
            ctx,
        }))
    }

    fn build_table(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl Database for GithubDatabase {
    fn name(&self) -> &str {
        &self.db_name
    }

    async fn get_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> common_exception::Result<Arc<dyn Table>> {
        unimplemented!()
    }

    async fn list_tables(&self, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        unimplemented!()
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<()> {
        unimplemented!()
    }

    async fn drop_table(&self, req: DropTableReq) -> Result<DropTableReply> {
        unimplemented!()
    }
}
