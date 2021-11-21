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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::CreateTableReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::MetaId;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use dyn_clone::DynClone;

use crate::catalogs::Table;

#[async_trait::async_trait]
pub trait Database: DynClone + Sync + Send {
    /// Database name.
    fn name(&self) -> &str;

    // Get one table by db and table name.
    async fn get_table(&self, db_name: &str, table_name: &str) -> Result<Arc<dyn Table>>;

    async fn list_tables(&self, db_name: &str) -> Result<Vec<Arc<dyn Table>>>;

    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)>;

    async fn create_table(&self, req: CreateTableReq) -> Result<()>;

    async fn drop_table(&self, req: DropTableReq) -> Result<DropTableReply>;

    // Check a db.table is exists or not.
    async fn exists_table(&self, db_name: &str, table_name: &str) -> Result<bool> {
        match self.get_table(db_name, table_name).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.code() == ErrorCode::UnknownTableCode() {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Build a `Arc<dyn Table>` from `TableInfo`.
    fn build_table(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>>;
}
