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

use std::any::Any;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::ReadDataSourcePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::catalogs::Table;
use crate::sessions::DatabendQueryContextRef;

pub struct DatabasesTable {
    table_info: TableInfo,
}

impl DatabasesTable {
    pub fn create(table_id: u64) -> Self {
        let schema =
            DataSchemaRefExt::create(vec![DataField::new("name", DataType::String, false)]);

        let table_info = TableInfo {
            desc: "'system'.'databases'".to_string(),
            name: "databases".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemDatabases".to_string(),
                ..Default::default()
            },
        };

        DatabasesTable { table_info }
    }
}

#[async_trait::async_trait]
impl Table for DatabasesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let dbs = ctx.get_catalog().list_databases().await?;

        let db_names: Vec<&[u8]> = dbs
            .iter()
            .map(|database| database.name().as_bytes())
            .collect();

        let block =
            DataBlock::create_by_array(self.table_info.schema(), vec![Series::new(db_names)]);

        let s = Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        ));
        Ok(s)
    }
}
