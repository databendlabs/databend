// Copyright 2021 Datafuse Labs.
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
use std::sync::Arc;

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
use crate::sessions::QueryContext;
use crate::storages::Table;

pub struct EnginesTable {
    table_info: TableInfo,
}

impl EnginesTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Engine", Vu8::to_data_type()),
            DataField::new("Comment", Vu8::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'engines'".to_string(),
            name: "engines".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemEngines".to_string(),
                ..Default::default()
            },
        };
        EnginesTable { table_info }
    }
}

#[async_trait::async_trait]
impl Table for EnginesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        _ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let table_engine_descriptors = _ctx.get_catalog().get_table_engines();
        let mut engine_name = Vec::with_capacity(table_engine_descriptors.len());
        let mut engine_comment = Vec::with_capacity(table_engine_descriptors.len());
        for descriptor in &table_engine_descriptors {
            engine_name.push(descriptor.engine_name.clone());
            engine_comment.push(descriptor.comment.clone());
        }
        let block = DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(engine_name),
            Series::from_data(engine_comment),
        ]);
        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }
}
