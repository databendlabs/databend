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

use common_context::IOContext;
use common_context::TableIOContext;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::catalogs::Table;
use crate::sessions::DatabendQueryContext;

pub struct EnginesTable {
    table_info: TableInfo,
}

impl EnginesTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", DataType::String, false),
            DataField::new("description", DataType::String, false),
        ]);

        let table_info = TableInfo {
            db: "system".to_string(),
            name: "engines".to_string(),
            table_id,
            schema,
            engine: "SystemEngines".to_string(),

            ..Default::default()
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
        io_ctx: Arc<TableIOContext>,
        _push_downs: &Option<Extras>,
    ) -> Result<SendableDataBlockStream> {
        let ctx: Arc<DatabendQueryContext> = io_ctx
            .get_user_data()?
            .expect("DatabendQueryContext should not be None");

        let engines = ctx.get_catalog().get_db_engines()?;
        let mut names: Vec<String> = vec![];
        let mut descs: Vec<String> = vec![];
        for description in engines.iter() {
            names.push(description.name.clone());
            descs.push(description.desc.clone());
        }

        let names: Vec<&[u8]> = names.iter().map(|x| x.as_bytes()).collect();
        let descs: Vec<&[u8]> = descs.iter().map(|x| x.as_bytes()).collect();
        let block = DataBlock::create_by_array(self.table_info.schema.clone(), vec![
            Series::new(names),
            Series::new(descs),
        ]);
        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema.clone(),
            None,
            vec![block],
        )))
    }
}
