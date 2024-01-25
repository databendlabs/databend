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

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct EnginesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for EnginesTable {
    const NAME: &'static str = "system.engines";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        // TODO passing catalog name
        let table_engine_descriptors = ctx.get_catalog(CATALOG_DEFAULT).await?.get_table_engines();
        let mut engine_name = Vec::with_capacity(table_engine_descriptors.len());
        let mut engine_comment = Vec::with_capacity(table_engine_descriptors.len());
        for descriptor in &table_engine_descriptors {
            engine_name.push(descriptor.engine_name.clone());
            engine_comment.push(descriptor.comment.clone());
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(engine_name),
            StringType::from_data(engine_comment),
        ]))
    }
}

impl EnginesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("Engine", TableDataType::String),
            TableField::new("Comment", TableDataType::String),
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
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(EnginesTable { table_info })
    }
}
