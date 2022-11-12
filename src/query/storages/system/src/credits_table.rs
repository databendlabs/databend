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

use std::sync::Arc;

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_expression::DataType;
use common_expression::SchemaDataType;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct CreditsTable {
    table_info: TableInfo,
}

impl SyncSystemTable for CreditsTable {
    const NAME: &'static str = "system.credits";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, _: Arc<dyn TableContext>) -> Result<Chunk> {
        let names: Vec<&[u8]> = env!("DATABEND_CREDITS_NAMES")
            .split_terminator(',')
            .map(|x| x.trim().as_bytes())
            .collect();
        let versions: Vec<&[u8]> = env!("DATABEND_CREDITS_VERSIONS")
            .split_terminator(',')
            .map(|x| x.trim().as_bytes())
            .collect();
        let licenses: Vec<&[u8]> = env!("DATABEND_CREDITS_LICENSES")
            .split_terminator(',')
            .map(|x| x.trim().as_bytes())
            .collect();

        let rows_len = names.len();
        Ok(Chunk::new(
            vec![
                (Value::Column(Column::from_data(names)), DataType::String),
                (Value::Column(Column::from_data(versions)), DataType::String),
                (Value::Column(Column::from_data(licenses)), DataType::String),
            ],
            rows_len,
        ))
    }
}

impl CreditsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", SchemaDataType::String),
            DataField::new("version", SchemaDataType::String),
            DataField::new("license", SchemaDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'credits'".to_string(),
            name: "credits".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemCredits".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(CreditsTable { table_info })
    }
}
