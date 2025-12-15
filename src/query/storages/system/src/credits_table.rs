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

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::StringType;
use databend_common_expression::utils::FromData;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct CreditsTable {
    table_info: TableInfo,
    data: DataBlock,
}

impl SyncSystemTable for CreditsTable {
    const NAME: &'static str = "system.credits";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, _: Arc<dyn TableContext>) -> Result<DataBlock> {
        Ok(self.data.clone())
    }
}

impl CreditsTable {
    pub fn create(
        table_id: u64,
        credits_names: &str,
        credits_versions: &str,
        credits_licenses: &str,
    ) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("version", TableDataType::String),
            TableField::new("license", TableDataType::String),
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

        let names: Vec<String> = credits_names
            .split_terminator(',')
            .map(|x| x.trim().to_string())
            .collect();
        let versions: Vec<String> = credits_versions
            .split_terminator(',')
            .map(|x| x.trim().to_string())
            .collect();
        let licenses: Vec<String> = credits_licenses
            .split_terminator(',')
            .map(|x| x.trim().to_string())
            .collect();
        assert_eq!(names.len(), versions.len());
        assert_eq!(names.len(), licenses.len());

        let data = DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(versions),
            StringType::from_data(licenses),
        ]);

        SyncOneBlockSystemTable::create(CreditsTable { table_info, data })
    }
}
