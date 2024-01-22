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
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::VariantType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_profile::OperatorExecutionInfo;
use databend_common_profile::QueryProfileManager;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

fn encode_operator_execution_info(info: &OperatorExecutionInfo) -> jsonb::Value {
    // Process time represent with number of milliseconds.
    let process_time = info.process_time.as_nanos() as f64 / 1e6;
    (&serde_json::json!({
        "process_time": process_time,
        "input_rows": info.input_rows,
        "input_bytes": info.input_bytes,
        "output_rows": info.output_rows,
        "output_bytes": info.output_bytes,
    }))
        .into()
}

pub struct QueryProfileTable {
    table_info: TableInfo,
}

impl QueryProfileTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("query_id", TableDataType::String),
            TableField::new("operator_id", TableDataType::Number(NumberDataType::UInt32)),
            TableField::new("execution_info", TableDataType::Variant),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'query_profile'".to_string(),
            ident: TableIdent::new(table_id, 0),
            name: "query_profile".to_string(),
            meta: TableMeta {
                schema,
                engine: "QueryProfileTable".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(Self { table_info })
    }
}

impl SyncSystemTable for QueryProfileTable {
    const NAME: &'static str = "system.query_profile";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(
        &self,
        _ctx: Arc<dyn TableContext>,
    ) -> databend_common_exception::Result<DataBlock> {
        let profile_mgr = QueryProfileManager::instance();
        let query_profs = profile_mgr.list_all();

        let mut query_ids: Vec<String> = Vec::with_capacity(query_profs.len());
        let mut operator_ids: Vec<u32> = Vec::with_capacity(query_profs.len());
        let mut execution_infos: Vec<Vec<u8>> = Vec::with_capacity(query_profs.len());

        for prof in query_profs.iter() {
            for plan_prof in prof.operator_profiles.iter() {
                query_ids.push(prof.query_id.clone());
                operator_ids.push(plan_prof.id);

                let execution_info = encode_operator_execution_info(&plan_prof.execution_info);
                execution_infos.push(execution_info.to_vec());
            }
        }

        let block = DataBlock::new_from_columns(vec![
            // query_id
            StringType::from_data(query_ids),
            // operator_id
            UInt32Type::from_data(operator_ids),
            // execution_info
            VariantType::from_data(execution_infos),
        ]);

        Ok(block)
    }
}
