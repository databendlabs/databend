// Copyright 2023 Datafuse Labs
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
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::types::UInt32Type;
use common_expression::types::UInt64Type;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct QueryProfileTable {
    table_info: TableInfo,
}

impl QueryProfileTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("query_id", TableDataType::String),
            TableField::new("plan_id", TableDataType::Number(NumberDataType::UInt32)),
            TableField::new("plan_name", TableDataType::String),
            TableField::new("description", TableDataType::String),
            TableField::new("cpu_time", TableDataType::Number(NumberDataType::UInt64)),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'query_profile'".to_string(),
            ident: TableIdent::new(table_id, 0),
            name: "query_profile".to_string(),
            meta: TableMeta {
                schema,
                engine: "QueryProfile".to_string(),
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

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> common_exception::Result<DataBlock> {
        let profile_mgr = ctx.get_query_profile_manager();
        let query_profs = profile_mgr.list_all();

        let mut query_ids: Vec<Vec<u8>> = Vec::with_capacity(query_profs.len());
        let mut plan_ids: Vec<u32> = Vec::with_capacity(query_profs.len());
        let mut plan_names: Vec<Vec<u8>> = Vec::with_capacity(query_profs.len());
        let mut descriptions: Vec<Vec<u8>> = Vec::with_capacity(query_profs.len());
        let mut cpu_times: Vec<u64> = Vec::with_capacity(query_profs.len());

        for prof in query_profs.iter() {
            for plan_prof in prof.plan_node_profs.iter() {
                query_ids.push(prof.query_id.clone().into_bytes());
                plan_ids.push(plan_prof.id);
                plan_names.push(plan_prof.plan_node_name.clone().into_bytes());
                descriptions.push(plan_prof.description.clone().into_bytes());
                cpu_times.push(plan_prof.cpu_time.as_nanos() as u64);
            }
        }

        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(query_ids),
            UInt32Type::from_data(plan_ids),
            StringType::from_data(plan_names),
            StringType::from_data(descriptions),
            UInt64Type::from_data(cpu_times),
        ]);

        Ok(block)
    }
}
