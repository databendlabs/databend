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

use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct ProcessorProfileTable {
    table_info: TableInfo,
}

impl SyncSystemTable for ProcessorProfileTable {
    const NAME: &'static str = "system.processor_profile";

    const IS_LOCAL: bool = false;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let queries_profiles = ctx.get_queries_profile();

        let local_id = ctx.get_cluster().local_id.clone();
        let total_size = queries_profiles.values().map(Vec::len).sum();

        let mut node: Vec<Vec<u8>> = Vec::with_capacity(total_size);
        let mut queries_id: Vec<Vec<u8>> = Vec::with_capacity(total_size);
        let mut pid: Vec<u64> = Vec::with_capacity(total_size);
        let mut p_name: Vec<Vec<u8>> = Vec::with_capacity(total_size);
        let mut plan_id: Vec<Option<u32>> = Vec::with_capacity(total_size);
        let mut parent_id: Vec<Option<u32>> = Vec::with_capacity(total_size);
        let mut plan_name: Vec<Option<Vec<u8>>> = Vec::with_capacity(total_size);
        let mut cpu_time: Vec<u64> = Vec::with_capacity(total_size);
        let mut wait_time: Vec<u64> = Vec::with_capacity(total_size);
        let mut exchange_rows: Vec<u64> = Vec::with_capacity(total_size);
        let mut exchange_bytes: Vec<u64> = Vec::with_capacity(total_size);

        for (query_id, query_profiles) in queries_profiles {
            for query_profile in query_profiles {
                node.push(local_id.clone().into_bytes());
                queries_id.push(query_id.clone().into_bytes());
                pid.push(query_profile.pid as u64);
                p_name.push(query_profile.p_name.clone().into_bytes());
                plan_id.push(query_profile.plan_id);
                parent_id.push(query_profile.plan_parent_id);
                plan_name.push(query_profile.plan_name.clone().map(String::into_bytes));

                cpu_time.push(query_profile.cpu_time.load(Ordering::Relaxed));
                wait_time.push(query_profile.wait_time.load(Ordering::Relaxed));
                exchange_rows.push(query_profile.exchange_rows.load(Ordering::Relaxed) as u64);
                exchange_bytes.push(query_profile.exchange_bytes.load(Ordering::Relaxed) as u64);
            }
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(node),
            StringType::from_data(queries_id),
            UInt64Type::from_data(pid),
            StringType::from_data(p_name),
            UInt32Type::from_opt_data(plan_id),
            UInt32Type::from_opt_data(parent_id),
            StringType::from_opt_data(plan_name),
            UInt64Type::from_data(cpu_time),
            UInt64Type::from_data(wait_time),
            UInt64Type::from_data(exchange_rows),
            UInt64Type::from_data(exchange_bytes),
        ]))
    }
}

impl ProcessorProfileTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("node", TableDataType::String),
            TableField::new("query_id", TableDataType::String),
            TableField::new("pid", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("pname", TableDataType::String),
            TableField::new(
                "plan_id",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt32))),
            ),
            TableField::new(
                "parent_plan_id",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt32))),
            ),
            TableField::new(
                "plan_name",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("cpu_time", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("wait_time", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "exchange_rows",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "exchange_bytes",
                TableDataType::Number(NumberDataType::UInt64),
            ),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'processor_profile'".to_string(),
            ident: TableIdent::new(table_id, 0),
            name: "processor_profile".to_string(),
            meta: TableMeta {
                schema,
                engine: "ProcessorProfileTable".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(Self { table_info })
    }
}
