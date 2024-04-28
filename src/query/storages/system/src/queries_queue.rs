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
use std::time::Duration;

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

pub struct QueriesQueueTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl SyncSystemTable for QueriesQueueTable {
    const NAME: &'static str = "system.queries_queue";

    const IS_LOCAL: bool = false;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let processes_info = ctx.get_queued_queries();

        let local_node = ctx.get_cluster().local_id.clone();

        let mut nodes = Vec::with_capacity(processes_info.len());
        let mut processes_id = Vec::with_capacity(processes_info.len());
        let mut processes_type = Vec::with_capacity(processes_info.len());
        let mut processes_host = Vec::with_capacity(processes_info.len());
        let mut processes_user = Vec::with_capacity(processes_info.len());
        let mut processes_mysql_connection_id = Vec::with_capacity(processes_info.len());
        let mut processes_time = Vec::with_capacity(processes_info.len());

        for process_info in &processes_info {
            let time = process_info
                .created_time
                .elapsed()
                .unwrap_or(Duration::from_secs(0))
                .as_secs();

            nodes.push(local_node.clone());
            processes_id.push(process_info.id.clone());
            processes_type.push(process_info.typ.clone());
            processes_host.push(process_info.client_address.clone());
            processes_user.push(Self::process_option_value(process_info.user.clone()).name);
            processes_mysql_connection_id.push(process_info.mysql_connection_id);
            processes_time.push(time);
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(nodes),
            StringType::from_data(processes_id),
            StringType::from_data(processes_type),
            StringType::from_opt_data(processes_host),
            StringType::from_data(processes_user),
            UInt32Type::from_opt_data(processes_mysql_connection_id),
            UInt64Type::from_data(processes_time),
        ]))
    }
}

impl QueriesQueueTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("node", TableDataType::String),
            TableField::new("id", TableDataType::String),
            TableField::new("type", TableDataType::String),
            TableField::new(
                "host",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("user", TableDataType::String),
            TableField::new(
                "mysql_connection_id",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt32))),
            ),
            TableField::new("wait_time", TableDataType::Number(NumberDataType::UInt64)),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'queries_queue'".to_string(),
            name: "queries_queue".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemQueriesQueue".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(QueriesQueueTable { table_info })
    }

    fn process_option_value<T>(opt: Option<T>) -> T
    where T: Default {
        opt.unwrap_or_default()
    }
}
