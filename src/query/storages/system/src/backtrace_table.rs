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

use common_base::dump_backtrace;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct BacktraceTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl SyncSystemTable for BacktraceTable {
    const NAME: &'static str = "system.backtrace";

    // Allow distributed query.
    const IS_LOCAL: bool = false;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let node_id = ctx.get_cluster().local_id.clone();
        let stack = dump_backtrace(false);

        let mut nodes = ColumnBuilder::with_capacity(&DataType::String, 1);
        nodes.push(Scalar::String(node_id.as_bytes().to_vec()).as_ref());

        let mut stacks = ColumnBuilder::with_capacity(&DataType::String, 1);
        stacks.push(Scalar::String(stack.as_bytes().to_vec()).as_ref());

        Ok(DataBlock::new_from_columns(vec![
            nodes.build(),
            stacks.build(),
        ]))
    }
}

impl BacktraceTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("node", TableDataType::String),
            TableField::new("stack", TableDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'backtrace'".to_string(),
            name: "backtrace".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemBacktrace".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(BacktraceTable { table_info })
    }
}
