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

use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::number::UInt8Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;

use crate::table::SystemTablePart;
use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct OneTable {
    table_info: TableInfo,
}

impl SyncSystemTable for OneTable {
    const NAME: &'static str = "system.one";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, _ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        Ok(DataBlock::new_from_columns(vec![UInt8Type::from_data(
            vec![1u8],
        )]))
    }

    fn get_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((
            PartStatistics::new_exact(1, 1, 1, 1),
            Partitions::create(PartitionsShuffleKind::Seq, vec![Arc::new(Box::new(
                SystemTablePart,
            ))]),
        ))
    }
}

impl OneTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![TableField::new(
            "dummy",
            TableDataType::Number(NumberDataType::UInt8),
        )]);

        let table_info = TableInfo {
            desc: "'system'.'one'".to_string(),
            name: "one".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemOne".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(OneTable { table_info })
    }
}
