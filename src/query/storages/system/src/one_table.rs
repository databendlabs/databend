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

use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::number::UInt8Type;
use common_expression::types::NumberDataType;
use common_expression::utils::FromData;
use common_expression::DataBlock;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

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
            Partitions::create_nolazy(PartitionsShuffleKind::Seq, vec![Arc::new(Box::new(
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
