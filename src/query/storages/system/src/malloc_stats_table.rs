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

use std::default::Default;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::Value;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct MallocStatsTable {
    table_info: TableInfo,
}

impl SyncSystemTable for MallocStatsTable {
    const NAME: &'static str = "system.malloc_stats";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, _ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let values = Self::build_columns().map_err(convert_je_err)?;
        Ok(DataBlock::new(values, 1))
    }
}

type BuildResult = std::result::Result<Vec<BlockEntry>, Box<dyn std::error::Error>>;

impl MallocStatsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema =
            TableSchemaRefExt::create(vec![TableField::new("statistics", TableDataType::Variant)]);

        let table_info = TableInfo {
            desc: "'system'.'malloc_stats'".to_string(),
            name: "malloc_stats".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemMetrics".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(MallocStatsTable { table_info })
    }

    fn build_columns() -> BuildResult {
        let mut buf = vec![];
        let mut options = tikv_jemalloc_ctl::stats_print::Options::default();
        // always return as json
        options.json_format = true;
        options.skip_constants = true;
        options.skip_mutex_statistics = true;
        options.skip_per_arena = true;

        tikv_jemalloc_ctl::stats_print::stats_print(&mut buf, options)?;
        let json_value: serde_json::Value = serde_json::from_slice(&buf)?;
        let jsonb_value: jsonb::Value = (&json_value).into();
        Ok(vec![BlockEntry::new(
            DataType::Variant,
            Value::Scalar(Scalar::Variant(jsonb_value.to_vec())),
        )])
    }
}

fn convert_je_err(je_err: Box<dyn std::error::Error>) -> ErrorCode {
    ErrorCode::Internal(format!("{}", je_err))
}
