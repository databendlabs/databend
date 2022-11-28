// Copyright 2022 Datafuse Labs.
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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

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

    fn get_full_data(&self, _: Arc<dyn TableContext>) -> Result<DataBlock> {
        let values = Self::build_columns().map_err(convert_je_err)?;
        Ok(DataBlock::create(self.table_info.schema(), vec![
            VariantObjectType {}.create_column(&values)?,
        ]))
    }
}

impl MallocStatsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![DataField::new(
            "statistics",
            VariantObjectType::new_impl(),
        )]);

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

    fn build_columns() -> std::result::Result<Vec<DataValue>, Box<dyn std::error::Error>> {
        let mut buf = vec![];
        let mut options = tikv_jemalloc_ctl::stats_print::Options::default();
        // always return as json
        options.json_format = true;
        options.skip_constants = true;
        options.skip_mutex_statistics = true;
        options.skip_per_arena = true;

        tikv_jemalloc_ctl::stats_print::stats_print(&mut buf, options)?;
        let json_value: serde_json::Value = serde_json::from_slice(&buf)?;
        let data_value = DataValue::Variant(VariantValue::from(json_value));

        Ok(vec![data_value])
    }
}

fn convert_je_err(je_err: Box<dyn std::error::Error>) -> ErrorCode {
    ErrorCode::Internal(format!("{}", je_err))
}
