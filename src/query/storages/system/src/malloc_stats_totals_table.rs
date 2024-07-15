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
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::ValueType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use tikv_jemalloc_ctl::epoch;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

macro_rules! set_value {
    ($stat:ident, $names:expr, $values:expr) => {
        let mib = $stat::mib()?;
        let value = mib.read()?;
        $names.put_str(&String::from_utf8_lossy($stat::name().as_bytes()));
        $names.commit_row();
        $values.push(value as u64);
    };
}

pub struct MallocStatsTotalsTable {
    table_info: TableInfo,
}

impl SyncSystemTable for MallocStatsTotalsTable {
    const NAME: &'static str = "system.malloc_stats_totals";
    const IS_LOCAL: bool = false;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let local_id = ctx.get_cluster().local_id.clone();
        let values = Self::build_columns(&local_id).map_err(convert_je_err)?;
        Ok(DataBlock::new_from_columns(values))
    }
}

type BuildResult = std::result::Result<Vec<Column>, Box<dyn std::error::Error>>;

impl MallocStatsTotalsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("node", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new("value", TableDataType::Number(NumberDataType::UInt64)),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'malloc_stats_totals'".to_string(),
            name: "malloc_stats_totals".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemMetrics".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(MallocStatsTotalsTable { table_info })
    }

    fn build_columns(node_name: &str) -> BuildResult {
        let mut names = StringColumnBuilder::with_capacity(6, 6 * 4);
        let mut values: Vec<u64> = vec![];

        let e = epoch::mib()?;
        e.advance()?;

        use tikv_jemalloc_ctl::stats::active;
        use tikv_jemalloc_ctl::stats::allocated;
        use tikv_jemalloc_ctl::stats::mapped;
        use tikv_jemalloc_ctl::stats::metadata;
        use tikv_jemalloc_ctl::stats::resident;
        use tikv_jemalloc_ctl::stats::retained;

        set_value!(active, names, values);
        set_value!(allocated, names, values);
        set_value!(retained, names, values);
        set_value!(mapped, names, values);
        set_value!(resident, names, values);
        set_value!(metadata, names, values);

        let node_names = Column::String(StringColumnBuilder::repeat(node_name, 6).build());
        let names = StringType::upcast_column(names.build());
        let values = NumberType::<u64>::upcast_column(values.into());

        Ok(vec![node_names, names, values])
    }
}

fn convert_je_err(je_err: Box<dyn std::error::Error>) -> ErrorCode {
    ErrorCode::Internal(format!("{}", je_err))
}
