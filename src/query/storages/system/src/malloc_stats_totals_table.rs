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
use tikv_jemalloc_ctl::epoch;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

macro_rules! set_value {
    ($stat:ident, $names:expr, $values:expr) => {
        let mib = $stat::mib()?;
        let value = mib.read()?;
        $names.push($stat::name().to_string().into_bytes());
        $values.push(value as u64);
    };
}

pub struct MallocStatsTotalsTable {
    table_info: TableInfo,
}

impl SyncSystemTable for MallocStatsTotalsTable {
    const NAME: &'static str = "system.malloc_stats_totals";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, _: Arc<dyn TableContext>) -> Result<DataBlock> {
        let (names, values) = Self::build_columns().map_err(convert_je_err)?;
        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(names),
            Series::from_data(values),
        ]))
    }
}

impl MallocStatsTotalsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("value", u64::to_data_type()),
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

    fn build_columns() -> std::result::Result<(Vec<Vec<u8>>, Vec<u64>), tikv_jemalloc_ctl::Error> {
        let mut names: Vec<Vec<u8>> = vec![];
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

        Ok((names, values))
    }
}

fn convert_je_err(je_err: tikv_jemalloc_ctl::Error) -> ErrorCode {
    ErrorCode::Internal(format!("{}", je_err))
}
