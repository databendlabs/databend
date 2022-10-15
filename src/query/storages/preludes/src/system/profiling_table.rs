use std::sync::Arc;

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use common_datavalues::Vu8;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;

use crate::system::SyncOneBlockSystemTable;
use crate::system::SyncSystemTable;

pub struct ProfilingTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl SyncSystemTable for ProfilingTable {
    const NAME: &'static str = "system.profiling";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let processes_profile_info = ctx.get_profiling_infos();

        let mut process_id = Vec::with_capacity(processes_profile_info.len());
        let mut process_rows = Vec::with_capacity(processes_profile_info.len());
        let mut process_bytes = Vec::with_capacity(processes_profile_info.len());
        let mut processor_name = Vec::with_capacity(processes_profile_info.len());
        let mut process_extra_info = Vec::with_capacity(processes_profile_info.len());

        for (id, items) in processes_profile_info.into_iter() {
            for item in &items {
                process_id.push(id.clone().into_bytes());
                process_rows.push(item.process_rows() as u64);
                process_bytes.push(item.process_bytes() as u64);
                processor_name.push(item.processor_name().into_bytes());

                match item.extra_info() {
                    None => process_extra_info.push(vec![]),
                    Some(extra_info) => process_extra_info.push(extra_info.display_data().into_bytes()),
                };
            }
        }

        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(process_id),
            Series::from_data(processor_name),
            Series::from_data(process_rows),
            Series::from_data(process_bytes),
            Series::from_data(process_extra_info),
        ]))
    }
}

impl ProfilingTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("id", Vu8::to_data_type()),
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("process_rows", u64::to_data_type()),
            DataField::new("process_bytes", u64::to_data_type()),
            DataField::new("extra_info", Vu8::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'profiling'".to_string(),
            name: "profiling".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemProfiling".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(ProfilingTable { table_info })
    }
}
