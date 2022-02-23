// Copyright 2021 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_tracing::tracing;

use crate::sessions::QueryContext;
use crate::storages::system::table::SyncOneBlockSystemTable;
use crate::storages::system::table::SyncSystemTable;
use crate::storages::Table;

pub struct CreditsTable {
    table_info: TableInfo,
}

impl SyncSystemTable for CreditsTable {
    const NAME: &'static str = "system.credits";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, _: Arc<QueryContext>) -> Result<DataBlock> {
        let metadata_command = cargo_metadata::MetadataCommand::new();

        let deps =
            match cargo_license::get_dependencies_from_cargo_lock(metadata_command, false, false) {
                Ok(v) => v,
                Err(err) => {
                    tracing::error!("{:?}", err);
                    vec![]
                }
            };

        let names: Vec<&[u8]> = deps.iter().map(|x| x.name.as_bytes()).collect();
        let version_strings: Vec<String> = deps.iter().map(|x| x.version.to_string()).collect();
        let versions: Vec<&[u8]> = version_strings.iter().map(|x| x.as_bytes()).collect();
        let licenses: Vec<&[u8]> = deps
            .iter()
            .map(|x| match &x.license {
                None => b"UNKNOWN",
                Some(license) => license.as_bytes(),
            })
            .collect();

        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(names),
            Series::from_data(versions),
            Series::from_data(licenses),
        ]))
    }
}

impl CreditsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("version", Vu8::to_data_type()),
            DataField::new("license", Vu8::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'credits'".to_string(),
            name: "credits".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemCredits".to_string(),
                ..Default::default()
            },
        };

        SyncOneBlockSystemTable::create(CreditsTable { table_info })
    }
}
