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

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::ReadDataSourcePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::sessions::QueryContext;
use crate::storages::Table;

pub struct SettingsTable {
    table_info: TableInfo,
}

impl SettingsTable {
    pub fn create(table_id: u64) -> Self {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("value", Vu8::to_data_type()),
            DataField::new("default", Vu8::to_data_type()),
            DataField::new("level", Vu8::to_data_type()),
            DataField::new("description", Vu8::to_data_type()),
            DataField::new("type", Vu8::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'settings'".to_string(),
            name: "settings".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemSettings".to_string(),

                ..Default::default()
            },
        };

        SettingsTable { table_info }
    }
}

#[async_trait::async_trait]
impl Table for SettingsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let settings = ctx.get_settings().get_setting_values();

        let mut names: Vec<String> = vec![];
        let mut values: Vec<String> = vec![];
        let mut defaults: Vec<String> = vec![];
        let mut levels: Vec<String> = vec![];
        let mut descs: Vec<String> = vec![];
        let mut types: Vec<String> = vec![];
        for setting in settings {
            if let DataValue::Struct(vals) = setting {
                // Name.
                names.push(format!("{:?}", vals[0]));
                // Value.
                values.push(format!("{:?}", vals[1]));
                // Default Value.
                defaults.push(format!("{:?}", vals[2]));
                // Scope level.
                levels.push(format!("{:?}", vals[3]));
                // Desc.
                descs.push(format!("{:?}", vals[4]));
                // Types.
                types.push(format!("{:?}", vals[2].max_data_type()));
            }
        }

        let names: Vec<&[u8]> = names.iter().map(|x| x.as_bytes()).collect();
        let values: Vec<&[u8]> = values.iter().map(|x| x.as_bytes()).collect();
        let defaults: Vec<&[u8]> = defaults.iter().map(|x| x.as_bytes()).collect();
        let levels: Vec<&[u8]> = levels.iter().map(|x| x.as_bytes()).collect();
        let descs: Vec<&[u8]> = descs.iter().map(|x| x.as_bytes()).collect();
        let types: Vec<&[u8]> = types.iter().map(|x| x.as_bytes()).collect();
        let block = DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(names),
            Series::from_data(values),
            Series::from_data(defaults),
            Series::from_data(levels),
            Series::from_data(descs),
            Series::from_data(types),
        ]);

        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }
}
