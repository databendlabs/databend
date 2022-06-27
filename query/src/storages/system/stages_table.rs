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

use std::sync::Arc;
use std::vec;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_types::StageType;

use super::table::AsyncOneBlockSystemTable;
use super::table::AsyncSystemTable;
use crate::sessions::QueryContext;
use crate::storages::Table;

pub struct StagesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for StagesTable {
    const NAME: &'static str = "system.stages";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<QueryContext>) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let stages = ctx.get_user_manager().get_stages(&tenant).await?;
        let mut name: Vec<Vec<u8>> = Vec::with_capacity(stages.len());
        let mut stage_type: Vec<Vec<u8>> = Vec::with_capacity(stages.len());
        let mut stage_params: Vec<Vec<u8>> = Vec::with_capacity(stages.len());
        let mut copy_options: Vec<Vec<u8>> = Vec::with_capacity(stages.len());
        let mut file_format_options: Vec<Vec<u8>> = Vec::with_capacity(stages.len());
        let mut comment: Vec<Vec<u8>> = Vec::with_capacity(stages.len());
        let mut number_of_files: Vec<Option<u64>> = Vec::with_capacity(stages.len());
        let mut creator: Vec<Option<Vec<u8>>> = Vec::with_capacity(stages.len());
        for stage in stages.into_iter() {
            name.push(stage.stage_name.clone().into_bytes());
            stage_type.push(stage.stage_type.clone().to_string().into_bytes());
            stage_params.push(format!("{:?}", stage.stage_params).into_bytes());
            copy_options.push(format!("{:?}", stage.copy_options).into_bytes());
            file_format_options.push(format!("{:?}", stage.file_format_options).into_bytes());
            match stage.stage_type {
                StageType::Internal => {
                    number_of_files.push(Some(stage.number_of_files));
                }
                StageType::External => {
                    number_of_files.push(None);
                }
            };
            creator.push(stage.creator.map(|c| c.to_string().into_bytes()));
            comment.push(stage.comment.clone().into_bytes());
        }
        Ok(DataBlock::create(self.table_info.schema(), vec![
            Series::from_data(name),
            Series::from_data(stage_type),
            Series::from_data(stage_params),
            Series::from_data(copy_options),
            Series::from_data(file_format_options),
            Series::from_data(number_of_files),
            Series::from_data(creator),
            Series::from_data(comment),
        ]))
    }
}

impl StagesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("stage_type", Vu8::to_data_type()),
            DataField::new("stage_params", Vu8::to_data_type()),
            DataField::new("copy_options", Vu8::to_data_type()),
            DataField::new("file_format_options", Vu8::to_data_type()),
            // NULL for external stage
            DataField::new_nullable("number_of_files", u64::to_data_type()),
            DataField::new_nullable("creator", Vu8::to_data_type()),
            DataField::new("comment", Vu8::to_data_type()),
        ]);
        let table_info = TableInfo {
            desc: "'system'.'stages'".to_string(),
            name: "stages".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemStages".to_string(),
                ..Default::default()
            },
        };

        AsyncOneBlockSystemTable::create(StagesTable { table_info })
    }
}
