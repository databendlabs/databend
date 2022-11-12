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

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_expression::DataType;
use common_expression::NumberDataType;
use common_expression::SchemaDataType;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_types::StageType;
use common_users::UserApiProvider;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct StagesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for StagesTable {
    const NAME: &'static str = "system.stages";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<Chunk> {
        let tenant = ctx.get_tenant();
        let stages = UserApiProvider::instance().get_stages(&tenant).await?;
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
            // TODO(xuanwo): we will remove this line.
            match stage.stage_type {
                StageType::LegacyInternal | StageType::Internal | StageType::User => {
                    number_of_files.push(Some(stage.number_of_files));
                }
                StageType::External => {
                    number_of_files.push(None);
                }
            };
            creator.push(stage.creator.map(|c| c.to_string().into_bytes()));
            comment.push(stage.comment.clone().into_bytes());
        }

        let rows_len = names.len();
        Ok(Chunk::new(
            vec![
                (Value::Column(Column::from_data(names)), DataType::String),
                (
                    Value::Column(Column::from_data(stage_type)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(stage_params)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(copy_options)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(file_format_options)),
                    DataType::String,
                ),
                (
                    Value::Column(Column::from_data(number_of_files)),
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
                ),
                (
                    Value::Column(Column::from_data(creator)),
                    DataType::Nullable(Box::new(DataType::String)),
                ),
                (Value::Column(Column::from_data(comment)), DataType::String),
            ],
            rows_len,
        ))
    }
}

impl StagesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("name", SchemaDataType::String),
            DataField::new("stage_type", SchemaDataType::String),
            DataField::new("stage_params", SchemaDataType::String),
            DataField::new("copy_options", SchemaDataType::String),
            DataField::new("file_format_options", SchemaDataType::String),
            // NULL for external stage
            DataField::new_nullable(
                "number_of_files",
                SchemaDataType::Nullable(Box::new(SchemaDataType::Number(NumberDataType::UInt64))),
            ),
            DataField::new_nullable(
                "creator",
                SchemaDataType::Nullable(Box::new(SchemaDataType::String)),
            ),
            DataField::new("comment", SchemaDataType::String),
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
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(StagesTable { table_info })
    }
}
