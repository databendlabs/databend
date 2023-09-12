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

use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::FromOptData;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_pipeline_core::query_spill_prefix;
use common_storage::DataOperator;
use opendal::Metakey;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct TempFilesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for TempFilesTable {
    const NAME: &'static str = "system.temp_files";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let operator = DataOperator::instance().operator();

        let mut temp_files_path = vec![];
        let mut temp_files_content_length = vec![];
        let mut temp_files_last_modified = vec![];

        let location_prefix = format!("{}/", query_spill_prefix(&tenant));
        if let Ok(mut lister) = operator.list(&location_prefix).await {
            let limit = push_downs.and_then(|x| x.limit).unwrap_or(usize::MAX);

            while let Some(page) = lister.next_page().await? {
                if temp_files_path.len() >= limit {
                    break;
                }

                temp_files_path.reserve(page.len());
                temp_files_last_modified.reserve(page.len());
                temp_files_content_length.reserve(page.len());

                for entry in page {
                    if temp_files_path.len() >= limit {
                        break;
                    }

                    let metadata = operator.metadata(&entry, Metakey::Complete).await?;

                    if metadata.is_file() {
                        temp_files_path.push(entry.path().as_bytes().to_vec());

                        temp_files_last_modified
                            .push(metadata.last_modified().map(|x| x.timestamp_micros()));
                        temp_files_content_length.push(metadata.content_length());
                    }
                }
            }
        }

        let num_rows = temp_files_path.len();
        let data_block = DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String("Spill".as_bytes().to_owned())),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Column(StringType::from_data(temp_files_path)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(NumberType::from_data(temp_files_content_length)),
                ),
                BlockEntry::new(
                    DataType::Timestamp.wrap_nullable(),
                    Value::Column(TimestampType::from_opt_data(temp_files_last_modified)),
                ),
            ],
            num_rows,
        );

        Ok(data_block.convert_to_full())
    }
}

impl TempFilesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("file_type", TableDataType::String),
            TableField::new("file_path", TableDataType::String),
            TableField::new(
                "file_content_length",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "file_last_modified_time",
                TableDataType::Timestamp.wrap_nullable(),
            ),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'temp_files'".to_string(),
            name: "temp_files".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemTempFilesTable".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(Self { table_info })
    }
}
