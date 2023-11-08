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

use common_arrow::arrow::datatypes::Field;
use common_arrow::native::read::reader::NativeReader;
use common_arrow::native::stat::stat_simple;
use common_arrow::native::stat::PageBody;
use common_arrow::native::stat::PageInfo;
use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::nullable::NullableColumnBuilder;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::types::UInt32Type;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use storages_common_table_meta::meta::SegmentInfo;

use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::FuseTable;

pub struct FuseEncoding<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
    pub column: String,
    pub limit: Option<usize>,
}

impl<'a> FuseEncoding<'a> {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        table: &'a FuseTable,
        column: String,
        limit: Option<usize>,
    ) -> Self {
        Self {
            ctx,
            table,
            column,
            limit,
        }
    }

    #[async_backtrace::framed]
    pub async fn get_blocks(&self) -> Result<DataBlock> {
        let snapshot = self.table.read_table_snapshot().await?;
        if snapshot.is_none() {
            return Ok(DataBlock::empty_with_schema(Arc::new(
                Self::schema().into(),
            )));
        }
        let snapshot = snapshot.unwrap();

        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.table.operator.clone(),
            self.table.schema(),
        );

        let chunk_size = self.ctx.get_settings().get_max_threads()? as usize * 4;

        let schema = self.table.schema();
        let field = schema.field_with_name(&self.column)?;
        if field.is_nested() {
            return Err(ErrorCode::Unimplemented(format!(
                "Nested column {} is not supported yet",
                self.column
            )));
        }
        let column_id = field.column_id;
        let arrow_field: Field = field.into();
        let mut pages_info = vec![];
        for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, false)
                .await?;
            for segment in segments {
                let segment = segment?;
                for block in segment.blocks.iter() {
                    let column_meta = block.col_metas.get(&column_id).unwrap();
                    let (offset, len) = column_meta.offset_length();
                    let ranges = vec![(column_id, offset..(offset + len))];
                    let read_settings = ReadSettings::from_ctx(&self.ctx)?;
                    let merge_io_read_res = BlockReader::merge_io_read(
                        &read_settings,
                        self.table.operator.clone(),
                        &block.location.0,
                        ranges,
                        true,
                    )
                    .await?;
                    let column_chunks = merge_io_read_res.columns_chunks()?;
                    let pages = column_chunks
                        .get(&column_id)
                        .unwrap()
                        .as_raw_data()
                        .unwrap();
                    let pages = std::io::Cursor::new(pages);
                    let page_metas = column_meta.as_native().unwrap().pages.clone();
                    let reader = NativeReader::new(pages, page_metas, vec![]);
                    let this_pages_info = stat_simple(reader, arrow_field.clone())?.pages;
                    pages_info.extend(this_pages_info);
                }
            }
        }
        self.to_block(&pages_info).await
    }

    #[async_backtrace::framed]
    async fn to_block(&self, pages_info: &[PageInfo]) -> Result<DataBlock> {
        let num_row = pages_info.len();
        let mut validity_size = Vec::with_capacity(pages_info.len());
        let mut compressed_size = Vec::with_capacity(pages_info.len());
        let mut uncompressed_size = Vec::with_capacity(pages_info.len());
        let mut l1 = StringColumnBuilder::with_capacity(pages_info.len(), pages_info.len());
        let mut l2 = NullableColumnBuilder::<StringType>::with_capacity(pages_info.len(), &[]);
        for p in pages_info {
            validity_size.push(p.validity_size);
            compressed_size.push(p.compressed_size);
            uncompressed_size.push(p.uncompressed_size);
            l1.put_slice(encoding_to_string(&p.body).as_bytes());
            l1.commit_row();
            let l2_encoding = match &p.body {
                PageBody::Dict(dict) => Some(encoding_to_string(&dict.indices.body)),
                PageBody::Freq(freq) => freq
                    .exceptions
                    .as_ref()
                    .map(|e| encoding_to_string(&e.body)),
                _ => None,
            };
            if let Some(l2_encoding) = l2_encoding {
                l2.push(l2_encoding.as_bytes());
            } else {
                l2.push_null();
            }
        }
        Ok(DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt32))),
                    Value::Column(UInt32Type::from_opt_data(validity_size)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt32),
                    Value::Column(UInt32Type::from_data(compressed_size)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt32),
                    Value::Column(UInt32Type::from_data(uncompressed_size)),
                ),
                BlockEntry::new(DataType::String, Value::Column(Column::String(l1.build()))),
                BlockEntry::new(
                    DataType::Nullable(Box::new(DataType::String)),
                    Value::Column(Column::Nullable(Box::new(l2.build().upcast()))),
                ),
            ],
            num_row,
        ))
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new(
                "validity_size",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt32))),
            ),
            TableField::new(
                "compressed_size",
                TableDataType::Number(NumberDataType::UInt32),
            ),
            TableField::new(
                "uncompressed_size",
                TableDataType::Number(NumberDataType::UInt32),
            ),
            TableField::new("level_one", TableDataType::String),
            TableField::new(
                "level_two",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ])
    }
}

fn encoding_to_string(page_body: &PageBody) -> String {
    match page_body {
        PageBody::Dict(_) => "Dict".to_string(),
        PageBody::Freq(_) => "Freq".to_string(),
        PageBody::OneValue => "OneValue".to_string(),
        PageBody::Rle => "Rle".to_string(),
        PageBody::Patas => "Patas".to_string(),
        PageBody::Bitpack => "Bitpack".to_string(),
        PageBody::DeltaBitpack => "DeltaBitpack".to_string(),
        PageBody::Common(c) => format!("Common({:?})", c),
    }
}
