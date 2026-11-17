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

use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_storages_common_blocks::iter_parquet_page_headers;
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use opendal::Operator;

use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::TableMetaFuncTemplate;
use crate::table_functions::function_template::TableMetaFunc;

pub struct FusePage;
pub type FusePageFunc = TableMetaFuncTemplate<FusePage>;

#[async_trait::async_trait]
impl TableMetaFunc for FusePage {
    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("snapshot_id", TableDataType::String),
            TableField::new("timestamp", TableDataType::Timestamp),
            TableField::new("block_location", TableDataType::String),
            TableField::new("column_name", TableDataType::String),
            TableField::new(
                "page_ordinal",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("page_type", TableDataType::String),
            TableField::new("encoding", TableDataType::String),
            TableField::new("num_values", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "num_rows",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "compressed_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "uncompressed_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("header_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("page_offset", TableDataType::Number(NumberDataType::UInt64)),
        ])
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        if !matches!(tbl.storage_format, FuseStorageFormat::Parquet) {
            return Err(ErrorCode::Unimplemented(
                "fuse_page only supports Parquet storage format",
            ));
        }

        let limit = limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(DataBlock::empty_with_schema(&Self::schema().into()));
        }

        let snapshot_id = snapshot.snapshot_id.simple().to_string();
        let timestamp = snapshot.timestamp.unwrap_or_default().timestamp_micros();
        let mut block_location = StringColumnBuilder::with_capacity(0);
        let mut column_name = StringColumnBuilder::with_capacity(0);
        let mut page_type = StringColumnBuilder::with_capacity(0);
        let mut encoding = StringColumnBuilder::with_capacity(0);
        let mut page_ordinal = Vec::new();
        let mut num_values = Vec::new();
        let mut num_rows = Vec::new();
        let mut compressed_size = Vec::new();
        let mut uncompressed_size = Vec::new();
        let mut header_size = Vec::new();
        let mut page_offset = Vec::new();

        let schema = tbl.schema();
        let segments_io = SegmentsIO::create(ctx.clone(), tbl.operator.clone(), schema.clone());
        let mut rows = 0usize;

        let chunk_size = std::cmp::min(
            ctx.get_settings().get_max_threads()? as usize * 4,
            snapshot.segments.len(),
        )
        .max(1);

        'FOR: for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, CachePolicy::DEFAULT)
                .await?;
            for segment in segments {
                let segment = segment?;
                for block in segment.blocks.iter() {
                    let block = block.as_ref();
                    for field in schema.fields().iter() {
                        if field.is_nested() {
                            continue;
                        }
                        let column_id = field.column_id;
                        let Some(column_meta) = block.col_metas.get(&column_id) else {
                            continue;
                        };
                        let Some(parquet_meta) = column_meta.as_parquet() else {
                            continue;
                        };

                        let column_bytes = read_parquet_column_chunk(
                            &tbl.operator,
                            &block.location.0,
                            parquet_meta.offset,
                            parquet_meta.len,
                        )
                        .await?;
                        let mut page_ordinal_in_column = 0u64;
                        for page in iter_parquet_page_headers(
                            field.name(),
                            parquet_meta.offset,
                            column_bytes.as_ref(),
                        ) {
                            let page = page?;
                            if page.is_index_page() {
                                continue;
                            }
                            block_location.put_and_commit(&block.location.0);
                            column_name.put_and_commit(field.name());
                            page_type.put_and_commit(page.page_type_name());
                            encoding.put_and_commit(page.encoding_name());
                            page_ordinal.push(page_ordinal_in_column);
                            num_values.push(page.num_values());
                            num_rows.push(page.num_rows());
                            compressed_size.push(page.compressed_size);
                            uncompressed_size.push(page.uncompressed_size);
                            header_size.push(page.header_size);
                            page_offset.push(page.page_offset);

                            page_ordinal_in_column += 1;
                            rows += 1;
                            if rows >= limit {
                                break 'FOR;
                            }
                        }
                    }
                }
            }
        }

        Ok(DataBlock::new(
            vec![
                BlockEntry::new_const_column_arg::<StringType>(snapshot_id, rows),
                BlockEntry::new_const_column_arg::<TimestampType>(timestamp, rows),
                Column::String(block_location.build()).into(),
                Column::String(column_name.build()).into(),
                UInt64Type::from_data(page_ordinal).into(),
                Column::String(page_type.build()).into(),
                Column::String(encoding.build()).into(),
                UInt64Type::from_data(num_values).into(),
                UInt64Type::from_opt_data(num_rows).into(),
                UInt64Type::from_data(compressed_size).into(),
                UInt64Type::from_data(uncompressed_size).into(),
                UInt64Type::from_data(header_size).into(),
                UInt64Type::from_data(page_offset).into(),
            ],
            rows,
        ))
    }
}

async fn read_parquet_column_chunk(
    operator: &Operator,
    location: &str,
    offset: u64,
    len: u64,
) -> Result<bytes::Bytes> {
    let end = offset.checked_add(len).ok_or_else(|| {
        ErrorCode::ParquetFileInvalid(format!(
            "invalid parquet file {}, column chunk length overflow",
            location
        ))
    })?;
    let buffer = operator
        .read_with(location)
        .range(offset..end)
        .await
        .map_err(|err| ErrorCode::StorageOther(err.to_string()))?;
    Ok(buffer.to_bytes())
}
