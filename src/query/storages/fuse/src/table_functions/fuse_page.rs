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

use std::io::Read;
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
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use opendal::Operator;
use parquet::format::PageHeader;
use parquet::format::PageType;
use parquet::thrift::TSerializable;
use thrift::protocol::TCompactInputProtocol;

use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::TableMetaFuncTemplate;
use crate::table_functions::function_template::TableMetaFunc;

pub struct FusePage;
pub type FusePageFunc = TableMetaFuncTemplate<FusePage>;

struct PageMetaRow<'a> {
    column_name: &'a str,
    page_type: &'static str,
    encoding: &'static str,
    num_values: u64,
    num_rows: Option<u64>,
    compressed_size: u64,
    uncompressed_size: u64,
    header_size: u64,
    page_offset: u64,
}

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
                .read_segments::<SegmentInfo>(chunk, true)
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
                        for (page_ordinal_in_column, page) in parse_page_headers(
                            field.name(),
                            parquet_meta.offset,
                            column_bytes.as_slice(),
                        )?
                        .into_iter()
                        .enumerate()
                        {
                            block_location.put_and_commit(&block.location.0);
                            column_name.put_and_commit(page.column_name);
                            page_type.put_and_commit(page.page_type);
                            encoding.put_and_commit(page.encoding);
                            page_ordinal.push(page_ordinal_in_column as u64);
                            num_values.push(page.num_values);
                            num_rows.push(page.num_rows);
                            compressed_size.push(page.compressed_size);
                            uncompressed_size.push(page.uncompressed_size);
                            header_size.push(page.header_size);
                            page_offset.push(page.page_offset);

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
) -> Result<Vec<u8>> {
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
    Ok(buffer.to_vec())
}

fn parse_page_headers<'a>(
    column_name: &'a str,
    column_offset: u64,
    column_bytes: &'a [u8],
) -> Result<Vec<PageMetaRow<'a>>> {
    let mut cursor = std::io::Cursor::new(column_bytes);
    let total_len = column_bytes.len() as u64;
    let mut pages = Vec::new();

    while cursor.position() < total_len {
        let header_offset = cursor.position();
        let (header_len, header) = read_page_header_len(&mut cursor)?;
        let compressed_size =
            convert_i32_to_u64(header.compressed_page_size, "compressed_page_size")?;
        let uncompressed_size =
            convert_i32_to_u64(header.uncompressed_page_size, "uncompressed_page_size")?;
        let data_end = cursor.position() + compressed_size;
        if data_end > total_len {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "invalid parquet column chunk for {}, page data exceeds chunk size",
                column_name
            )));
        }
        if header_len == 0 && compressed_size == 0 {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "invalid parquet column chunk for {}, empty page header",
                column_name
            )));
        }

        cursor.set_position(data_end);

        let page_type_str = page_type_to_string(header.type_);
        if page_type_str == "index_page" {
            continue;
        }

        let (encoding, num_values, num_rows) = page_header_fields(&header, column_name)?;
        pages.push(PageMetaRow {
            column_name,
            page_type: page_type_str,
            encoding,
            num_values,
            num_rows,
            compressed_size,
            uncompressed_size,
            header_size: header_len as u64,
            page_offset: column_offset + header_offset,
        });
    }

    Ok(pages)
}

fn read_page_header_len<R: Read>(input: &mut R) -> Result<(usize, PageHeader)> {
    struct TrackedRead<R>(R, usize);

    impl<R: Read> Read for TrackedRead<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let read = self.0.read(buf)?;
            self.1 += read;
            Ok(read)
        }
    }

    let mut tracked = TrackedRead(input, 0);
    let mut prot = TCompactInputProtocol::new(&mut tracked);
    let header = PageHeader::read_from_in_protocol(&mut prot)
        .map_err(|err| ErrorCode::ParquetFileInvalid(err.to_string()))?;
    Ok((tracked.1, header))
}

fn page_type_to_string(page_type: PageType) -> &'static str {
    match page_type {
        PageType::DATA_PAGE => "data_page_v1",
        PageType::DATA_PAGE_V2 => "data_page_v2",
        PageType::DICTIONARY_PAGE => "dictionary",
        PageType::INDEX_PAGE => "index_page",
        _ => "unknown",
    }
}

fn page_header_fields(
    header: &PageHeader,
    column_name: &str,
) -> Result<(&'static str, u64, Option<u64>)> {
    match header.type_ {
        PageType::DATA_PAGE => {
            let data_page = header.data_page_header.as_ref().ok_or_else(|| {
                ErrorCode::ParquetFileInvalid(format!(
                    "invalid parquet column chunk for {}, missing data_page_header",
                    column_name
                ))
            })?;
            let num_values = convert_i32_to_u64(data_page.num_values, "num_values")?;
            Ok((
                encoding_to_string(data_page.encoding.0),
                num_values,
                Some(num_values),
            ))
        }
        PageType::DATA_PAGE_V2 => {
            let data_page = header.data_page_header_v2.as_ref().ok_or_else(|| {
                ErrorCode::ParquetFileInvalid(format!(
                    "invalid parquet column chunk for {}, missing data_page_header_v2",
                    column_name
                ))
            })?;
            let num_values = convert_i32_to_u64(data_page.num_values, "num_values")?;
            let num_rows = convert_i32_to_u64(data_page.num_rows, "num_rows")?;
            Ok((
                encoding_to_string(data_page.encoding.0),
                num_values,
                Some(num_rows),
            ))
        }
        PageType::DICTIONARY_PAGE => {
            let dict_page = header.dictionary_page_header.as_ref().ok_or_else(|| {
                ErrorCode::ParquetFileInvalid(format!(
                    "invalid parquet column chunk for {}, missing dictionary_page_header",
                    column_name
                ))
            })?;
            let num_values = convert_i32_to_u64(dict_page.num_values, "num_values")?;
            Ok((encoding_to_string(dict_page.encoding.0), num_values, None))
        }
        PageType::INDEX_PAGE => Ok(("unknown", 0, None)),
        _ => Ok(("unknown", 0, None)),
    }
}

fn encoding_to_string(encoding: i32) -> &'static str {
    match encoding {
        0 => "plain",
        2 => "plain_dictionary",
        3 => "rle",
        4 => "bit_packed",
        5 => "delta_binary_packed",
        6 => "delta_length_byte_array",
        7 => "delta_byte_array",
        8 => "rle_dictionary",
        9 => "byte_stream_split",
        _ => "unknown",
    }
}

fn convert_i32_to_u64(value: i32, field: &str) -> Result<u64> {
    if value < 0 {
        return Err(ErrorCode::ParquetFileInvalid(format!(
            "invalid parquet page header: {} is negative",
            field
        )));
    }
    Ok(value as u64)
}
