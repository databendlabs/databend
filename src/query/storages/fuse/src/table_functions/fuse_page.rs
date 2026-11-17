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

#[derive(Debug)]
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
    let total_len = column_bytes.len();
    let mut pos = 0usize;
    let mut pages = Vec::new();

    while pos < total_len {
        let header_offset = pos;
        let (header_len, header) = read_page_header_len(&column_bytes[pos..])?;
        let compressed_size =
            convert_i32_to_u64(header.compressed_page_size, "compressed_page_size")?;
        let uncompressed_size =
            convert_i32_to_u64(header.uncompressed_page_size, "uncompressed_page_size")?;
        let compressed_len = usize::try_from(compressed_size).map_err(|_| {
            ErrorCode::ParquetFileInvalid(format!(
                "invalid parquet column chunk for {}, compressed page size overflow",
                column_name
            ))
        })?;
        let data_end = pos
            .checked_add(header_len)
            .and_then(|end| end.checked_add(compressed_len))
            .ok_or_else(|| {
                ErrorCode::ParquetFileInvalid(format!(
                    "invalid parquet column chunk for {}, page data length overflow",
                    column_name
                ))
            })?;
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

        pos = data_end;

        let page_type_str = page_type_to_string(header.page_type);
        if page_type_str == "index_page" {
            continue;
        }

        let (encoding, num_values, num_rows) = page_header_fields(&header, column_name)?;
        let page_offset = column_offset
            .checked_add(header_offset as u64)
            .ok_or_else(|| {
                ErrorCode::ParquetFileInvalid(format!(
                    "invalid parquet column chunk for {}, page offset overflow",
                    column_name
                ))
            })?;
        pages.push(PageMetaRow {
            column_name,
            page_type: page_type_str,
            encoding,
            num_values,
            num_rows,
            compressed_size,
            uncompressed_size,
            header_size: header_len as u64,
            page_offset,
        });
    }

    Ok(pages)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DecodedDataPageHeader {
    num_values: i32,
    encoding: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DecodedDataPageHeaderV2 {
    num_values: i32,
    num_rows: i32,
    encoding: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DecodedDictionaryPageHeader {
    num_values: i32,
    encoding: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DecodedPageHeader {
    page_type: i32,
    uncompressed_page_size: i32,
    compressed_page_size: i32,
    data_page_header: Option<DecodedDataPageHeader>,
    dictionary_page_header: Option<DecodedDictionaryPageHeader>,
    data_page_header_v2: Option<DecodedDataPageHeaderV2>,
}

const MAX_VARINT32_BYTES: usize = 5;
const MAX_VARINT64_BYTES: usize = 10;
const MAX_THRIFT_SKIP_DEPTH: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompactFieldType {
    Stop,
    BooleanTrue,
    BooleanFalse,
    Byte,
    I16,
    I32,
    I64,
    Double,
    Binary,
    List,
    Set,
    Map,
    Struct,
    Uuid,
}

impl CompactFieldType {
    fn from_field_nibble(nibble: u8) -> Result<Self> {
        match nibble {
            0x00 => Ok(Self::Stop),
            0x01 => Ok(Self::BooleanTrue),
            0x02 => Ok(Self::BooleanFalse),
            0x03 => Ok(Self::Byte),
            0x04 => Ok(Self::I16),
            0x05 => Ok(Self::I32),
            0x06 => Ok(Self::I64),
            0x07 => Ok(Self::Double),
            0x08 => Ok(Self::Binary),
            0x09 => Ok(Self::List),
            0x0A => Ok(Self::Set),
            0x0B => Ok(Self::Map),
            0x0C => Ok(Self::Struct),
            0x0D => Ok(Self::Uuid),
            other => Err(invalid_page_header(format!(
                "unknown Thrift compact field type {other}"
            ))),
        }
    }

    fn is_bool(self) -> bool {
        matches!(self, Self::BooleanTrue | Self::BooleanFalse)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompactElementType {
    Bool,
    Byte,
    I16,
    I32,
    I64,
    Double,
    Binary,
    List,
    Set,
    Map,
    Struct,
    Uuid,
}

impl CompactElementType {
    fn from_nibble(nibble: u8) -> Result<Self> {
        match nibble {
            // Both 0x01 and 0x02 are accepted for collection booleans per the Thrift compact spec.
            0x01 | 0x02 => Ok(Self::Bool),
            0x03 => Ok(Self::Byte),
            0x04 => Ok(Self::I16),
            0x05 => Ok(Self::I32),
            0x06 => Ok(Self::I64),
            0x07 => Ok(Self::Double),
            0x08 => Ok(Self::Binary),
            0x09 => Ok(Self::List),
            0x0A => Ok(Self::Set),
            0x0B => Ok(Self::Map),
            0x0C => Ok(Self::Struct),
            0x0D => Ok(Self::Uuid),
            other => Err(invalid_page_header(format!(
                "unknown Thrift compact collection element type {other}"
            ))),
        }
    }

    fn min_wire_size(self) -> usize {
        match self {
            Self::Double => 8,
            Self::Uuid => 16,
            _ => 1,
        }
    }
}

/// Zero-allocation Thrift compact protocol decoder operating directly on an in-memory slice.
///
/// Binary and container lengths are validated against the remaining slice bytes before advancing,
/// avoiding eager heap allocations from untrusted length prefixes.
struct CompactSliceDecoder<'a> {
    buf: &'a [u8],
}

impl<'a> CompactSliceDecoder<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    fn remaining_len(&self) -> usize {
        self.buf.len()
    }

    fn read_byte(&mut self) -> Result<u8> {
        let (&b, rest) = self
            .buf
            .split_first()
            .ok_or_else(|| invalid_page_header("unexpected EOF"))?;
        self.buf = rest;
        Ok(b)
    }

    fn skip_bytes(&mut self, len: usize) -> Result<()> {
        if len > self.buf.len() {
            return Err(invalid_page_header(
                "unexpected EOF while skipping binary data",
            ));
        }
        self.buf = &self.buf[len..];
        Ok(())
    }

    fn read_varint32(&mut self) -> Result<u32> {
        let mut result = 0u32;
        let mut shift = 0u32;
        for _ in 0..MAX_VARINT32_BYTES {
            let b = self.read_byte()?;
            result |= u32::from(b & 0x7F) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
        Err(invalid_page_header("varint32 exceeds 5 bytes"))
    }

    fn skip_varint(&mut self, max_bytes: usize) -> Result<()> {
        for _ in 0..max_bytes {
            if self.read_byte()? & 0x80 == 0 {
                return Ok(());
            }
        }
        Err(invalid_page_header("varint exceeds maximum length"))
    }

    fn read_i32(&mut self) -> Result<i32> {
        let n = self.read_varint32()?;
        Ok(((n >> 1) as i32) ^ -(n as i32 & 1))
    }

    fn read_i16(&mut self) -> Result<i16> {
        let val = self.read_i32()?;
        i16::try_from(val).map_err(|_| invalid_page_header("i16 field value out of range"))
    }

    fn read_field_begin(&mut self, last_field_id: i16) -> Result<(CompactFieldType, i16)> {
        let header = self.read_byte()?;
        let field_delta = (header & 0xF0) >> 4;
        let field_type = CompactFieldType::from_field_nibble(header & 0x0F)?;
        if field_type == CompactFieldType::Stop {
            return Ok((CompactFieldType::Stop, 0));
        }

        let field_id = if field_delta != 0 {
            last_field_id
                .checked_add(i16::from(field_delta))
                .ok_or_else(|| invalid_page_header("field id delta overflow"))?
        } else {
            self.read_i16()?
        };
        Ok((field_type, field_id))
    }

    fn expect_field_type(actual: CompactFieldType, expected: CompactFieldType) -> Result<()> {
        if actual == expected {
            Ok(())
        } else {
            Err(invalid_page_header(format!(
                "unexpected Thrift field type {actual:?}, expected {expected:?}"
            )))
        }
    }

    fn read_i32_field(&mut self, field_type: CompactFieldType) -> Result<i32> {
        Self::expect_field_type(field_type, CompactFieldType::I32)?;
        self.read_i32()
    }

    fn expect_bool_field(field_type: CompactFieldType) -> Result<()> {
        if field_type.is_bool() {
            Ok(())
        } else {
            Err(invalid_page_header(format!(
                "unexpected Thrift field type {field_type:?}, expected Bool"
            )))
        }
    }

    fn read_collection_bool(&mut self) -> Result<()> {
        match self.read_byte()? {
            0x00..=0x02 => Ok(()),
            other => Err(invalid_page_header(format!(
                "invalid Thrift boolean value {other}"
            ))),
        }
    }

    fn skip_binary(&mut self) -> Result<()> {
        let len = usize::try_from(self.read_varint32()?)
            .map_err(|_| invalid_page_header("binary length overflow"))?;
        self.skip_bytes(len)
    }

    fn skip_struct(&mut self, depth: usize) -> Result<()> {
        if depth == 0 {
            return Err(invalid_page_header("Thrift recursion depth limit exceeded"));
        }
        let mut last_field_id = 0i16;
        loop {
            let (field_type, field_id) = self.read_field_begin(last_field_id)?;
            if field_type == CompactFieldType::Stop {
                return Ok(());
            }
            self.skip_field_value(field_type, depth - 1)?;
            last_field_id = field_id;
        }
    }

    fn skip_list_or_set(&mut self, depth: usize) -> Result<()> {
        if depth == 0 {
            return Err(invalid_page_header("Thrift recursion depth limit exceeded"));
        }
        let header = self.read_byte()?;
        // Some Parquet writers emit 0x00 for an empty list header.
        if header == 0 {
            return Ok(());
        }
        let elem_type = CompactElementType::from_nibble(header & 0x0F)?;
        let high = (header & 0xF0) >> 4;
        let count_i32 = if high != 15 {
            i32::from(high)
        } else {
            i32::try_from(self.read_varint32()?)
                .map_err(|_| invalid_page_header("negative or oversized list length"))?
        };
        let count =
            usize::try_from(count_i32).map_err(|_| invalid_page_header("negative list length"))?;
        let min_bytes = count
            .checked_mul(elem_type.min_wire_size())
            .ok_or_else(|| invalid_page_header("list byte size overflow"))?;
        if min_bytes > self.buf.len() {
            return Err(invalid_page_header(
                "list length exceeds remaining page bytes",
            ));
        }
        for _ in 0..count {
            self.skip_element_value(elem_type, depth - 1)?;
        }
        Ok(())
    }

    fn skip_map(&mut self, depth: usize) -> Result<()> {
        if depth == 0 {
            return Err(invalid_page_header("Thrift recursion depth limit exceeded"));
        }
        let count_i32 = i32::try_from(self.read_varint32()?)
            .map_err(|_| invalid_page_header("negative or oversized map length"))?;
        let count =
            usize::try_from(count_i32).map_err(|_| invalid_page_header("negative map length"))?;
        if count == 0 {
            return Ok(());
        }
        let type_header = self.read_byte()?;
        let key_type = CompactElementType::from_nibble((type_header & 0xF0) >> 4)?;
        let val_type = CompactElementType::from_nibble(type_header & 0x0F)?;
        let entry_min_size = key_type.min_wire_size() + val_type.min_wire_size();
        let min_bytes = count
            .checked_mul(entry_min_size)
            .ok_or_else(|| invalid_page_header("map byte size overflow"))?;
        if min_bytes > self.buf.len() {
            return Err(invalid_page_header(
                "map length exceeds remaining page bytes",
            ));
        }
        for _ in 0..count {
            self.skip_element_value(key_type, depth - 1)?;
            self.skip_element_value(val_type, depth - 1)?;
        }
        Ok(())
    }

    fn skip_field_value(&mut self, field_type: CompactFieldType, depth: usize) -> Result<()> {
        match field_type {
            CompactFieldType::Stop => Ok(()),
            // Struct boolean fields carry their value in the field header nibble.
            CompactFieldType::BooleanTrue | CompactFieldType::BooleanFalse => Ok(()),
            CompactFieldType::Byte => self.skip_bytes(1),
            CompactFieldType::I16 | CompactFieldType::I32 => self.skip_varint(MAX_VARINT32_BYTES),
            CompactFieldType::I64 => self.skip_varint(MAX_VARINT64_BYTES),
            CompactFieldType::Double => self.skip_bytes(8),
            CompactFieldType::Binary => self.skip_binary(),
            CompactFieldType::List | CompactFieldType::Set => self.skip_list_or_set(depth),
            CompactFieldType::Map => self.skip_map(depth),
            CompactFieldType::Struct => self.skip_struct(depth),
            CompactFieldType::Uuid => self.skip_bytes(16),
        }
    }

    fn skip_element_value(&mut self, elem_type: CompactElementType, depth: usize) -> Result<()> {
        match elem_type {
            CompactElementType::Bool => self.read_collection_bool(),
            CompactElementType::Byte => self.skip_bytes(1),
            CompactElementType::I16 | CompactElementType::I32 => {
                self.skip_varint(MAX_VARINT32_BYTES)
            }
            CompactElementType::I64 => self.skip_varint(MAX_VARINT64_BYTES),
            CompactElementType::Double => self.skip_bytes(8),
            CompactElementType::Binary => self.skip_binary(),
            CompactElementType::List | CompactElementType::Set => self.skip_list_or_set(depth),
            CompactElementType::Map => self.skip_map(depth),
            CompactElementType::Struct => self.skip_struct(depth),
            CompactElementType::Uuid => self.skip_bytes(16),
        }
    }
}

fn invalid_page_header(msg: impl Into<String>) -> ErrorCode {
    ErrorCode::ParquetFileInvalid(format!("invalid parquet page header: {}", msg.into()))
}

fn required_field<T>(val: Option<T>, name: &'static str) -> Result<T> {
    val.ok_or_else(|| invalid_page_header(format!("missing required field {name}")))
}

fn decode_data_page_header(dec: &mut CompactSliceDecoder<'_>) -> Result<DecodedDataPageHeader> {
    let mut num_values = None;
    let mut encoding = None;
    let mut def_level_encoding = None;
    let mut rep_level_encoding = None;
    let mut last_field_id = 0i16;

    loop {
        let (field_type, field_id) = dec.read_field_begin(last_field_id)?;
        if field_type == CompactFieldType::Stop {
            break;
        }
        match field_id {
            1 => num_values = Some(dec.read_i32_field(field_type)?),
            2 => encoding = Some(dec.read_i32_field(field_type)?),
            3 => def_level_encoding = Some(dec.read_i32_field(field_type)?),
            4 => rep_level_encoding = Some(dec.read_i32_field(field_type)?),
            _ => dec.skip_field_value(field_type, MAX_THRIFT_SKIP_DEPTH - 2)?,
        }
        last_field_id = field_id;
    }

    let _ = required_field(
        def_level_encoding,
        "DataPageHeader.definition_level_encoding",
    )?;
    let _ = required_field(
        rep_level_encoding,
        "DataPageHeader.repetition_level_encoding",
    )?;
    Ok(DecodedDataPageHeader {
        num_values: required_field(num_values, "DataPageHeader.num_values")?,
        encoding: required_field(encoding, "DataPageHeader.encoding")?,
    })
}

fn decode_dictionary_page_header(
    dec: &mut CompactSliceDecoder<'_>,
) -> Result<DecodedDictionaryPageHeader> {
    let mut num_values = None;
    let mut encoding = None;
    let mut last_field_id = 0i16;

    loop {
        let (field_type, field_id) = dec.read_field_begin(last_field_id)?;
        if field_type == CompactFieldType::Stop {
            break;
        }
        match field_id {
            1 => num_values = Some(dec.read_i32_field(field_type)?),
            2 => encoding = Some(dec.read_i32_field(field_type)?),
            3 => CompactSliceDecoder::expect_bool_field(field_type)?,
            _ => dec.skip_field_value(field_type, MAX_THRIFT_SKIP_DEPTH - 2)?,
        }
        last_field_id = field_id;
    }

    Ok(DecodedDictionaryPageHeader {
        num_values: required_field(num_values, "DictionaryPageHeader.num_values")?,
        encoding: required_field(encoding, "DictionaryPageHeader.encoding")?,
    })
}

fn decode_data_page_header_v2(
    dec: &mut CompactSliceDecoder<'_>,
) -> Result<DecodedDataPageHeaderV2> {
    let mut num_values = None;
    let mut num_nulls = None;
    let mut num_rows = None;
    let mut encoding = None;
    let mut def_levels_byte_len = None;
    let mut rep_levels_byte_len = None;
    let mut last_field_id = 0i16;

    loop {
        let (field_type, field_id) = dec.read_field_begin(last_field_id)?;
        if field_type == CompactFieldType::Stop {
            break;
        }
        match field_id {
            1 => num_values = Some(dec.read_i32_field(field_type)?),
            2 => num_nulls = Some(dec.read_i32_field(field_type)?),
            3 => num_rows = Some(dec.read_i32_field(field_type)?),
            4 => encoding = Some(dec.read_i32_field(field_type)?),
            5 => def_levels_byte_len = Some(dec.read_i32_field(field_type)?),
            6 => rep_levels_byte_len = Some(dec.read_i32_field(field_type)?),
            7 => CompactSliceDecoder::expect_bool_field(field_type)?,
            _ => dec.skip_field_value(field_type, MAX_THRIFT_SKIP_DEPTH - 2)?,
        }
        last_field_id = field_id;
    }

    let _ = required_field(num_nulls, "DataPageHeaderV2.num_nulls")?;
    let _ = required_field(
        def_levels_byte_len,
        "DataPageHeaderV2.definition_levels_byte_length",
    )?;
    let _ = required_field(
        rep_levels_byte_len,
        "DataPageHeaderV2.repetition_levels_byte_length",
    )?;
    Ok(DecodedDataPageHeaderV2 {
        num_values: required_field(num_values, "DataPageHeaderV2.num_values")?,
        num_rows: required_field(num_rows, "DataPageHeaderV2.num_rows")?,
        encoding: required_field(encoding, "DataPageHeaderV2.encoding")?,
    })
}

fn read_page_header_len(input: &[u8]) -> Result<(usize, DecodedPageHeader)> {
    let initial_len = input.len();
    let mut dec = CompactSliceDecoder::new(input);

    let mut page_type = None;
    let mut uncompressed_page_size = None;
    let mut compressed_page_size = None;
    let mut data_page_header = None;
    let mut dictionary_page_header = None;
    let mut data_page_header_v2 = None;
    let mut last_field_id = 0i16;

    loop {
        let (field_type, field_id) = dec.read_field_begin(last_field_id)?;
        if field_type == CompactFieldType::Stop {
            break;
        }
        match field_id {
            1 => page_type = Some(dec.read_i32_field(field_type)?),
            2 => uncompressed_page_size = Some(dec.read_i32_field(field_type)?),
            3 => compressed_page_size = Some(dec.read_i32_field(field_type)?),
            4 => {
                let _ = dec.read_i32_field(field_type)?;
            }
            5 => {
                CompactSliceDecoder::expect_field_type(field_type, CompactFieldType::Struct)?;
                data_page_header = Some(decode_data_page_header(&mut dec)?);
            }
            6 => {
                CompactSliceDecoder::expect_field_type(field_type, CompactFieldType::Struct)?;
                dec.skip_struct(MAX_THRIFT_SKIP_DEPTH - 1)?;
            }
            7 => {
                CompactSliceDecoder::expect_field_type(field_type, CompactFieldType::Struct)?;
                dictionary_page_header = Some(decode_dictionary_page_header(&mut dec)?);
            }
            8 => {
                CompactSliceDecoder::expect_field_type(field_type, CompactFieldType::Struct)?;
                data_page_header_v2 = Some(decode_data_page_header_v2(&mut dec)?);
            }
            _ => dec.skip_field_value(field_type, MAX_THRIFT_SKIP_DEPTH - 1)?,
        }
        last_field_id = field_id;
    }

    let header = DecodedPageHeader {
        page_type: required_field(page_type, "PageHeader.type")?,
        uncompressed_page_size: required_field(
            uncompressed_page_size,
            "PageHeader.uncompressed_page_size",
        )?,
        compressed_page_size: required_field(
            compressed_page_size,
            "PageHeader.compressed_page_size",
        )?,
        data_page_header,
        dictionary_page_header,
        data_page_header_v2,
    };
    Ok((initial_len - dec.remaining_len(), header))
}

const PAGE_TYPE_DATA_PAGE: i32 = 0;
const PAGE_TYPE_INDEX_PAGE: i32 = 1;
const PAGE_TYPE_DICTIONARY_PAGE: i32 = 2;
const PAGE_TYPE_DATA_PAGE_V2: i32 = 3;

fn page_type_to_string(page_type: i32) -> &'static str {
    match page_type {
        PAGE_TYPE_DATA_PAGE => "data_page_v1",
        PAGE_TYPE_DATA_PAGE_V2 => "data_page_v2",
        PAGE_TYPE_DICTIONARY_PAGE => "dictionary",
        PAGE_TYPE_INDEX_PAGE => "index_page",
        _ => "unknown",
    }
}

fn page_header_fields(
    header: &DecodedPageHeader,
    column_name: &str,
) -> Result<(&'static str, u64, Option<u64>)> {
    match header.page_type {
        PAGE_TYPE_DATA_PAGE => {
            let data_page = header.data_page_header.as_ref().ok_or_else(|| {
                ErrorCode::ParquetFileInvalid(format!(
                    "invalid parquet column chunk for {}, missing data_page_header",
                    column_name
                ))
            })?;
            let num_values = convert_i32_to_u64(data_page.num_values, "num_values")?;
            Ok((
                encoding_to_string(data_page.encoding),
                num_values,
                Some(num_values),
            ))
        }
        PAGE_TYPE_DATA_PAGE_V2 => {
            let data_page = header.data_page_header_v2.as_ref().ok_or_else(|| {
                ErrorCode::ParquetFileInvalid(format!(
                    "invalid parquet column chunk for {}, missing data_page_header_v2",
                    column_name
                ))
            })?;
            let num_values = convert_i32_to_u64(data_page.num_values, "num_values")?;
            let num_rows = convert_i32_to_u64(data_page.num_rows, "num_rows")?;
            Ok((
                encoding_to_string(data_page.encoding),
                num_values,
                Some(num_rows),
            ))
        }
        PAGE_TYPE_DICTIONARY_PAGE => {
            let dict_page = header.dictionary_page_header.as_ref().ok_or_else(|| {
                ErrorCode::ParquetFileInvalid(format!(
                    "invalid parquet column chunk for {}, missing dictionary_page_header",
                    column_name
                ))
            })?;
            let num_values = convert_i32_to_u64(dict_page.num_values, "num_values")?;
            Ok((encoding_to_string(dict_page.encoding), num_values, None))
        }
        PAGE_TYPE_INDEX_PAGE => Ok(("unknown", 0, None)),
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

#[cfg(test)]
mod tests {
    use arrow_array::Int32Array;
    use arrow_array::RecordBatch;
    use arrow_array::StringArray;
    use arrow_schema::DataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::ParquetMetaDataReader;
    use parquet::file::properties::EnabledStatistics;
    use parquet::file::properties::WriterProperties;
    use parquet::file::properties::WriterVersion;

    use super::*;

    fn write_single_column_parquet(batch: &RecordBatch, props: WriterProperties) -> Vec<u8> {
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), Some(props)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        out
    }

    #[test]
    fn test_parse_arrow_writer_v1_and_v2_pages() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let values = StringArray::from(vec!["alpha", "alpha", "beta", "beta", "gamma"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap();

        let props_v1 = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_dictionary_enabled(true)
            .set_data_page_row_count_limit(2)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let parquet_v1 = write_single_column_parquet(&batch, props_v1);
        let meta_v1 = ParquetMetaDataReader::new()
            .parse_and_finish(&bytes::Bytes::from(parquet_v1.clone()))
            .unwrap();
        let (col_offset, col_len) = meta_v1.row_group(0).column(0).byte_range();
        let chunk_v1 = &parquet_v1[col_offset as usize..(col_offset + col_len) as usize];

        let pages_v1 = parse_page_headers("s", col_offset, chunk_v1).unwrap();
        assert!(pages_v1.len() >= 2);
        assert_eq!(pages_v1[0].page_type, "dictionary");
        assert_eq!(pages_v1[0].num_values, 3);
        assert_eq!(pages_v1[0].num_rows, None);
        assert_eq!(pages_v1[0].page_offset, col_offset);

        let data_v1_values: u64 = pages_v1[1..]
            .iter()
            .map(|p| {
                assert_eq!(p.page_type, "data_page_v1");
                assert_eq!(p.num_rows, Some(p.num_values));
                p.num_values
            })
            .sum();
        assert_eq!(data_v1_values, 5);
        let spanned_v1: u64 = pages_v1
            .iter()
            .map(|p| p.header_size + p.compressed_size)
            .sum();
        assert_eq!(spanned_v1, col_len);

        let schema_v2 = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let nums = Int32Array::from(vec![10, 20, 30, 40, 50]);
        let batch_v2 = RecordBatch::try_new(schema_v2, vec![Arc::new(nums)]).unwrap();
        let props_v2 = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(false)
            .set_data_page_row_count_limit(2)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let parquet_v2 = write_single_column_parquet(&batch_v2, props_v2);
        let meta_v2 = ParquetMetaDataReader::new()
            .parse_and_finish(&bytes::Bytes::from(parquet_v2.clone()))
            .unwrap();
        let (col_offset_v2, col_len_v2) = meta_v2.row_group(0).column(0).byte_range();
        let chunk_v2 = &parquet_v2[col_offset_v2 as usize..(col_offset_v2 + col_len_v2) as usize];

        let pages_v2 = parse_page_headers("n", col_offset_v2, chunk_v2).unwrap();
        assert!(!pages_v2.is_empty());
        let mut expected_offset = col_offset_v2;
        let mut total_rows = 0u64;
        for page in &pages_v2 {
            assert_eq!(page.page_type, "data_page_v2");
            assert_eq!(page.page_offset, expected_offset);
            total_rows += page.num_rows.unwrap();
            expected_offset += page.header_size + page.compressed_size;
        }
        assert_eq!(total_rows, 5);
        assert_eq!(expected_offset, col_offset_v2 + col_len_v2);
    }

    #[test]
    fn test_rejects_oversized_binary_length_without_eager_alloc() {
        // PageHeader(DATA_PAGE, uncompressed=0, compressed=0) with DataPageHeader.statistics.max
        // declaring u32::MAX (4 GiB) bytes while no payload follows.
        let crafted = [
            0x15, 0x00, // 1: type = DATA_PAGE (0)
            0x15, 0x00, // 2: uncompressed_page_size = 0
            0x15, 0x00, // 3: compressed_page_size = 0
            0x2C, // 5: data_page_header (struct, delta = 2)
            0x15, 0x02, //   1: num_values = 1
            0x15, 0x00, //   2: encoding = PLAIN
            0x15, 0x00, //   3: def_level_encoding = PLAIN
            0x15, 0x00, //   4: rep_level_encoding = PLAIN
            0x1C, //   5: statistics (struct, delta = 1)
            0x18, //     1: max (binary, delta = 1)
            0xFF, 0xFF, 0xFF, 0xFF, 0x0F, // varint32 = u32::MAX
            0x00, 0x00, 0x00,
        ];

        let err = parse_page_headers("c", 0, &crafted).unwrap_err();
        assert_eq!(err.code(), ErrorCode::ParquetFileInvalid("").code());
    }

    #[test]
    fn test_rejects_oversized_containers_deep_nesting_and_bad_types() {
        // Unknown list<byte> field (id = 9, delta = 6 from field 3 -> 0x69) with count = 0x7FFFFFFF.
        let oversized_list = [
            0x15, 0x00, // 1: type = 0
            0x15, 0x00, // 2: uncompressed_page_size = 0
            0x15, 0x00, // 3: compressed_page_size = 0
            0x69, // 9: list (delta = 6)
            0xF3, // count = varint, elem = byte (0x03)
            0xFF, 0xFF, 0xFF, 0xFF, 0x07, // i32::MAX elements
            0x00,
        ];
        assert!(parse_page_headers("c", 0, &oversized_list).is_err());

        // Unknown map<binary, binary> field (0x6B) with negative i32 count (u32::MAX).
        let negative_map = [
            0x15, 0x00, 0x15, 0x00, 0x15, 0x00, 0x6B, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F, 0x88, 0x00,
        ];
        assert!(parse_page_headers("c", 0, &negative_map).is_err());

        // Deeply nested unknown structs exceeding MAX_THRIFT_SKIP_DEPTH.
        let mut deep_structs = vec![0x15, 0x00, 0x15, 0x00, 0x15, 0x00];
        deep_structs.extend(std::iter::repeat_n(0x6C, MAX_THRIFT_SKIP_DEPTH + 4));
        deep_structs.extend(std::iter::repeat_n(0x00, MAX_THRIFT_SKIP_DEPTH + 5));
        let err = parse_page_headers("c", 0, &deep_structs).unwrap_err();
        assert!(err.message().contains("recursion depth"));

        // DictionaryPageHeader.is_sorted (field 3) sent as I32 (0x15) instead of Bool.
        let bad_bool_type = [
            0x15, 0x04, // 1: type = DICTIONARY_PAGE (2 -> zigzag 4)
            0x15, 0x00, // 2: uncompressed = 0
            0x15, 0x00, // 3: compressed = 0
            0x4C, // 7: dictionary_page_header (delta = 4)
            0x15, 0x02, //   1: num_values = 1
            0x15, 0x00, //   2: encoding = 0
            0x15, 0x00, //   3: is_sorted wrongly encoded as I32
            0x00, 0x00,
        ];
        assert!(parse_page_headers("c", 0, &bad_bool_type).is_err());
    }

    #[test]
    fn test_skips_index_page_and_tracks_following_offset() {
        let mut bytes = vec![
            // Page 0: INDEX_PAGE (type = 1 -> zigzag 2), compressed_size = 3 bytes
            0x15, 0x02, // 1: type = INDEX_PAGE
            0x15, 0x06, // 2: uncompressed = 3
            0x15, 0x06, // 3: compressed = 3
            0x3C, 0x00, // 6: index_page_header = {}
            0x00, // end PageHeader (9 bytes)
            0xAA, 0xBB, 0xCC, // 3 bytes index payload
        ];
        let second_page_offset = bytes.len() as u64;
        bytes.extend_from_slice(&[
            // Page 1: DATA_PAGE (type = 0), compressed_size = 2 bytes
            0x15, 0x00, // 1: type = DATA_PAGE
            0x15, 0x04, // 2: uncompressed = 2
            0x15, 0x04, // 3: compressed = 2
            0x2C, // 5: data_page_header
            0x15, 0x08, //   1: num_values = 4
            0x15, 0x00, //   2: encoding = PLAIN
            0x15, 0x06, //   3: def_level_encoding = RLE
            0x15, 0x06, //   4: rep_level_encoding = RLE
            0x00, // end DataPageHeader
            0x00, // end PageHeader
            0x11, 0x22, // 2 bytes data payload
        ]);

        let pages = parse_page_headers("col", 100, &bytes).unwrap();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].page_type, "data_page_v1");
        assert_eq!(pages[0].encoding, "plain");
        assert_eq!(pages[0].num_values, 4);
        assert_eq!(pages[0].num_rows, Some(4));
        assert_eq!(pages[0].compressed_size, 2);
        assert_eq!(pages[0].page_offset, 100 + second_page_offset);
    }
}
