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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::thrift_compact::CompactType;
use crate::thrift_compact::ThriftCompactReader;

const PAGE_TYPE_DATA_PAGE: i32 = 0;
const PAGE_TYPE_INDEX_PAGE: i32 = 1;
const PAGE_TYPE_DICTIONARY_PAGE: i32 = 2;
const PAGE_TYPE_DATA_PAGE_V2: i32 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParquetPageKind {
    DataPageV1 {
        num_values: u64,
        encoding: i32,
    },
    DataPageV2 {
        num_values: u64,
        num_rows: u64,
        encoding: i32,
    },
    DictionaryPage {
        num_values: u64,
        encoding: i32,
    },
    IndexPage,
    Unknown(i32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParquetPageHeader {
    pub kind: ParquetPageKind,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub header_size: u64,
    pub page_offset: u64,
}

impl ParquetPageHeader {
    pub fn is_index_page(&self) -> bool {
        matches!(self.kind, ParquetPageKind::IndexPage)
    }

    pub fn page_type_name(&self) -> &'static str {
        match self.kind {
            ParquetPageKind::DataPageV1 { .. } => "data_page_v1",
            ParquetPageKind::DataPageV2 { .. } => "data_page_v2",
            ParquetPageKind::DictionaryPage { .. } => "dictionary",
            ParquetPageKind::IndexPage => "index_page",
            ParquetPageKind::Unknown(_) => "unknown",
        }
    }

    pub fn encoding_name(&self) -> &'static str {
        match self.kind {
            ParquetPageKind::DataPageV1 { encoding, .. }
            | ParquetPageKind::DataPageV2 { encoding, .. }
            | ParquetPageKind::DictionaryPage { encoding, .. } => parquet_encoding_name(encoding),
            ParquetPageKind::IndexPage | ParquetPageKind::Unknown(_) => "unknown",
        }
    }

    pub fn num_values(&self) -> u64 {
        match self.kind {
            ParquetPageKind::DataPageV1 { num_values, .. }
            | ParquetPageKind::DataPageV2 { num_values, .. }
            | ParquetPageKind::DictionaryPage { num_values, .. } => num_values,
            ParquetPageKind::IndexPage | ParquetPageKind::Unknown(_) => 0,
        }
    }

    pub fn num_rows(&self) -> Option<u64> {
        match self.kind {
            ParquetPageKind::DataPageV1 { num_values, .. } => Some(num_values),
            ParquetPageKind::DataPageV2 { num_rows, .. } => Some(num_rows),
            ParquetPageKind::DictionaryPage { .. }
            | ParquetPageKind::IndexPage
            | ParquetPageKind::Unknown(_) => None,
        }
    }
}

pub fn parquet_encoding_name(encoding: i32) -> &'static str {
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

/// Decodes a single Parquet `PageHeader` from the start of `input` without allocating.
pub fn read_parquet_page_header(input: &[u8], page_offset: u64) -> Result<ParquetPageHeader> {
    let mut reader = ThriftCompactReader::new(input);
    let mut page_type = None;
    let mut uncompressed_size = None;
    let mut compressed_size = None;
    let mut data_v1 = None;
    let mut dict = None;
    let mut data_v2 = None;

    reader.read_root_struct(|r, field_id, ty| {
        match field_id {
            1 => page_type = Some(r.read_i32(ty)?),
            2 => uncompressed_size = Some(r.read_i32(ty)?),
            3 => compressed_size = Some(r.read_i32(ty)?),
            4 => {
                let _ = r.read_i32(ty)?;
            }
            5 => data_v1 = Some(read_data_page_v1(r, ty)?),
            6 => r.read_struct(ty, |r, _, ty| r.skip_value(ty))?,
            7 => dict = Some(read_dictionary_page(r, ty)?),
            8 => data_v2 = Some(read_data_page_v2(r, ty)?),
            _ => r.skip_value(ty)?,
        }
        Ok(())
    })?;

    let header_size = (input.len() - reader.remaining()) as u64;
    let page_type = required_field(page_type, "PageHeader.type")?;
    let uncompressed_size = non_negative_i32(
        required_field(uncompressed_size, "PageHeader.uncompressed_page_size")?,
        "uncompressed_page_size",
    )?;
    let compressed_size = non_negative_i32(
        required_field(compressed_size, "PageHeader.compressed_page_size")?,
        "compressed_page_size",
    )?;

    let kind = match page_type {
        PAGE_TYPE_DATA_PAGE => {
            let (num_values, encoding) = required_field(data_v1, "PageHeader.data_page_header")?;
            ParquetPageKind::DataPageV1 {
                num_values: non_negative_i32(num_values, "num_values")?,
                encoding,
            }
        }
        PAGE_TYPE_DATA_PAGE_V2 => {
            let (num_values, num_rows, encoding) =
                required_field(data_v2, "PageHeader.data_page_header_v2")?;
            ParquetPageKind::DataPageV2 {
                num_values: non_negative_i32(num_values, "num_values")?,
                num_rows: non_negative_i32(num_rows, "num_rows")?,
                encoding,
            }
        }
        PAGE_TYPE_DICTIONARY_PAGE => {
            let (num_values, encoding) = required_field(dict, "PageHeader.dictionary_page_header")?;
            ParquetPageKind::DictionaryPage {
                num_values: non_negative_i32(num_values, "num_values")?,
                encoding,
            }
        }
        PAGE_TYPE_INDEX_PAGE => ParquetPageKind::IndexPage,
        other => ParquetPageKind::Unknown(other),
    };

    Ok(ParquetPageHeader {
        kind,
        compressed_size,
        uncompressed_size,
        header_size,
        page_offset,
    })
}

/// Lazily iterates over all Parquet page headers in an in-memory column chunk slice.
pub fn iter_parquet_page_headers<'a>(
    column_name: &'a str,
    column_offset: u64,
    column_bytes: &'a [u8],
) -> ParquetPageHeaderIter<'a> {
    ParquetPageHeaderIter {
        column_name,
        column_offset,
        column_bytes,
        pos: 0,
    }
}

pub struct ParquetPageHeaderIter<'a> {
    column_name: &'a str,
    column_offset: u64,
    column_bytes: &'a [u8],
    pos: usize,
}

impl ParquetPageHeaderIter<'_> {
    fn next_page(&mut self) -> Result<ParquetPageHeader> {
        let page_offset = self
            .column_offset
            .checked_add(self.pos as u64)
            .ok_or_else(|| {
                ErrorCode::ParquetFileInvalid(format!(
                    "invalid parquet column chunk for {}, page offset overflow",
                    self.column_name
                ))
            })?;
        let page = read_parquet_page_header(&self.column_bytes[self.pos..], page_offset)?;
        let header_len = page.header_size as usize;
        let compressed_len = usize::try_from(page.compressed_size).map_err(|_| {
            ErrorCode::ParquetFileInvalid(format!(
                "invalid parquet column chunk for {}, compressed page size overflow",
                self.column_name
            ))
        })?;
        let data_end = self
            .pos
            .checked_add(header_len)
            .and_then(|end| end.checked_add(compressed_len))
            .ok_or_else(|| {
                ErrorCode::ParquetFileInvalid(format!(
                    "invalid parquet column chunk for {}, page data length overflow",
                    self.column_name
                ))
            })?;
        if data_end > self.column_bytes.len() {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "invalid parquet column chunk for {}, page data exceeds chunk size",
                self.column_name
            )));
        }
        if header_len == 0 && compressed_len == 0 {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "invalid parquet column chunk for {}, empty page header",
                self.column_name
            )));
        }
        self.pos = data_end;
        Ok(page)
    }
}

impl Iterator for ParquetPageHeaderIter<'_> {
    type Item = Result<ParquetPageHeader>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.column_bytes.len() {
            return None;
        }
        match self.next_page() {
            Ok(page) => Some(Ok(page)),
            Err(err) => {
                self.pos = self.column_bytes.len();
                Some(Err(err))
            }
        }
    }
}

fn read_data_page_v1(r: &mut ThriftCompactReader<'_>, ty: CompactType) -> Result<(i32, i32)> {
    let mut num_values = None;
    let mut encoding = None;
    let mut def_level_encoding = None;
    let mut rep_level_encoding = None;

    r.read_struct(ty, |r, id, ty| {
        match id {
            1 => num_values = Some(r.read_i32(ty)?),
            2 => encoding = Some(r.read_i32(ty)?),
            3 => def_level_encoding = Some(r.read_i32(ty)?),
            4 => rep_level_encoding = Some(r.read_i32(ty)?),
            _ => r.skip_value(ty)?,
        }
        Ok(())
    })?;

    let _ = required_field(
        def_level_encoding,
        "DataPageHeader.definition_level_encoding",
    )?;
    let _ = required_field(
        rep_level_encoding,
        "DataPageHeader.repetition_level_encoding",
    )?;
    Ok((
        required_field(num_values, "DataPageHeader.num_values")?,
        required_field(encoding, "DataPageHeader.encoding")?,
    ))
}

fn read_dictionary_page(r: &mut ThriftCompactReader<'_>, ty: CompactType) -> Result<(i32, i32)> {
    let mut num_values = None;
    let mut encoding = None;

    r.read_struct(ty, |r, id, ty| {
        match id {
            1 => num_values = Some(r.read_i32(ty)?),
            2 => encoding = Some(r.read_i32(ty)?),
            3 => {
                let _ = r.read_bool(ty)?;
            }
            _ => r.skip_value(ty)?,
        }
        Ok(())
    })?;

    Ok((
        required_field(num_values, "DictionaryPageHeader.num_values")?,
        required_field(encoding, "DictionaryPageHeader.encoding")?,
    ))
}

fn read_data_page_v2(r: &mut ThriftCompactReader<'_>, ty: CompactType) -> Result<(i32, i32, i32)> {
    let mut num_values = None;
    let mut num_nulls = None;
    let mut num_rows = None;
    let mut encoding = None;
    let mut def_levels_byte_len = None;
    let mut rep_levels_byte_len = None;

    r.read_struct(ty, |r, id, ty| {
        match id {
            1 => num_values = Some(r.read_i32(ty)?),
            2 => num_nulls = Some(r.read_i32(ty)?),
            3 => num_rows = Some(r.read_i32(ty)?),
            4 => encoding = Some(r.read_i32(ty)?),
            5 => def_levels_byte_len = Some(r.read_i32(ty)?),
            6 => rep_levels_byte_len = Some(r.read_i32(ty)?),
            7 => {
                let _ = r.read_bool(ty)?;
            }
            _ => r.skip_value(ty)?,
        }
        Ok(())
    })?;

    let _ = required_field(num_nulls, "DataPageHeaderV2.num_nulls")?;
    let _ = required_field(
        def_levels_byte_len,
        "DataPageHeaderV2.definition_levels_byte_length",
    )?;
    let _ = required_field(
        rep_levels_byte_len,
        "DataPageHeaderV2.repetition_levels_byte_length",
    )?;
    Ok((
        required_field(num_values, "DataPageHeaderV2.num_values")?,
        required_field(num_rows, "DataPageHeaderV2.num_rows")?,
        required_field(encoding, "DataPageHeaderV2.encoding")?,
    ))
}

fn required_field<T>(val: Option<T>, name: &'static str) -> Result<T> {
    val.ok_or_else(|| {
        ErrorCode::ParquetFileInvalid(format!(
            "invalid parquet page header: missing required field {name}"
        ))
    })
}

fn non_negative_i32(value: i32, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| {
        ErrorCode::ParquetFileInvalid(format!("invalid parquet page header: {field} is negative"))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
    use crate::thrift_compact::DEFAULT_MAX_THRIFT_DEPTH;

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

        let pages_v1: Vec<_> = iter_parquet_page_headers("s", col_offset, chunk_v1)
            .collect::<Result<_>>()
            .unwrap();
        assert!(pages_v1.len() >= 2);
        assert_eq!(pages_v1[0].page_type_name(), "dictionary");
        assert_eq!(pages_v1[0].num_values(), 3);
        assert_eq!(pages_v1[0].num_rows(), None);
        assert_eq!(pages_v1[0].page_offset, col_offset);

        let data_v1_values: u64 = pages_v1[1..]
            .iter()
            .map(|p| {
                assert_eq!(p.page_type_name(), "data_page_v1");
                assert_eq!(p.num_rows(), Some(p.num_values()));
                p.num_values()
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

        let pages_v2: Vec<_> = iter_parquet_page_headers("n", col_offset_v2, chunk_v2)
            .collect::<Result<_>>()
            .unwrap();
        assert!(!pages_v2.is_empty());
        let mut expected_offset = col_offset_v2;
        let mut total_rows = 0u64;
        for page in &pages_v2 {
            assert_eq!(page.page_type_name(), "data_page_v2");
            assert_eq!(page.page_offset, expected_offset);
            total_rows += page.num_rows().unwrap();
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

        let err = iter_parquet_page_headers("c", 0, &crafted)
            .next()
            .unwrap()
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::ParquetFileInvalid("").code());
    }

    #[test]
    fn test_rejects_oversized_containers_deep_nesting_and_bad_types() {
        // Unknown list<byte> field (id = 9, delta = 6 from field 3 -> 0x69) with count = 0x7FFFFFFF.
        let oversized_list = [
            0x15, 0x00, 0x15, 0x00, 0x15, 0x00, 0x69, 0xF3, 0xFF, 0xFF, 0xFF, 0xFF, 0x07, 0x00,
        ];
        assert!(read_parquet_page_header(&oversized_list, 0).is_err());

        // Unknown map<binary, binary> field (0x6B) with negative i32 count (u32::MAX).
        let negative_map = [
            0x15, 0x00, 0x15, 0x00, 0x15, 0x00, 0x6B, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F, 0x88, 0x00,
        ];
        assert!(read_parquet_page_header(&negative_map, 0).is_err());

        // Deeply nested unknown structs exceeding DEFAULT_MAX_THRIFT_DEPTH.
        let mut deep_structs = vec![0x15, 0x00, 0x15, 0x00, 0x15, 0x00];
        deep_structs.extend(std::iter::repeat_n(0x6C, DEFAULT_MAX_THRIFT_DEPTH + 4));
        deep_structs.extend(std::iter::repeat_n(0x00, DEFAULT_MAX_THRIFT_DEPTH + 5));
        let err = read_parquet_page_header(&deep_structs, 0).unwrap_err();
        assert!(err.message().contains("recursion depth"));

        // DictionaryPageHeader.is_sorted (field 3) sent as I32 (0x15) instead of Bool.
        let bad_bool_type = [
            0x15, 0x04, 0x15, 0x00, 0x15, 0x00, 0x4C, 0x15, 0x02, 0x15, 0x00, 0x15, 0x00, 0x00,
            0x00,
        ];
        assert!(read_parquet_page_header(&bad_bool_type, 0).is_err());
    }

    #[test]
    fn test_iterates_index_and_data_pages_with_offsets() {
        let mut bytes = vec![
            // Page 0: INDEX_PAGE (type = 1 -> zigzag 2), compressed_size = 3 bytes
            0x15, 0x02, 0x15, 0x06, 0x15, 0x06, 0x3C, 0x00, 0x00, 0xAA, 0xBB, 0xCC,
        ];
        let second_page_offset = bytes.len() as u64;
        bytes.extend_from_slice(&[
            // Page 1: DATA_PAGE (type = 0), compressed_size = 2 bytes, num_values = 4
            0x15, 0x00, 0x15, 0x04, 0x15, 0x04, 0x2C, 0x15, 0x08, 0x15, 0x00, 0x15, 0x06, 0x15,
            0x06, 0x00, 0x00, 0x11, 0x22,
        ]);

        let pages: Vec<_> = iter_parquet_page_headers("col", 100, &bytes)
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(pages.len(), 2);
        assert!(pages[0].is_index_page());
        assert_eq!(pages[0].page_offset, 100);

        assert!(!pages[1].is_index_page());
        assert_eq!(pages[1].page_type_name(), "data_page_v1");
        assert_eq!(pages[1].encoding_name(), "plain");
        assert_eq!(pages[1].num_values(), 4);
        assert_eq!(pages[1].num_rows(), Some(4));
        assert_eq!(pages[1].compressed_size, 2);
        assert_eq!(pages[1].page_offset, 100 + second_page_offset);
    }
}
