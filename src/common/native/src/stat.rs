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

use std::io::BufRead;

use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::types::NumberDataType;

use crate::CommonCompression;
use crate::compression::Compression;
use crate::error::Result;
use crate::read::PageIterator;

#[derive(Debug)]
pub struct ColumnInfo {
    pub field: TableField,
    pub pages: Vec<PageInfo>,
}

#[derive(Debug)]
pub struct PageInfo {
    pub validity_size: Option<u32>,
    pub compressed_size: u32,
    pub uncompressed_size: u32,
    pub body: PageBody,
}

#[derive(Debug)]
pub enum PageBody {
    Dict(DictPageBody),
    Freq(FreqPageBody),
    OneValue,
    Rle,
    Patas,
    Bitpack,
    DeltaBitpack,
    Common(CommonCompression),
}

#[derive(Debug)]
pub struct FreqPageBody {
    pub exceptions: Option<Box<PageInfo>>,
    pub exceptions_bitmap_size: u32,
}

#[derive(Debug)]
pub struct DictPageBody {
    pub indices: Box<PageInfo>,
    pub unique_num: u32,
}

pub fn stat_simple<'a, I>(reader: I, field: TableField) -> Result<ColumnInfo>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync + 'a {
    let mut pages = vec![];

    for compressed in reader {
        let (num_values, buffer) = compressed?;

        let mut buffer = buffer.as_slice();
        let mut opt_validity_size = None;
        if field.data_type().is_nullable() {
            let validity_size = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
            debug_assert!(validity_size == 0 || validity_size as u64 == num_values);
            let consume_validity_size = 4 + validity_size.div_ceil(8) as usize;
            buffer.consume(consume_validity_size);
            if validity_size > 0 {
                opt_validity_size = Some(validity_size);
            }
        };

        let data_type = field.data_type();
        let page = stat_body(&mut buffer, opt_validity_size, data_type)?;
        pages.push(page);
    }
    Ok(ColumnInfo { field, pages })
}

fn stat_body(
    buffer: &mut &[u8],
    opt_validity_size: Option<u32>,
    data_type: &TableDataType,
) -> Result<PageInfo> {
    let codec = Compression::from_codec(buffer[0])?;
    let compressed_size = u32::from_le_bytes(buffer[1..5].try_into().unwrap());
    let uncompressed_size = u32::from_le_bytes(buffer[5..9].try_into().unwrap());
    *buffer = &buffer[9..];

    let body = match codec {
        Compression::Rle => PageBody::Rle,
        Compression::Dict => stat_dict_body(buffer, data_type)?,
        Compression::OneValue => PageBody::OneValue,
        Compression::Freq => stat_freq_body(buffer, data_type)?,
        Compression::Bitpacking => PageBody::Bitpack,
        Compression::DeltaBitpacking => PageBody::DeltaBitpack,
        Compression::Patas => PageBody::Patas,
        _ => PageBody::Common(CommonCompression::try_from(&codec).unwrap()),
    };
    *buffer = &buffer[compressed_size as usize..];
    Ok(PageInfo {
        validity_size: opt_validity_size,
        compressed_size,
        uncompressed_size,
        body,
    })
}

fn stat_freq_body(mut buffer: &[u8], data_type: &TableDataType) -> Result<PageBody> {
    match data_type {
        TableDataType::Number(p) => {
            let top_value_size = size_of_primitive(p);
            buffer = &buffer[top_value_size..];
            let exceptions_bitmap_size = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
            buffer = &buffer[4 + exceptions_bitmap_size as usize..];
            let exceptions = stat_body(&mut buffer, None, data_type)?;
            Ok(PageBody::Freq(FreqPageBody {
                exceptions: Some(Box::new(exceptions)),
                exceptions_bitmap_size,
            }))
        }
        TableDataType::Decimal(decimal_type) => {
            let top_value_size = if decimal_type.size().can_carried_by_128() {
                16
            } else {
                32
            };
            buffer = &buffer[top_value_size..];
            let exceptions_bitmap_size = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
            buffer = &buffer[4 + exceptions_bitmap_size as usize..];
            let exceptions = stat_body(&mut buffer, None, data_type)?;
            Ok(PageBody::Freq(FreqPageBody {
                exceptions: Some(Box::new(exceptions)),
                exceptions_bitmap_size,
            }))
        }
        TableDataType::Binary | TableDataType::String => {
            let len = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
            buffer = &buffer[8 + len as usize..];
            let exceptions_bitmap_size = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
            Ok(PageBody::Freq(FreqPageBody {
                exceptions: None,
                exceptions_bitmap_size,
            }))
        }
        _ => unreachable!("type {:?} not supported", data_type),
    }
}

fn stat_dict_body(mut buffer: &[u8], data_type: &TableDataType) -> Result<PageBody> {
    let indices = stat_body(&mut buffer, None, data_type)?;
    let unique_num = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
    Ok(PageBody::Dict(DictPageBody {
        indices: Box::new(indices),
        unique_num,
    }))
}

fn size_of_primitive(p: &NumberDataType) -> usize {
    match p {
        NumberDataType::Int8 => 1,
        NumberDataType::Int16 => 2,
        NumberDataType::Int32 => 4,
        NumberDataType::Int64 => 8,

        NumberDataType::UInt8 => 1,
        NumberDataType::UInt16 => 2,
        NumberDataType::UInt32 => 4,
        NumberDataType::UInt64 => 8,
        NumberDataType::Float32 => 4,
        NumberDataType::Float64 => 8,
    }
}

#[cfg(test)]
mod test {
    use std::io::BufRead;

    use databend_common_column::binary::BinaryColumn;
    use databend_common_expression::Column;
    use databend_common_expression::FromData;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::infer_schema_type;
    use databend_common_expression::types::Int64Type;

    use super::ColumnInfo;
    use super::stat_simple;
    use crate::CommonCompression;
    use crate::read::reader::NativeReader;
    use crate::stat::PageBody;
    use crate::util::env::remove_all_env;
    use crate::util::env::set_dict_env;
    use crate::util::env::set_freq_env;
    use crate::write::NativeWriter;
    use crate::write::WriteOptions;

    const PAGE_SIZE: usize = 2048;
    const PAGE_PER_COLUMN: usize = 10;
    const COLUMN_SIZE: usize = PAGE_SIZE * PAGE_PER_COLUMN;

    fn write_and_stat_simple_column(column: Column) -> ColumnInfo {
        let options = WriteOptions {
            default_compression: CommonCompression::Lz4,
            max_page_size: Some(PAGE_SIZE),
            default_compress_ratio: Some(1.2),
            forbidden_compressions: vec![],
        };

        let mut bytes = Vec::new();
        let field = TableField::new("name", infer_schema_type(&column.data_type()).unwrap());
        let table_schema = TableSchema::new(vec![field.clone()]);
        let mut writer = NativeWriter::new(&mut bytes, table_schema, options).unwrap();

        writer.start().unwrap();
        writer.write(&[column]).unwrap();
        writer.finish().unwrap();

        let meta = writer.metas[0].clone();

        let mut range_bytes = std::io::Cursor::new(bytes.clone());
        range_bytes.consume(meta.offset as usize);

        let native_reader = NativeReader::new(range_bytes, meta.pages, vec![]);
        stat_simple(native_reader, field).unwrap()
    }

    #[test]
    fn test_stat_simple() {
        remove_all_env();

        let values: Vec<Option<i64>> = (0..COLUMN_SIZE)
            .map(|d| if d % 3 == 0 { None } else { Some(d as i64) })
            .collect();
        let column = Int64Type::from_opt_data(values);
        let column_info = write_and_stat_simple_column(column.clone());

        assert_eq!(column_info.pages.len(), 10);
        for p in column_info.pages {
            assert_eq!(p.validity_size, Some(PAGE_SIZE as u32));
        }

        let column = Column::Binary(BinaryColumn::from_iter(["a"; COLUMN_SIZE].iter()));
        let column_info = write_and_stat_simple_column(column.clone());
        assert_eq!(column_info.pages.len(), 10);
        for p in column_info.pages {
            assert_eq!(p.validity_size, None);
            assert!(matches!(p.body, PageBody::OneValue));
        }

        set_dict_env();
        let column_info = write_and_stat_simple_column(column.clone());
        assert_eq!(column_info.pages.len(), 10);
        for p in column_info.pages {
            assert_eq!(p.validity_size, None);
            match p.body {
                PageBody::Dict(dict) => {
                    assert_eq!(dict.unique_num, 1);
                    assert_eq!(dict.indices.validity_size, None);
                    assert!(matches!(dict.indices.body, PageBody::OneValue));
                }
                _ => panic!("expect dict page"),
            }
        }
        remove_all_env();

        set_freq_env();
        let column_info = write_and_stat_simple_column(column);
        assert_eq!(column_info.pages.len(), 10);
        for p in column_info.pages {
            assert_eq!(p.validity_size, None);
            match p.body {
                PageBody::Freq(freq) => {
                    assert!(freq.exceptions.is_none());
                }
                _ => panic!("expect freq page"),
            }
        }
    }
}
