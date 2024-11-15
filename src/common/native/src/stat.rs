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

use arrow_buffer::ArrowNativeType;
use arrow_schema::Field;

use crate::compression::Compression;
use crate::error::Result;
use crate::read::PageIterator;
use crate::CommonCompression;

#[derive(Debug)]
pub struct ColumnInfo {
    pub field: Field,
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

pub fn stat_simple<'a, I>(reader: I, field: Field) -> Result<ColumnInfo>
where I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync + 'a {
    let mut pages = vec![];

    for compressed in reader {
        let (num_values, buffer) = compressed?;

        let mut buffer = buffer.as_slice();
        let mut opt_validity_size = None;
        if field.is_nullable {
            let validity_size = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
            debug_assert!(validity_size == 0 || validity_size as u64 == num_values);
            let consume_validity_size = 4 + ((validity_size + 7) / 8) as usize;
            buffer.consume(consume_validity_size);
            if validity_size > 0 {
                opt_validity_size = Some(validity_size);
            }
        };

        let physical_type = field.data_type.to_physical_type();
        let page = stat_body(&mut buffer, opt_validity_size, physical_type)?;
        pages.push(page);
    }
    Ok(ColumnInfo { field, pages })
}

fn stat_body(
    buffer: &mut &[u8],
    opt_validity_size: Option<u32>,
    physical_type: PhysicalType,
) -> Result<PageInfo> {
    let codec = Compression::from_codec(buffer[0])?;
    let compressed_size = u32::from_le_bytes(buffer[1..5].try_into().unwrap());
    let uncompressed_size = u32::from_le_bytes(buffer[5..9].try_into().unwrap());
    *buffer = &buffer[9..];

    let body = match codec {
        Compression::Rle => PageBody::Rle,
        Compression::Dict => stat_dict_body(buffer, physical_type)?,
        Compression::OneValue => PageBody::OneValue,
        Compression::Freq => stat_freq_body(buffer, physical_type)?,
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

fn stat_freq_body(mut buffer: &[u8], physical_type: PhysicalType) -> Result<PageBody> {
    match physical_type {
        PhysicalType::Primitive(p) => {
            let top_value_size = size_of_primitive(p);
            buffer = &buffer[top_value_size..];
            let exceptions_bitmap_size = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
            buffer = &buffer[4 + exceptions_bitmap_size as usize..];
            let exceptions = stat_body(&mut buffer, None, physical_type)?;
            Ok(PageBody::Freq(FreqPageBody {
                exceptions: Some(Box::new(exceptions)),
                exceptions_bitmap_size,
            }))
        }
        PhysicalType::Binary
        | PhysicalType::LargeBinary
        | PhysicalType::Utf8
        | PhysicalType::LargeUtf8 => {
            let len = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
            buffer = &buffer[8 + len as usize..];
            let exceptions_bitmap_size = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
            Ok(PageBody::Freq(FreqPageBody {
                exceptions: None,
                exceptions_bitmap_size,
            }))
        }
        _ => unreachable!("type {:?} not supported", physical_type),
    }
}

fn stat_dict_body(mut buffer: &[u8], physical_type: PhysicalType) -> Result<PageBody> {
    let indices = stat_body(&mut buffer, None, physical_type)?;
    let unique_num = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
    Ok(PageBody::Dict(DictPageBody {
        indices: Box::new(indices),
        unique_num,
    }))
}

fn size_of_primitive(p: ArrowNativeType) -> usize {
    match p {
        ArrowNativeType::Int8 => 1,
        ArrowNativeType::Int16 => 2,
        ArrowNativeType::Int32 => 4,
        ArrowNativeType::Int64 => 8,
        ArrowNativeType::Int128 | ArrowNativeType::UInt128 => 16,
        ArrowNativeType::Int256 => 32,
        ArrowNativeType::UInt8 => 1,
        ArrowNativeType::UInt16 => 2,
        ArrowNativeType::UInt32 => 4,
        ArrowNativeType::UInt64 => 8,
        ArrowNativeType::Float16 => unimplemented!(),
        ArrowNativeType::Float32 => 4,
        ArrowNativeType::Float64 => 8,
        ArrowNativeType::DaysMs => unimplemented!(),
        ArrowNativeType::MonthDayNano => unimplemented!(),
    }
}

#[cfg(test)]
mod test {
    use std::io::BufRead;

    use arrow_array::Array;
    use arrow_array::PrimitiveArray;
    use arrow_schema::Field;
    use arrow_schema::Schema;

    use super::stat_simple;
    use super::ColumnInfo;
    use crate::read::reader::NativeReader;
    use crate::stat::PageBody;
    use crate::util::env::remove_all_env;
    use crate::util::env::set_dict_env;
    use crate::util::env::set_freq_env;
    use crate::write::NativeWriter;
    use crate::write::WriteOptions;
    use crate::CommonCompression;

    const PAGE_SIZE: usize = 2048;
    const PAGE_PER_COLUMN: usize = 10;
    const COLUMN_SIZE: usize = PAGE_SIZE * PAGE_PER_COLUMN;

    fn write_and_stat_simple_column(array: ArrayRef) -> ColumnInfo {
        assert!(array.data_type().is_primitive());
        let options = WriteOptions {
            default_compression: CommonCompression::Lz4,
            max_page_size: Some(PAGE_SIZE),
            default_compress_ratio: Some(1.2),
            forbidden_compressions: vec![],
        };

        let mut bytes = Vec::new();
        let field = Field::new(
            "name",
            array.data_type().clone(),
            array.validity().is_some(),
        );
        let schema = Schema::from(vec![field.clone()]);
        let mut writer = NativeWriter::new(&mut bytes, schema, options).unwrap();

        writer.start().unwrap();
        writer.write(&Chunk::new(vec![array])).unwrap();
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
        let array = Box::new(PrimitiveArray::<i64>::from_iter(values));
        let column_info = write_and_stat_simple_column(array.clone());

        assert_eq!(column_info.pages.len(), 10);
        for p in column_info.pages {
            assert_eq!(p.validity_size, Some(PAGE_SIZE as u32));
        }

        let array = Box::new(BinaryArray::<i64>::from_iter_values(
            ["a"; COLUMN_SIZE].iter(),
        ));
        let column_info = write_and_stat_simple_column(array.clone());
        assert_eq!(column_info.pages.len(), 10);
        for p in column_info.pages {
            assert_eq!(p.validity_size, None);
            assert!(matches!(p.body, PageBody::OneValue));
        }

        set_dict_env();
        let column_info = write_and_stat_simple_column(array.clone());
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
        let column_info = write_and_stat_simple_column(array);
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
