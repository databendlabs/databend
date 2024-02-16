// Copyright (c) 2020 Ritchie Vink
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

use parquet2::encoding::delta_bitpacked;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::statistics::serialize_statistics;
use parquet2::statistics::BinaryStatistics;
use parquet2::statistics::ParquetStatistics;
use parquet2::statistics::Statistics;

use crate::arrow::array::Array;
use crate::arrow::array::BinaryViewArray;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::read::schema::is_nullable;
use crate::arrow::io::parquet::write::binary::ord_binary;
use crate::arrow::io::parquet::write::utils;
use crate::arrow::io::parquet::write::WriteOptions;

pub(crate) fn encode_non_null_values<'a, I: Iterator<Item = &'a [u8]>>(
    iter: I,
    buffer: &mut Vec<u8>,
) {
    iter.for_each(|x| {
        // BYTE_ARRAY: first 4 bytes denote length in littleendian.
        let len = (x.len() as u32).to_le_bytes();
        buffer.extend_from_slice(&len);
        buffer.extend_from_slice(x);
    })
}

pub(crate) fn encode_plain(array: &BinaryViewArray, buffer: &mut Vec<u8>) {
    let capacity =
        array.total_bytes_len() + (array.len() - array.null_count()) * std::mem::size_of::<u32>();

    let len_before = buffer.len();
    buffer.reserve(capacity);

    encode_non_null_values(array.non_null_values_iter(), buffer);
    // Append the non-null values.
    debug_assert_eq!(buffer.len() - len_before, capacity);
}

pub(crate) fn encode_delta(array: &BinaryViewArray, buffer: &mut Vec<u8>) {
    let lengths = array.non_null_views_iter().map(|v| v.length as i64);
    delta_bitpacked::encode(lengths, buffer);

    for slice in array.non_null_values_iter() {
        buffer.extend_from_slice(slice)
    }
}

pub fn array_to_page(
    array: &BinaryViewArray,
    options: WriteOptions,
    type_: PrimitiveType,
    encoding: Encoding,
) -> Result<Page> {
    let validity = array.validity();
    let is_optional = is_nullable(&type_.field_info);

    let mut buffer = vec![];
    utils::write_def_levels(
        &mut buffer,
        is_optional,
        validity,
        array.len(),
        options.version,
    )?;

    let definition_levels_byte_length = buffer.len();

    match encoding {
        Encoding::Plain => encode_plain(array, &mut buffer),
        Encoding::DeltaLengthByteArray => encode_delta(array, &mut buffer),
        _ => {
            return Err(Error::oos(format!(
                "Datatype {:?} cannot be encoded by {:?} encoding",
                array.data_type(),
                encoding
            )));
        }
    }

    let statistics = if options.write_statistics {
        Some(build_statistics(array, type_.clone()))
    } else {
        None
    };

    utils::build_plain_page(
        buffer,
        array.len(),
        array.len(),
        array.null_count(),
        0,
        definition_levels_byte_length,
        statistics,
        type_,
        options,
        encoding,
    )
    .map(Page::Data)
}

pub(crate) fn build_statistics(
    array: &BinaryViewArray,
    primitive_type: PrimitiveType,
) -> ParquetStatistics {
    let statistics = &BinaryStatistics {
        primitive_type,
        null_count: Some(array.null_count() as i64),
        distinct_count: None,
        max_value: array
            .iter()
            .flatten()
            .max_by(|x, y| ord_binary(x, y))
            .map(|x| x.to_vec()),
        min_value: array
            .iter()
            .flatten()
            .min_by(|x, y| ord_binary(x, y))
            .map(|x| x.to_vec()),
    } as &dyn Statistics;
    serialize_statistics(statistics)
}
