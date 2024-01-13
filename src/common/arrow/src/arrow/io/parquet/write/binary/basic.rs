// Copyright 2020-2022 Jorge C. Leitão
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
use parquet2::page::DataPage;
use parquet2::schema::types::PrimitiveType;
use parquet2::statistics::serialize_statistics;
use parquet2::statistics::BinaryStatistics;
use parquet2::statistics::ParquetStatistics;
use parquet2::statistics::Statistics;

use super::super::utils;
use super::super::WriteOptions;
use crate::arrow::array::Array;
use crate::arrow::array::BinaryArray;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::read::schema::is_nullable;
use crate::arrow::offset::Offset;

pub(crate) fn encode_plain<O: Offset>(
    array: &BinaryArray<O>,
    is_optional: bool,
    buffer: &mut Vec<u8>,
) {
    // append the non-null values
    if is_optional {
        array.iter().for_each(|x| {
            if let Some(x) = x {
                // BYTE_ARRAY: first 4 bytes denote length in littleendian.
                let len = (x.len() as u32).to_le_bytes();
                buffer.extend_from_slice(&len);
                buffer.extend_from_slice(x);
            }
        })
    } else {
        array.values_iter().for_each(|x| {
            // BYTE_ARRAY: first 4 bytes denote length in littleendian.
            let len = (x.len() as u32).to_le_bytes();
            buffer.extend_from_slice(&len);
            buffer.extend_from_slice(x);
        })
    }
}

pub fn array_to_page<O: Offset>(
    array: &BinaryArray<O>,
    options: WriteOptions,
    type_: PrimitiveType,
    encoding: Encoding,
) -> Result<DataPage> {
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
        Encoding::Plain => encode_plain(array, is_optional, &mut buffer),
        Encoding::DeltaLengthByteArray => encode_delta(
            array.values(),
            array.offsets().buffer(),
            array.validity(),
            is_optional,
            &mut buffer,
        ),
        _ => {
            return Err(Error::InvalidArgumentError(format!(
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
}

pub(crate) fn build_statistics<O: Offset>(
    array: &BinaryArray<O>,
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

pub(crate) fn encode_delta<O: Offset>(
    values: &[u8],
    offsets: &[O],
    validity: Option<&Bitmap>,
    is_optional: bool,
    buffer: &mut Vec<u8>,
) {
    if is_optional {
        if let Some(validity) = validity {
            let lengths = offsets
                .windows(2)
                .map(|w| (w[1] - w[0]).to_usize() as i64)
                .zip(validity.iter())
                .flat_map(|(x, is_valid)| if is_valid { Some(x) } else { None });
            let length = offsets.len() - 1 - validity.unset_bits();
            let lengths = utils::ExactSizedIter::new(lengths, length);

            delta_bitpacked::encode(lengths, buffer);
        } else {
            let lengths = offsets.windows(2).map(|w| (w[1] - w[0]).to_usize() as i64);
            delta_bitpacked::encode(lengths, buffer);
        }
    } else {
        let lengths = offsets.windows(2).map(|w| (w[1] - w[0]).to_usize() as i64);
        delta_bitpacked::encode(lengths, buffer);
    }

    buffer.extend_from_slice(
        &values[offsets.first().unwrap().to_usize()..offsets.last().unwrap().to_usize()],
    )
}

/// Returns the ordering of two binary values. This corresponds to pyarrows' ordering
/// of statistics.
pub(crate) fn ord_binary<'a>(a: &'a [u8], b: &'a [u8]) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a.is_empty(), b.is_empty()) {
        (true, true) => return Equal,
        (true, false) => return Less,
        (false, true) => return Greater,
        (false, false) => {}
    }

    for (v1, v2) in a.iter().zip(b.iter()) {
        match v1.cmp(v2) {
            Equal => continue,
            other => return other,
        }
    }
    Equal
}
