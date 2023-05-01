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

use std::cmp::Ordering;

use chrono::Datelike;
use chrono::NaiveDate;
use common_arrow::arrow::chunk::Chunk as ArrowChunk;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::io::parquet::write::transverse;
use common_arrow::arrow::io::parquet::write::RowGroupIterator;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::parquet::compression::CompressionOptions;
use common_arrow::parquet::encoding::Encoding;
use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_arrow::parquet::write::Version;
use common_arrow::write_parquet_file;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalSize;
use crate::DataBlock;
use crate::TableSchema;

pub fn serialize_to_parquet_with_compression(
    blocks: Vec<DataBlock>,
    schema: impl AsRef<TableSchema>,
    buf: &mut Vec<u8>,
    compression: CompressionOptions,
) -> Result<(u64, ThriftFileMetaData)> {
    let arrow_schema = schema.as_ref().to_arrow();

    let row_group_write_options = WriteOptions {
        write_statistics: false,
        compression,
        version: Version::V2,
        data_pagesize_limit: None,
    };
    let batches = blocks
        .into_iter()
        .map(ArrowChunk::try_from)
        .collect::<Result<Vec<_>>>()?;

    let encoding_map = |data_type: &ArrowDataType| match data_type {
        ArrowDataType::Dictionary(..) => Encoding::RleDictionary,
        _ => col_encoding(data_type),
    };

    let encodings: Vec<Vec<_>> = arrow_schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, encoding_map))
        .collect::<Vec<_>>();

    let row_groups = RowGroupIterator::try_new(
        batches.into_iter().map(Ok),
        &arrow_schema,
        row_group_write_options,
        encodings,
    )?;

    use common_arrow::parquet::write::WriteOptions as FileWriteOption;
    let options = FileWriteOption {
        write_statistics: false,
        version: Version::V2,
    };

    match write_parquet_file(buf, row_groups, arrow_schema.clone(), options) {
        Ok(result) => Ok(result),
        Err(cause) => Err(ErrorCode::ParquetFileInvalid(cause.to_string())),
    }
}

pub fn serialize_to_parquet(
    blocks: Vec<DataBlock>,
    schema: impl AsRef<TableSchema>,
    buf: &mut Vec<u8>,
) -> Result<(u64, ThriftFileMetaData)> {
    serialize_to_parquet_with_compression(blocks, schema, buf, CompressionOptions::Lz4Raw)
}

pub fn col_encoding(_data_type: &ArrowDataType) -> Encoding {
    // Although encoding does work, parquet2 has not implemented decoding of DeltaLengthByteArray yet, we fallback to Plain
    // From parquet2: Decoding "DeltaLengthByteArray"-encoded required V2 pages is not yet implemented for Binary.
    //
    // match data_type {
    //    ArrowDataType::Binary
    //    | ArrowDataType::LargeBinary
    //    | ArrowDataType::Utf8
    //    | ArrowDataType::LargeUtf8 => Encoding::DeltaLengthByteArray,
    //    _ => Encoding::Plain,
    //}
    Encoding::Plain
}

pub const EPOCH_DAYS_FROM_CE: i32 = 719_163;

#[inline]
pub fn uniform_date(date: NaiveDate) -> i32 {
    date.num_days_from_ce() - EPOCH_DAYS_FROM_CE
}

pub fn read_decimal_with_size<T: Decimal>(
    buf: &[u8],
    size: DecimalSize,
    exact: bool,
) -> Result<(T, usize)> {
    let (n, d, e, n_read) = read_decimal::<T>(buf, size.precision as u32, exact)?;
    if d as i32 + e > (size.precision - size.scale).into() {
        return Err(decimal_overflow_error());
    }
    let scale_diff = e + size.scale as i32;
    let n = match scale_diff.cmp(&0) {
        Ordering::Less => {
            // e < 0, than  -e is the actual scale, (-e) > scale means we need to cut more
            n.checked_div(T::e(-scale_diff as u32))
                .ok_or_else(decimal_overflow_error)?
        }
        Ordering::Greater => n
            .checked_mul(T::e(scale_diff as u32))
            .ok_or_else(decimal_overflow_error)?,
        Ordering::Equal => n,
    };
    Ok((n, n_read))
}

/// Return (n, n_digits, exponent, bytes_consumed), where:
///   value = n * 10^exponent.
///   n has n_digits digits, with no leading or fraction trailing zero.
///   Excessive digits in a fraction are discarded (not rounded)
/// no information is lost except excessive digits cut due to max_digits.
/// e.g '010.010' return (1001, 4 -2, 7)
/// usage:
///   used directly: for example 'select 1.1' should return a decimal
///   used read_decimal_with_size: for example 'select 1.1' should return a decimal
pub fn read_decimal<T: Decimal>(
    buf: &[u8],
    max_digits: u32,
    exact: bool,
) -> Result<(T, u8, i32, usize)> {
    if buf.is_empty() {
        return Err(decimal_parse_error("empty"));
    }

    let mut n = T::zero();
    let mut pos = 0;
    let len = buf.len();

    let sign = match buf[0] {
        b'+' => {
            pos += 1;
            T::one()
        }
        b'-' => {
            pos += 1;
            T::minus_one()
        }
        _ => T::one(),
    };

    let mut digits = 0;
    let mut leading_zero = false;
    let mut leading_digits = 0;
    let mut zeros = 0;
    let mut has_point = false;
    let mut has_e = false;
    let mut stop = -1;

    // ignore leading zeros
    while pos < len && buf[pos] == b'0' {
        pos += 1;
        leading_zero = true
    }

    // use 3 separate loops make code more clear and each loop is more tight.
    while pos < len {
        match buf[pos] {
            b'0'..=b'9' => {
                digits += 1;
                if digits > max_digits {
                    return Err(decimal_overflow_error());
                } else {
                    let v = buf[pos];
                    if v == b'0' {
                        zeros += 1;
                    } else {
                        n = n
                            .checked_mul(T::e(zeros + 1))
                            .ok_or_else(decimal_overflow_error)?;
                        n = n
                            .checked_add(T::from_u64((v - b'0') as u64))
                            .ok_or_else(decimal_overflow_error)?;
                        zeros = 0;
                    }
                }
            }
            b'.' => {
                has_point = true;
                leading_digits = digits;
                pos += 1;
                break;
            }
            b'e' | b'E' => {
                has_e = true;
                pos += 1;
                break;
            }
            _ => {
                if exact {
                    return Err(decimal_parse_error("unexpected char"));
                } else {
                    stop = pos as i32;
                    break;
                }
            }
        }
        pos += 1;
    }

    if zeros > 0 {
        n = n
            .checked_mul(T::e(zeros))
            .ok_or_else(decimal_overflow_error)?;
        zeros = 0;
    }

    if has_point && stop < 0 {
        while pos < len {
            match buf[pos] {
                b'0' => {
                    if digits >= max_digits {
                        // cut and consume excessive digits.
                        pos += 1;
                        continue;
                    } else {
                        zeros += 1;
                    }
                }

                b'1'..=b'9' => {
                    if digits >= max_digits {
                        // cut and consume excessive digits.
                        pos += 1;
                        continue;
                    } else {
                        let v = buf[pos];
                        n = n
                            .checked_mul(T::e(zeros + 1))
                            .ok_or_else(decimal_overflow_error)?;
                        n = n
                            .checked_add(T::from_u64((v - b'0') as u64))
                            .ok_or_else(decimal_overflow_error)?;
                        digits += zeros + 1;
                        zeros = 0;
                    }
                }
                b'e' | b'E' => {
                    has_e = true;
                    pos += 1;
                    break;
                }
                _ => {
                    if exact {
                        return Err(decimal_parse_error("unexpected char"));
                    } else {
                        stop = pos as i32;
                        break;
                    }
                }
            }
            pos += 1;
        }
    }

    if digits == 0 && zeros == 0 && !leading_zero {
        // these are ok: 0 0.0 0. .0 +0
        return Err(decimal_parse_error("no digits"));
    }

    let mut exponent = if has_point {
        leading_digits as i32 - digits as i32
    } else {
        0i32
    };

    if has_e && stop < 0 {
        let mut exp = 0i32;
        if pos == len - 1 {
            return Err(decimal_parse_error("empty exponent"));
        }

        let exp_sign = match buf[pos] {
            b'+' => {
                pos += 1;
                1
            }
            b'-' => {
                pos += 1;
                -1
            }
            _ => 1,
        };

        if pos == len - 1 {
            return Err(decimal_parse_error("bad exponent"));
        }

        for (i, v) in buf[pos..].iter().enumerate() {
            match v {
                b'0'..=b'9' => {
                    exp *= 10;
                    exp += (v - b'0') as i32
                }
                c => {
                    if exact {
                        return Err(decimal_parse_error(&format!("unexpected char: {c}")));
                    } else {
                        stop = (pos + i) as i32;
                        break;
                    }
                }
            }
        }
        exponent += exp * exp_sign;
    }

    let n = n.checked_mul(sign).ok_or_else(decimal_overflow_error)?;
    let n_read = if stop > 0 { stop as usize } else { len };
    Ok((n, digits as u8, exponent, n_read))
}

pub fn read_decimal_from_json<T: Decimal>(
    value: &serde_json::Value,
    size: DecimalSize,
) -> Result<T> {
    match value {
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                Ok(T::from_i64(n.as_i64().unwrap())
                    .with_size(size)
                    .ok_or_else(decimal_overflow_error)?)
            } else if n.is_u64() {
                Ok(T::from_u64(n.as_u64().unwrap())
                    .with_size(size)
                    .ok_or_else(decimal_overflow_error)?)
            } else {
                let f = n.as_f64().unwrap() * (10_f64).powi(size.scale as i32);
                let n = T::from_float(f);
                Ok(n)
            }
        }
        serde_json::Value::String(s) => {
            let (n, _) = read_decimal_with_size::<T>(s.as_bytes(), size, true)?;
            Ok(n)
        }
        _ => Err(ErrorCode::from("Incorrect json value for decimal")),
    }
}

fn decimal_parse_error(msg: &str) -> ErrorCode {
    ErrorCode::BadArguments(format!("bad decimal literal: {msg}"))
}

fn decimal_overflow_error() -> ErrorCode {
    ErrorCode::Overflow("decimal overflow")
}
