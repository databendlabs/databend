// Copyright 2023 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;

use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalSize;
use crate::types::DecimalDataType;
use crate::Column;
use crate::Scalar;
use crate::TypeDeserializer;

pub struct DecimalDeserializer<T: Decimal> {
    pub values: Vec<T>,
    pub ty: DecimalDataType,
    // for fast access
    pub size: DecimalSize,
}

impl<T: Decimal> DecimalDeserializer<T> {
    pub fn with_capacity(ty: &DecimalDataType, capacity: usize) -> Self {
        Self {
            size: ty.size(),
            ty: *ty,
            values: Vec::with_capacity(capacity),
        }
    }
}

impl<T: Decimal> DecimalDeserializer<T> {
    pub fn de_json_inner(&mut self, value: &serde_json::Value) -> Result<()> {
        match value {
            serde_json::Value::Number(n) => {
                if n.is_i64() {
                    self.values.push(
                        T::from_i64(n.as_i64().unwrap())
                            .with_size(self.size)
                            .ok_or_else(overflow_error)?,
                    );
                    Ok(())
                } else if n.is_u64() {
                    self.values.push(
                        T::from_u64(n.as_u64().unwrap())
                            .with_size(self.size)
                            .ok_or_else(overflow_error)?,
                    );
                    Ok(())
                } else {
                    Err(parse_error("Incorrect json value for decimal"))
                }
            }
            serde_json::Value::String(s) => {
                let (n, _) = read_decimal_with_size::<T>(s.as_bytes(), self.size, true)?;
                self.values.push(n);
                Ok(())
            }
            _ => Err(ErrorCode::from("Incorrect json value for decimal")),
        }
    }
}

impl<T: Decimal> TypeDeserializer for DecimalDeserializer<T> {
    fn memory_size(&self) -> usize {
        self.values.len() * T::mem_size()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    // See GroupHash.rs for StringColumn
    #[allow(clippy::uninit_vec)]
    fn de_binary(&mut self, reader: &mut &[u8], _format: &FormatSettings) -> Result<()> {
        let t: T = T::de_binary(reader);
        self.values.push(t);
        Ok(())
    }

    fn de_default(&mut self) {
        self.values.push(T::zero());
    }

    fn de_fixed_binary_batch(
        &mut self,
        reader: &[u8],
        step: usize,
        rows: usize,
        _format: &FormatSettings,
    ) -> Result<()> {
        for row in 0..rows {
            let mut row_reader = &reader[step * row..];
            let value: T = T::de_binary(&mut row_reader);
            self.values.push(value);
        }
        Ok(())
    }

    fn de_json(&mut self, value: &serde_json::Value, _format: &FormatSettings) -> Result<()> {
        self.de_json_inner(value)
    }

    fn append_data_value(&mut self, value: Scalar, _format: &FormatSettings) -> Result<()> {
        let d = value
            .as_decimal()
            .ok_or_else(|| ErrorCode::from("Unable to get decimal value"))?;
        let i = T::try_downcast_scalar(d)
            .ok_or_else(|| ErrorCode::from("Unable to get decimal value"))?;
        self.values.push(i);
        Ok(())
    }

    fn pop_data_value(&mut self) -> Result<Scalar> {
        match self.values.pop() {
            Some(v) => Ok(T::upcast_scalar(v, self.size)),
            None => Err(ErrorCode::from(
                "Decimal column is empty when pop data value",
            )),
        }
    }

    fn finish_to_column(&mut self) -> Column {
        Column::Decimal(T::to_column(std::mem::take(&mut self.values), self.size))
    }
}

fn parse_error(msg: &str) -> ErrorCode {
    ErrorCode::BadArguments(format!("bad decimal literal: {msg}"))
}

fn overflow_error() -> ErrorCode {
    ErrorCode::Overflow("decimal overflow")
}

pub fn read_decimal_with_size<T: Decimal>(
    buf: &[u8],
    size: DecimalSize,
    exact: bool,
) -> Result<(T, usize)> {
    let (n, d, e, n_read) = read_decimal::<T>(buf, size.precision as u32, exact)?;
    if d as i32 + e > (size.precision - size.scale).into() {
        return Err(overflow_error());
    }
    let scale_diff = e + size.scale as i32;
    let n = match scale_diff.cmp(&0) {
        Ordering::Less => {
            // e < 0, than  -e is the actual scale, (-e) > scale means we need to cut more
            n.checked_div(T::e(-scale_diff as u32))
                .ok_or_else(overflow_error)?
        }
        Ordering::Greater => n
            .checked_mul(T::e(scale_diff as u32))
            .ok_or_else(overflow_error)?,
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
        return Err(parse_error("empty"));
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
                    return Err(overflow_error());
                } else {
                    let v = buf[pos];
                    if v == b'0' {
                        zeros += 1;
                    } else {
                        n = n.checked_mul(T::e(zeros + 1)).ok_or_else(overflow_error)?;
                        n = n
                            .checked_add(T::from_u64((v - b'0') as u64))
                            .ok_or_else(overflow_error)?;
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
                    return Err(parse_error("unexpected char"));
                } else {
                    stop = pos as i32;
                    break;
                }
            }
        }
        pos += 1;
    }

    if zeros > 0 {
        n = n.checked_mul(T::e(zeros)).ok_or_else(overflow_error)?;
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
                        n = n.checked_mul(T::e(zeros + 1)).ok_or_else(overflow_error)?;
                        n = n
                            .checked_add(T::from_u64((v - b'0') as u64))
                            .ok_or_else(overflow_error)?;
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
                        return Err(parse_error("unexpected char"));
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
        return Err(parse_error("no digits"));
    }

    let mut exponent = if has_point {
        leading_digits as i32 - digits as i32
    } else {
        0i32
    };

    if has_e && stop < 0 {
        let mut exp = 0i32;
        if pos == len - 1 {
            return Err(parse_error("empty exponent"));
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
            return Err(parse_error("bad exponent"));
        }

        for (i, v) in buf[pos..].iter().enumerate() {
            match v {
                b'0'..=b'9' => {
                    exp *= 10;
                    exp += (v - b'0') as i32
                }
                c => {
                    if exact {
                        return Err(parse_error(&format!("unexpected char: {c}")));
                    } else {
                        stop = (pos + i) as i32;
                        break;
                    }
                }
            }
        }
        exponent += exp * exp_sign;
    }

    let n = n.checked_mul(sign).ok_or_else(overflow_error)?;
    let n_read = if stop > 0 { stop as usize } else { len };
    Ok((n, digits as u8, exponent, n_read))
}
