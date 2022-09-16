// Copyright 2022 Datafuse Labs.
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

use super::constants::*;
use super::error::Error;

#[allow(clippy::zero_prefixed_literal)]
static HEX: [u8; 256] = {
    const __: u8 = 255; // not a hex digit
    [
        //   1   2   3   4   5   6   7   8   9   A   B   C   D   E   F
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 0
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 1
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 2
        00, 01, 02, 03, 04, 05, 06, 07, 08, 09, __, __, __, __, __, __, // 3
        __, 10, 11, 12, 13, 14, 15, __, __, __, __, __, __, __, __, __, // 4
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 5
        __, 10, 11, 12, 13, 14, 15, __, __, __, __, __, __, __, __, __, // 6
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 7
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 8
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // 9
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // A
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // B
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // C
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // D
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // E
        __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, // F
    ]
};

pub fn parse_escaped_string<'a>(
    mut data: &'a [u8],
    str_buf: &mut String,
) -> Result<&'a [u8], Error> {
    let byte = data[0];
    data = &data[1..];
    match byte {
        b'\\' => str_buf.push(BS),
        b'"' => str_buf.push(QU),
        b'/' => str_buf.push(SD),
        b'b' => str_buf.push(BB),
        b'f' => str_buf.push(FF),
        b'n' => str_buf.push(NN),
        b'r' => str_buf.push(RR),
        b't' => str_buf.push(TT),
        b'u' => {
            let mut numbers = vec![0; UNICODE_LEN];
            data.read_exact(numbers.as_mut_slice())?;
            let hex = decode_hex_escape(numbers)?;

            let c = match hex {
                n @ 0xDC00..=0xDFFF => {
                    return Err(Error::InvalidLoneLeadingSurrogateInHexEscape(n));
                }

                // Non-BMP characters are encoded as a sequence of two hex
                // escapes, representing UTF-16 surrogates. If deserializing a
                // utf-8 string the surrogates are required to be paired,
                // whereas deserializing a byte string accepts lone surrogates.
                n1 @ 0xD800..=0xDBFF => {
                    let next_byte = data.first().ok_or(Error::InvalidEOF)?;
                    if *next_byte == b'\\' {
                        data = &data[1..];
                    } else {
                        return Err(Error::UnexpectedEndOfHexEscape);
                    }
                    let next_byte = data.first().ok_or(Error::InvalidEOF)?;
                    if *next_byte == b'u' {
                        data = &data[1..];
                    } else {
                        return parse_escaped_string(data, str_buf);
                    }
                    let mut numbers = vec![0; UNICODE_LEN];
                    data.read_exact(numbers.as_mut_slice())?;
                    let n2 = decode_hex_escape(numbers)?;
                    if !(0xDC00..=0xDFFF).contains(&n2) {
                        return Err(Error::InvalidSurrogateInHexEscape(n2));
                    }

                    let n = (((n1 - 0xD800) as u32) << 10 | (n2 - 0xDC00) as u32) + 0x1_0000;
                    char::from_u32(n as u32).unwrap()
                }

                // Every u16 outside of the surrogate ranges above is guaranteed
                // to be a legal char.
                n => char::from_u32(n as u32).unwrap(),
            };
            str_buf.push(c);
        }
        other => return Err(Error::InvalidEscaped(other)),
    }
    Ok(data)
}

#[inline]
fn decode_hex_val(val: u8) -> Option<u16> {
    let n = HEX[val as usize] as u16;
    if n == 255 { None } else { Some(n) }
}

#[inline]
fn decode_hex_escape(numbers: Vec<u8>) -> Result<u16, Error> {
    let mut n = 0;
    for number in numbers {
        let hex = decode_hex_val(number).ok_or(Error::InvalidHex(number))?;
        n = (n << 4) + hex;
    }
    Ok(n)
}
