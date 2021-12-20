// Copyright 2021 Datafuse Labs.
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

use bytes::BufMut;

use super::String2StringFunction;
use super::StringOperator;

#[derive(Clone, Default)]
pub struct Soundex {}

impl Soundex {
    #[inline(always)]
    fn number_map(i: u8) -> Option<u8> {
        match i.to_ascii_lowercase() {
            b'b' | b'f' | b'p' | b'v' => Some(b'1'),
            b'c' | b'g' | b'j' | b'k' | b'q' | b's' | b'x' | b'z' => Some(b'2'),
            b'd' | b't' => Some(b'3'),
            b'l' => Some(b'4'),
            b'm' | b'n' => Some(b'5'),
            b'r' => Some(b'6'),
            _ => Some(b'0'),
        }
    }

    #[inline(always)]
    fn is_drop(c: u8) -> bool {
        matches!(
            c.to_ascii_lowercase(),
            b'a' | b'e' | b'i' | b'o' | b'u' | b'y' | b'h' | b'w'
        )
    }

    // https://github.com/mysql/mysql-server/blob/3290a66c89eb1625a7058e0ef732432b6952b435/sql/item_strfunc.cc#L1919
    #[inline(always)]
    fn is_uni_alphabetic(c: u8) -> bool {
        (b'a'..=b'z').contains(&c) || (b'A'..=b'Z').contains(&c) || c as i32 >= 0xC0
    }
}

impl StringOperator for Soundex {
    #[inline]
    fn apply_with_no_null<'a>(&'a mut self, data: &'a [u8], mut buffer: &mut [u8]) -> usize {
        let mut last = None;
        let mut count = 0;

        for b in data {
            let score = Self::number_map(*b);
            if last.is_none() {
                if !Self::is_uni_alphabetic(*b) {
                    continue;
                }

                last = score;
                buffer.put_slice(&[b.to_ascii_uppercase()]);
            } else {
                if !b.is_ascii_alphabetic() || Self::is_drop(*b) || score.is_none() || score == last
                {
                    continue;
                }

                last = score;
                buffer.put_slice(&[score.unwrap()]);
            }

            count += 1;
        }

        // add '0'
        if count != 0 && count < 4 {
            for _ in 0..4 - count {
                buffer.put_slice(&[b'0']);
            }
        }

        count
    }
}

pub type SoundexFunction = String2StringFunction<Soundex>;
