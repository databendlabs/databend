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
use common_datavalues::Column;
use common_datavalues::StringColumn;
use common_exception::Result;

use super::String2StringFunction;
use super::StringOperator;

#[derive(Clone, Default)]
pub struct Soundex {
    buf: String,
}

impl Soundex {
    #[inline(always)]
    fn number_map(i: char) -> Option<u8> {
        match i.to_ascii_lowercase() {
            'b' | 'f' | 'p' | 'v' => Some(b'1'),
            'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => Some(b'2'),
            'd' | 't' => Some(b'3'),
            'l' => Some(b'4'),
            'm' | 'n' => Some(b'5'),
            'r' => Some(b'6'),
            _ => Some(b'0'),
        }
    }

    #[inline(always)]
    fn is_drop(c: char) -> bool {
        matches!(
            c.to_ascii_lowercase(),
            'a' | 'e' | 'i' | 'o' | 'u' | 'y' | 'h' | 'w'
        )
    }

    // https://github.com/mysql/mysql-server/blob/3290a66c89eb1625a7058e0ef732432b6952b435/sql/item_strfunc.cc#L1919
    #[inline(always)]
    fn is_uni_alphabetic(c: char) -> bool {
        ('a'..='z').contains(&c) || ('A'..='Z').contains(&c) || c as i32 >= 0xC0
    }
}

impl StringOperator for Soundex {
    #[inline]
    fn try_apply<'a>(&'a mut self, data: &'a [u8], mut buffer: &mut [u8]) -> Result<usize> {
        let mut last = None;
        let mut count = 0;

        self.buf.clear();

        for ch in String::from_utf8_lossy(data).chars() {
            let score = Self::number_map(ch);
            if last.is_none() {
                if !Self::is_uni_alphabetic(ch) {
                    continue;
                }

                last = score;
                self.buf.push(ch.to_ascii_uppercase());
            } else {
                if !ch.is_ascii_alphabetic()
                    || Self::is_drop(ch)
                    || score.is_none()
                    || score == last
                {
                    continue;
                }

                last = score;
                self.buf.push(score.unwrap() as char);
            }

            count += 1;
        }

        // add '0'
        if !self.buf.is_empty() && count < 4 {
            self.buf.extend(vec!['0'; 4 - count])
        }
        let bytes = self.buf.as_bytes();
        buffer.put_slice(bytes);
        Ok(bytes.len())
    }

    fn estimate_bytes(&self, array: &StringColumn) -> usize {
        usize::max(array.values().len(), 4 * array.len())
    }
}

pub type SoundexFunction = String2StringFunction<Soundex>;
