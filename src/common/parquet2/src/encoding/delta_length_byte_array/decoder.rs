// Copyright [2021] [Jorge C Leitao]
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

use super::super::delta_bitpacked;
use crate::error::Error;

/// Decodes [Delta-length byte array](https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-length-byte-array-delta_length_byte_array--6)
/// lengths and values.
/// # Implementation
/// This struct does not allocate on the heap.
/// # Example
/// ```
/// use parquet2::encoding::delta_length_byte_array::Decoder;
///
/// let expected = &["Hello", "World"];
/// let expected_lengths = expected.iter().map(|x| x.len() as i32).collect::<Vec<_>>();
/// let expected_values = expected.join("");
/// let expected_values = expected_values.as_bytes();
/// let data = &[
///     128, 1, 4, 2, 10, 0, 0, 0, 0, 0, 72, 101, 108, 108, 111, 87, 111, 114, 108, 100,
/// ];
///
/// let mut decoder = Decoder::try_new(data).unwrap();
///
/// // Extract the lengths
/// let lengths = decoder.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
/// assert_eq!(lengths, expected_lengths);
///
/// // Extract the values. This _must_ be called after consuming all lengths by reference (see above).
/// let values = decoder.into_values();
///
/// assert_eq!(values, expected_values);
#[derive(Debug)]
pub struct Decoder<'a> {
    values: &'a [u8],
    lengths: delta_bitpacked::Decoder<'a>,
    total_length: u32,
}

impl<'a> Decoder<'a> {
    pub fn try_new(values: &'a [u8]) -> Result<Self, Error> {
        let lengths = delta_bitpacked::Decoder::try_new(values)?;
        Ok(Self {
            values,
            lengths,
            total_length: 0,
        })
    }

    /// Consumes this decoder and returns the slice of concatenated values.
    /// # Panics
    /// This function panics if this iterator has not been fully consumed.
    pub fn into_values(self) -> &'a [u8] {
        assert_eq!(self.lengths.size_hint().0, 0);
        let start = self.lengths.consumed_bytes();
        &self.values[start..start + self.total_length as usize]
    }

    /// Returns the slice of concatenated values.
    /// # Panics
    /// This function panics if this iterator has not yet been fully consumed.
    pub fn values(&self) -> &'a [u8] {
        assert_eq!(self.lengths.size_hint().0, 0);
        let start = self.lengths.consumed_bytes();
        &self.values[start..start + self.total_length as usize]
    }
}

impl<'a> Iterator for Decoder<'a> {
    type Item = Result<i32, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.lengths.next();
        match result {
            Some(Ok(v)) => {
                self.total_length += v as u32;
                Some(Ok(v as i32))
            }
            Some(Err(error)) => Some(Err(error)),
            None => None,
        }
    }
}
