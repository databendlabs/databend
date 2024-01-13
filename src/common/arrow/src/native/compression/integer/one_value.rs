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

use std::io::Read;
use std::io::Write;

use super::IntegerCompression;
use super::IntegerStats;
use super::IntegerType;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::error::Result;
use crate::native::compression::Compression;
use crate::native::write::WriteOptions;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OneValue {}

impl<T: IntegerType> IntegerCompression<T> for OneValue {
    fn compress(
        &self,
        array: &PrimitiveArray<T>,
        _stats: &IntegerStats<T>,
        _write_options: &WriteOptions,
        output: &mut Vec<u8>,
    ) -> Result<usize> {
        let size = output.len();
        self.encode_native(output, array)?;
        Ok(output.len() - size)
    }

    fn decompress(&self, input: &[u8], length: usize, output: &mut Vec<T>) -> Result<()> {
        self.decode_native(input, length, output)?;
        Ok(())
    }

    fn to_compression(&self) -> Compression {
        Compression::OneValue
    }

    fn compress_ratio(&self, stats: &IntegerStats<T>) -> f64 {
        if stats.unique_count <= 1 {
            stats.tuple_count as f64
        } else {
            0.0f64
        }
    }
}

impl OneValue {
    pub fn encode_native<T: IntegerType, W: Write>(
        &self,
        w: &mut W,
        array: &PrimitiveArray<T>,
    ) -> Result<()> {
        let val = array.iter().find(|v| v.is_some());
        let val = match val {
            Some(Some(v)) => *v,
            _ => T::default(),
        };
        let _ = w.write(val.to_le_bytes().as_ref())?;
        Ok(())
    }

    pub fn decode_native<T: IntegerType>(
        &self,
        mut input: &[u8],
        length: usize,
        array: &mut Vec<T>,
    ) -> Result<()> {
        let mut bs = vec![0u8; std::mem::size_of::<T>()];
        input.read_exact(&mut bs)?;
        let a: T::Bytes = match bs.as_slice().try_into() {
            Ok(a) => a,
            Err(_) => unreachable!(),
        };
        let val = T::from_le_bytes(a);

        array.reserve(length);
        array.extend(std::iter::repeat(val).take(length));
        Ok(())
    }
}
