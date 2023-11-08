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

use std::hash::Hash;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;

use super::compress_integer;
use super::decompress_integer;
use super::IntegerCompression;
use super::IntegerStats;
use super::IntegerType;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::arrow::types::NativeType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Dict {}

impl<T: IntegerType> IntegerCompression<T> for Dict {
    fn compress(
        &self,
        array: &PrimitiveArray<T>,
        _stats: &IntegerStats<T>,
        write_options: &WriteOptions,
        output_buf: &mut Vec<u8>,
    ) -> Result<usize> {
        let start = output_buf.len();
        let mut encoder = DictEncoder::with_capacity(array.len());
        for val in array.iter() {
            match val {
                Some(val) => encoder.push(&RawNative { inner: *val }),
                None => {
                    if encoder.is_empty() {
                        encoder.push(&RawNative {
                            inner: T::default(),
                        });
                    } else {
                        encoder.push_last_index();
                    }
                }
            };
        }
        let indices = encoder.take_indices();

        // dict data use custom encoding
        let mut write_options = write_options.clone();
        write_options.forbidden_compressions.push(Compression::Dict);
        compress_integer(&indices, write_options, output_buf)?;

        let sets = encoder.get_sets();
        output_buf.extend_from_slice(&(sets.len() as u32).to_le_bytes());
        // data page use plain encoding
        for val in sets.iter() {
            let bs = val.inner.to_le_bytes();
            output_buf.extend_from_slice(bs.as_ref());
        }

        Ok(output_buf.len() - start)
    }

    fn decompress(&self, mut input: &[u8], length: usize, output: &mut Vec<T>) -> Result<()> {
        let mut indices: Vec<u32> = Vec::new();
        decompress_integer(&mut input, length, &mut indices, &mut vec![])?;

        let data_size = input.read_u32::<LittleEndian>()? as usize * std::mem::size_of::<T>();
        if input.len() < data_size {
            return Err(general_err!(
                "Invalid data size: {} less than {}",
                input.len(),
                data_size
            ));
        }

        let data: Vec<T> = input[0..data_size]
            .chunks(std::mem::size_of::<T>())
            .map(|chunk| match <T::Bytes>::try_from(chunk) {
                Ok(bs) => T::from_le_bytes(bs),
                Err(_e) => {
                    unreachable!()
                }
            })
            .collect();

        output.reserve(length);
        for i in indices.iter() {
            output.push(data[*i as usize]);
        }
        Ok(())
    }

    fn to_compression(&self) -> Compression {
        Compression::Dict
    }

    fn compress_ratio(&self, stats: &super::IntegerStats<T>) -> f64 {
        const MIN_DICT_RATIO: usize = 3;
        if stats.unique_count * MIN_DICT_RATIO >= stats.tuple_count {
            return 0.0f64;
        }

        let mut after_size = stats.unique_count * std::mem::size_of::<T>()
            + stats.tuple_count * (get_bits_needed(stats.unique_count as u64) / 8) as usize;
        // after_size += std::mem::size_of::<DynamicDictionaryStructure>() + 5;
        after_size += (stats.tuple_count) * 2 / 128;
        stats.total_bytes as f64 / after_size as f64
    }
}

/// Dictionary encoder.
/// The dictionary encoding builds a dictionary of values encountered in a given column.
/// The dictionary page is written first, before the data pages of the column chunk.
///
/// Dictionary page format: the entries in the dictionary - in dictionary order -
/// using the plain encoding.
///
/// Data page format: the bit width used to encode the entry ids stored as 1 byte
/// (max bit width = 32), followed by the values encoded using RLE/Bit packed described
/// above (with the given bit width).
pub struct DictEncoder<T: AsBytes> {
    interner: DictMap<T>,
    indices: Vec<u32>,
}

impl<T> DictEncoder<T>
where T: AsBytes + PartialEq + Clone
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            interner: DictMap::new(),
            indices: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, value: &T) {
        let key = self.interner.entry_key(value);
        self.indices.push(key);
    }

    pub fn push_last_index(&mut self) {
        self.indices.push(self.indices.last().cloned().unwrap());
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    pub fn get_sets(&self) -> &[T] {
        &self.interner.sets
    }

    pub fn take_indices(&mut self) -> PrimitiveArray<u32> {
        let indices = std::mem::take(&mut self.indices);
        PrimitiveArray::<u32>::from_vec(indices)
    }
}

use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;

use crate::general_err;
use crate::native::compression::get_bits_needed;
use crate::native::compression::Compression;
use crate::native::util::AsBytes;
use crate::native::write::WriteOptions;

const DEFAULT_DEDUP_CAPACITY: usize = 4096;

#[derive(Debug, Default)]
pub struct DictMap<T: AsBytes> {
    state: ahash::RandomState,
    dedup: HashMap<u32, (), ()>,
    sets: Vec<T>,
}

impl<T> DictMap<T>
where T: AsBytes + PartialEq + Clone
{
    pub fn new() -> Self {
        Self {
            state: Default::default(),
            dedup: HashMap::with_capacity_and_hasher(DEFAULT_DEDUP_CAPACITY, ()),
            sets: vec![],
        }
    }

    pub fn entry_key(&mut self, value: &T) -> u32 {
        let hash = self.state.hash_one(value.as_bytes());

        let entry = self
            .dedup
            .raw_entry_mut()
            .from_hash(hash, |index| value == &self.sets[*index as usize]);

        match entry {
            RawEntryMut::Occupied(entry) => *entry.into_key(),
            RawEntryMut::Vacant(entry) => {
                let key = self.sets.len() as u32;
                self.sets.push(value.clone());
                *entry
                    .insert_with_hasher(hash, key, (), |key| {
                        self.state.hash_one(self.sets[*key as usize].as_bytes())
                    })
                    .0
            }
        }
    }
}

#[repr(C)]
#[derive(Clone, PartialEq)]
pub struct RawNative<T: NativeType> {
    pub(crate) inner: T,
}

impl<T: NativeType> AsBytes for RawNative<T> {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const RawNative<T> as *const u8,
                std::mem::size_of::<T>(),
            )
        }
    }
}

#[cfg(test)]
mod tests {}
