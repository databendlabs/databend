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

use std::collections::VecDeque;

use parquet2::page::DictPage;

use crate::arrow::array::Array;
use crate::arrow::array::DictionaryArray;
use crate::arrow::array::DictionaryKey;
use crate::arrow::array::MutableBinaryViewArray;
use crate::arrow::bitmap::MutableBitmap;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::PhysicalType;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::read::deserialize::binary::SizedBinaryIter;
use crate::arrow::io::parquet::read::deserialize::dictionary::nested_next_dict;
use crate::arrow::io::parquet::read::deserialize::dictionary::next_dict;
use crate::arrow::io::parquet::read::deserialize::utils::MaybeNext;
use crate::arrow::io::parquet::read::InitNested;
use crate::arrow::io::parquet::read::NestedState;
use crate::arrow::io::parquet::read::Pages;

// An iterator adapter over [`PagesIter`] assumed to be encoded as parquet's dictionary-encoded binary representation
#[derive(Debug)]
pub struct DictIter<K, I>
where
    I: Pages,
    K: DictionaryKey,
{
    iter: I,
    data_type: DataType,
    values: Option<Box<dyn Array>>,
    items: VecDeque<(Vec<K>, MutableBitmap)>,
    remaining: usize,
    chunk_size: Option<usize>,
}

impl<K, I> DictIter<K, I>
where
    K: DictionaryKey,
    I: Pages,
{
    pub fn new(iter: I, data_type: DataType, num_rows: usize, chunk_size: Option<usize>) -> Self {
        Self {
            iter,
            data_type,
            values: None,
            items: VecDeque::new(),
            remaining: num_rows,
            chunk_size,
        }
    }
}

fn read_dict(data_type: DataType, dict: &DictPage) -> Box<dyn Array> {
    let data_type = match data_type {
        DataType::Dictionary(_, values, _) => *values,
        _ => data_type,
    };

    let values = SizedBinaryIter::new(&dict.buffer, dict.num_values);

    let mut data = MutableBinaryViewArray::<[u8]>::with_capacity(dict.num_values);
    for item in values {
        data.push_value(item)
    }

    match data_type.to_physical_type() {
        PhysicalType::Utf8View => data.freeze().to_utf8view().unwrap().boxed(),
        PhysicalType::BinaryView => data.freeze().boxed(),
        _ => unreachable!(),
    }
}

impl<K, I> Iterator for DictIter<K, I>
where
    I: Pages,
    K: DictionaryKey,
{
    type Item = Result<DictionaryArray<K>>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next_dict(
            &mut self.iter,
            &mut self.items,
            &mut self.values,
            self.data_type.clone(),
            &mut self.remaining,
            self.chunk_size,
            |dict| read_dict(self.data_type.clone(), dict),
        );
        match maybe_state {
            MaybeNext::Some(Ok(dict)) => Some(Ok(dict)),
            MaybeNext::Some(Err(e)) => Some(Err(e)),
            MaybeNext::None => None,
            MaybeNext::More => self.next(),
        }
    }
}

/// An iterator adapter that converts [`DataPages`] into an [`Iterator`] of [`DictionaryArray`]
#[derive(Debug)]
pub struct NestedDictIter<K, I>
where
    I: Pages,
    K: DictionaryKey,
{
    iter: I,
    init: Vec<InitNested>,
    data_type: DataType,
    values: Option<Box<dyn Array>>,
    items: VecDeque<(NestedState, (Vec<K>, MutableBitmap))>,
    remaining: usize,
    chunk_size: Option<usize>,
}

impl<K, I> NestedDictIter<K, I>
where
    I: Pages,
    K: DictionaryKey,
{
    pub fn new(
        iter: I,
        init: Vec<InitNested>,
        data_type: DataType,
        num_rows: usize,
        chunk_size: Option<usize>,
    ) -> Self {
        Self {
            iter,
            init,
            data_type,
            values: None,
            items: VecDeque::new(),
            remaining: num_rows,
            chunk_size,
        }
    }
}

impl<K, I> Iterator for NestedDictIter<K, I>
where
    I: Pages,
    K: DictionaryKey,
{
    type Item = Result<(NestedState, DictionaryArray<K>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let maybe_state = nested_next_dict(
                &mut self.iter,
                &mut self.items,
                &mut self.remaining,
                &self.init,
                &mut self.values,
                self.data_type.clone(),
                self.chunk_size,
                |dict| read_dict(self.data_type.clone(), dict),
            );
            match maybe_state {
                MaybeNext::Some(Ok(dict)) => return Some(Ok(dict)),
                MaybeNext::Some(Err(e)) => return Some(Err(e)),
                MaybeNext::None => return None,
                MaybeNext::More => continue,
            }
        }
    }
}
