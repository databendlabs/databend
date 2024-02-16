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

use parquet2::page::DataPage;
use parquet2::page::DictPage;

use crate::arrow::array::MutableBinaryViewArray;
use crate::arrow::bitmap::MutableBitmap;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::read::deserialize::binary::basic::deserialize_plain;
use crate::arrow::io::parquet::read::deserialize::binary::basic::BinaryDict;
use crate::arrow::io::parquet::read::deserialize::binary::nested::build_nested_state;
use crate::arrow::io::parquet::read::deserialize::binary::nested::BinaryNestedState;
use crate::arrow::io::parquet::read::deserialize::binview::basic::finish;
use crate::arrow::io::parquet::read::deserialize::nested_utils::next;
use crate::arrow::io::parquet::read::deserialize::nested_utils::NestedDecoder;
use crate::arrow::io::parquet::read::deserialize::utils::MaybeNext;
use crate::arrow::io::parquet::read::InitNested;
use crate::arrow::io::parquet::read::NestedState;
use crate::arrow::io::parquet::read::Pages;
use crate::ArrayRef;

#[derive(Debug, Default)]
struct BinViewDecoder {}

type DecodedStateTuple = (MutableBinaryViewArray<[u8]>, MutableBitmap);

impl<'a> NestedDecoder<'a> for BinViewDecoder {
    type State = BinaryNestedState<'a>;
    type Dictionary = BinaryDict;
    type DecodedState = DecodedStateTuple;

    fn build_state(
        &self,
        page: &'a DataPage,
        dict: Option<&'a Self::Dictionary>,
    ) -> Result<Self::State> {
        build_nested_state(page, dict)
    }

    fn with_capacity(&self, capacity: usize) -> Self::DecodedState {
        (
            MutableBinaryViewArray::with_capacity(capacity),
            MutableBitmap::with_capacity(capacity),
        )
    }

    fn push_valid(&self, state: &mut Self::State, decoded: &mut Self::DecodedState) -> Result<()> {
        let (values, validity) = decoded;
        match state {
            BinaryNestedState::Optional(page) => {
                let value = page.next().unwrap_or_default();
                values.push_value_ignore_validity(value);
                validity.push(true);
            }
            BinaryNestedState::Required(page) => {
                let value = page.next().unwrap_or_default();
                values.push_value_ignore_validity(value);
            }
            BinaryNestedState::RequiredDictionary(page) => {
                let dict_values = &page.dict;
                let item = page
                    .values
                    .next()
                    .map(|index| dict_values[index.unwrap() as usize].as_ref())
                    .unwrap_or_default();
                values.push_value_ignore_validity::<&[u8]>(item);
            }
            BinaryNestedState::OptionalDictionary(page) => {
                let dict_values = &page.dict;
                let item = page
                    .values
                    .next()
                    .map(|index| dict_values[index.unwrap() as usize].as_ref())
                    .unwrap_or_default();
                values.push_value_ignore_validity::<&[u8]>(item);
                validity.push(true);
            }
        }
        Ok(())
    }

    fn push_null(&self, decoded: &mut Self::DecodedState) {
        let (values, validity) = decoded;
        values.push_null();
        validity.push(false);
    }

    fn deserialize_dict(&self, page: &DictPage) -> Self::Dictionary {
        deserialize_plain(&page.buffer, page.num_values)
    }
}

pub struct NestedIter<I: Pages> {
    iter: I,
    data_type: DataType,
    init: Vec<InitNested>,
    items: VecDeque<(NestedState, DecodedStateTuple)>,
    dict: Option<BinaryDict>,
    chunk_size: Option<usize>,
    remaining: usize,
}

impl<I: Pages> NestedIter<I> {
    pub fn new(
        iter: I,
        init: Vec<InitNested>,
        data_type: DataType,
        num_rows: usize,
        chunk_size: Option<usize>,
    ) -> Self {
        Self {
            iter,
            data_type,
            init,
            items: VecDeque::new(),
            dict: None,
            chunk_size,
            remaining: num_rows,
        }
    }
}

impl<I: Pages> Iterator for NestedIter<I> {
    type Item = Result<(NestedState, ArrayRef)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let maybe_state = next(
                &mut self.iter,
                &mut self.items,
                &mut self.dict,
                &mut self.remaining,
                &self.init,
                self.chunk_size,
                &BinViewDecoder::default(),
            );
            match maybe_state {
                MaybeNext::Some(Ok((nested, decoded))) => {
                    return Some(
                        finish(&self.data_type, decoded.0, decoded.1).map(|array| (nested, array)),
                    );
                }
                MaybeNext::Some(Err(e)) => return Some(Err(e)),
                MaybeNext::None => return None,
                MaybeNext::More => continue, /* Using continue in a loop instead of calling next helps prevent stack overflow. */
            }
        }
    }
}
