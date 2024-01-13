// Copyright 2020-2022 Jorge C. Leitão
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

use parquet2::encoding::Encoding;
use parquet2::page::split_buffer;
use parquet2::page::DataPage;
use parquet2::page::DictPage;
use parquet2::schema::Repetition;

use super::super::nested_utils::*;
use super::super::utils;
use super::super::utils::MaybeNext;
use super::super::Pages;
use crate::arrow::array::BooleanArray;
use crate::arrow::bitmap::utils::BitmapIter;
use crate::arrow::bitmap::MutableBitmap;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Result;

// The state of a `DataPage` of `Boolean` parquet boolean type
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum State<'a> {
    Optional(BitmapIter<'a>),
    Required(BitmapIter<'a>),
}

impl<'a> State<'a> {
    pub fn len(&self) -> usize {
        match self {
            State::Optional(iter) => iter.size_hint().0,
            State::Required(iter) => iter.size_hint().0,
        }
    }
}

impl<'a> utils::PageState<'a> for State<'a> {
    fn len(&self) -> usize {
        self.len()
    }
}

#[derive(Default)]
struct BooleanDecoder {}

impl<'a> NestedDecoder<'a> for BooleanDecoder {
    type State = State<'a>;
    type Dictionary = ();
    type DecodedState = (MutableBitmap, MutableBitmap);

    fn build_state(
        &self,
        page: &'a DataPage,
        _: Option<&'a Self::Dictionary>,
    ) -> Result<Self::State> {
        let is_optional =
            page.descriptor.primitive_type.field_info.repetition == Repetition::Optional;
        let is_filtered = page.selected_rows().is_some();

        match (page.encoding(), is_optional, is_filtered) {
            (Encoding::Plain, true, false) => {
                let (_, _, values) = split_buffer(page)?;
                let values = BitmapIter::new(values, 0, values.len() * 8);

                Ok(State::Optional(values))
            }
            (Encoding::Plain, false, false) => {
                let (_, _, values) = split_buffer(page)?;
                let values = BitmapIter::new(values, 0, values.len() * 8);

                Ok(State::Required(values))
            }
            _ => Err(utils::not_implemented(page)),
        }
    }

    fn with_capacity(&self, capacity: usize) -> Self::DecodedState {
        (
            MutableBitmap::with_capacity(capacity),
            MutableBitmap::with_capacity(capacity),
        )
    }

    fn push_valid(&self, state: &mut State, decoded: &mut Self::DecodedState) -> Result<()> {
        let (values, validity) = decoded;
        match state {
            State::Optional(page_values) => {
                let value = page_values.next().unwrap_or_default();
                values.push(value);
                validity.push(true);
            }
            State::Required(page_values) => {
                let value = page_values.next().unwrap_or_default();
                values.push(value);
            }
        }
        Ok(())
    }

    fn push_null(&self, decoded: &mut Self::DecodedState) {
        let (values, validity) = decoded;
        values.push(false);
        validity.push(false);
    }

    fn deserialize_dict(&self, _: &DictPage) -> Self::Dictionary {}
}

/// An iterator adapter over [`Pages`] assumed to be encoded as boolean arrays
#[derive(Debug)]
pub struct NestedIter<I: Pages> {
    iter: I,
    init: Vec<InitNested>,
    items: VecDeque<(NestedState, (MutableBitmap, MutableBitmap))>,
    remaining: usize,
    chunk_size: Option<usize>,
}

impl<I: Pages> NestedIter<I> {
    pub fn new(iter: I, init: Vec<InitNested>, num_rows: usize, chunk_size: Option<usize>) -> Self {
        Self {
            iter,
            init,
            items: VecDeque::new(),
            remaining: num_rows,
            chunk_size,
        }
    }
}

fn finish(data_type: &DataType, values: MutableBitmap, validity: MutableBitmap) -> BooleanArray {
    BooleanArray::new(data_type.clone(), values.into(), validity.into())
}

impl<I: Pages> Iterator for NestedIter<I> {
    type Item = Result<(NestedState, BooleanArray)>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_state = next(
            &mut self.iter,
            &mut self.items,
            &mut None,
            &mut self.remaining,
            &self.init,
            self.chunk_size,
            &BooleanDecoder::default(),
        );
        match maybe_state {
            MaybeNext::Some(Ok((nested, (values, validity)))) => {
                Some(Ok((nested, finish(&DataType::Boolean, values, validity))))
            }
            MaybeNext::Some(Err(e)) => Some(Err(e)),
            MaybeNext::None => None,
            MaybeNext::More => self.next(),
        }
    }
}
