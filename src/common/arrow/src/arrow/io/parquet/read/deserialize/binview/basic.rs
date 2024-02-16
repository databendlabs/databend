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

use std::cell::Cell;
use std::collections::VecDeque;

use parquet2::page::DataPage;
use parquet2::page::DictPage;

use super::super::binary::basic::deserialize_plain;
use super::super::binary::basic::BinaryDict;
use super::super::binary::basic::BinaryState;
use super::super::utils;
use super::super::utils::extend_from_decoder;
use super::super::utils::next;
use super::super::utils::DecodedState;
use super::super::utils::MaybeNext;
use crate::arrow::array::Array;
use crate::arrow::array::BinaryViewArray;
use crate::arrow::array::MutableBinaryViewArray;
use crate::arrow::array::Utf8ViewArray;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::bitmap::MutableBitmap;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::PhysicalType;
use crate::arrow::error::Result;
use crate::arrow::io::parquet::read::deserialize::binary::basic::build_binary_state;
use crate::arrow::io::parquet::read::Pages;
use crate::ArrayRef;

type DecodedStateTuple = (MutableBinaryViewArray<[u8]>, MutableBitmap);

#[derive(Default)]
struct BinViewDecoder {
    check_utf8: Cell<bool>,
}

impl DecodedState for DecodedStateTuple {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<'a> utils::Decoder<'a> for BinViewDecoder {
    type State = BinaryState<'a>;
    type Dict = BinaryDict;
    type DecodedState = DecodedStateTuple;

    fn build_state(&self, page: &'a DataPage, dict: Option<&'a Self::Dict>) -> Result<Self::State> {
        build_binary_state(page, dict)
    }

    fn with_capacity(&self, capacity: usize) -> Self::DecodedState {
        (
            MutableBinaryViewArray::with_capacity(capacity),
            MutableBitmap::with_capacity(capacity),
        )
    }

    fn extend_from_state(
        &self,
        state: &mut Self::State,
        decoded: &mut Self::DecodedState,
        additional: usize,
    ) {
        let (values, validity) = decoded;
        let mut validate_utf8 = self.check_utf8.take();

        match state {
            BinaryState::Optional(page_validity, page_values) => extend_from_decoder(
                validity,
                page_validity,
                Some(additional),
                values,
                page_values,
            ),
            BinaryState::Required(page) => {
                for x in page.values.by_ref().take(additional) {
                    values.push_value_ignore_validity(x)
                }
            }
            BinaryState::Delta(page) => {
                for value in page {
                    values.push_value_ignore_validity(value)
                }
            }
            BinaryState::OptionalDelta(page_validity, page_values) => {
                extend_from_decoder(
                    validity,
                    page_validity,
                    Some(additional),
                    values,
                    page_values,
                );
            }
            BinaryState::FilteredRequired(page) => {
                for x in page.values.by_ref().take(additional) {
                    values.push_value_ignore_validity(x)
                }
            }
            BinaryState::FilteredDelta(page) => {
                for x in page.values.by_ref().take(additional) {
                    values.push_value_ignore_validity(x)
                }
            }
            BinaryState::OptionalDictionary(page_validity, page_values) => {
                // Already done on the dict.
                validate_utf8 = false;
                let page_dict = &page_values.dict;
                utils::extend_from_decoder(
                    validity,
                    page_validity,
                    Some(additional),
                    values,
                    &mut page_values
                        .values
                        .by_ref()
                        .map(|index| page_dict[index.unwrap() as usize].as_ref()),
                )
            }
            BinaryState::RequiredDictionary(page) => {
                // Already done on the dict.
                validate_utf8 = false;
                let page_dict = &page.dict;

                for x in page
                    .values
                    .by_ref()
                    .map(|index| page_dict[index.unwrap() as usize].as_ref())
                    .take(additional)
                {
                    values.push_value_ignore_validity::<&[u8]>(x)
                }
            }
            BinaryState::FilteredOptional(page_validity, page_values) => {
                extend_from_decoder(
                    validity,
                    page_validity,
                    Some(additional),
                    values,
                    page_values.by_ref(),
                );
            }
            BinaryState::FilteredOptionalDelta(page_validity, page_values) => {
                extend_from_decoder(
                    validity,
                    page_validity,
                    Some(additional),
                    values,
                    page_values.by_ref(),
                );
            }
            BinaryState::FilteredRequiredDictionary(page) => {
                // TODO! directly set the dict as buffers and only insert the proper views.
                // This will save a lot of memory.
                // Already done on the dict.
                validate_utf8 = false;
                let page_dict = &page.dict;
                for x in page
                    .values
                    .by_ref()
                    .map(|index| page_dict[index.unwrap() as usize].as_ref())
                    .take(additional)
                {
                    values.push_value_ignore_validity::<&[u8]>(x)
                }
            }
            BinaryState::FilteredOptionalDictionary(page_validity, page_values) => {
                // Already done on the dict.
                validate_utf8 = false;
                // TODO! directly set the dict as buffers and only insert the proper views.
                // This will save a lot of memory.
                let page_dict = &page_values.dict;
                extend_from_decoder(
                    validity,
                    page_validity,
                    Some(additional),
                    values,
                    &mut page_values
                        .values
                        .by_ref()
                        .map(|index| page_dict[index.unwrap() as usize].as_ref()),
                )
            }
        }

        if validate_utf8 {
            values.validate_utf8().expect("Not an utf8 string buffer.")
        }
    }

    fn deserialize_dict(&self, page: &DictPage) -> Self::Dict {
        deserialize_plain(&page.buffer, page.num_values)
    }
}

pub struct BinaryViewArrayIter<I: Pages> {
    iter: I,
    data_type: DataType,
    items: VecDeque<DecodedStateTuple>,
    dict: Option<BinaryDict>,
    chunk_size: Option<usize>,
    remaining: usize,
}
impl<I: Pages> BinaryViewArrayIter<I> {
    pub fn new(iter: I, data_type: DataType, chunk_size: Option<usize>, num_rows: usize) -> Self {
        Self {
            iter,
            data_type,
            items: VecDeque::new(),
            dict: None,
            chunk_size,
            remaining: num_rows,
        }
    }
}

impl<I: Pages> Iterator for BinaryViewArrayIter<I> {
    type Item = Result<ArrayRef>;

    fn next(&mut self) -> Option<Self::Item> {
        let decoder = BinViewDecoder::default();
        loop {
            let maybe_state = next(
                &mut self.iter,
                &mut self.items,
                &mut self.dict,
                &mut self.remaining,
                self.chunk_size,
                &decoder,
            );
            match maybe_state {
                MaybeNext::Some(Ok((values, validity))) => {
                    return Some(finish(&self.data_type, values, validity));
                }
                MaybeNext::Some(Err(e)) => return Some(Err(e)),
                MaybeNext::None => return None,
                MaybeNext::More => continue,
            }
        }
    }
}

pub(super) fn finish(
    data_type: &DataType,
    values: MutableBinaryViewArray<[u8]>,
    validity: MutableBitmap,
) -> Result<Box<dyn Array>> {
    let mut array: BinaryViewArray = values.into();
    let validity: Bitmap = validity.into();

    if validity.unset_bits() != validity.len() {
        array = array.with_validity(Some(validity))
    }

    match data_type.to_physical_type() {
        PhysicalType::BinaryView => Ok(BinaryViewArray::new_unchecked(
            data_type.clone(),
            array.views().clone(),
            array.data_buffers().clone(),
            array.validity().cloned(),
            array.total_bytes_len(),
            array.total_buffer_len(),
        )
        .boxed()),
        PhysicalType::Utf8View => {
            // Safety: we already checked utf8
            Ok(Utf8ViewArray::new_unchecked(
                data_type.clone(),
                array.views().clone(),
                array.data_buffers().clone(),
                array.validity().cloned(),
                array.total_bytes_len(),
                array.total_buffer_len(),
            )
            .boxed())
        }
        _ => unreachable!(),
    }
}
