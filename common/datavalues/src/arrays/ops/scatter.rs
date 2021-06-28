// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::arrays::get_list_builder;
use crate::arrays::BooleanArrayBuilder;
use crate::arrays::DataArray;
use crate::arrays::PrimitiveArrayBuilder;
use crate::arrays::Utf8ArrayBuilder;
use crate::prelude::*;
use crate::utils::get_iter_capacity;
use crate::*;

pub trait ArrayScatter {
    unsafe fn scatter_unchecked(
        &self,
        _indices: &mut dyn Iterator<Item = u32>,
        _scattered_size: usize,
    ) -> Result<Vec<Self>>
    where
        Self: std::marker::Sized,
    {
        unimplemented!()
    }
}

impl<T> ArrayScatter for DataArray<T>
where T: DFNumericType
{
    unsafe fn scatter_unchecked(
        &self,
        indices: &mut dyn Iterator<Item = u32>,
        scattered_size: usize,
    ) -> Result<Vec<Self>>
    where
        Self: std::marker::Sized,
    {
        let array = self.downcast_ref();
        let mut builders = Vec::with_capacity(scattered_size);

        for _i in 0..scattered_size {
            builders.push(PrimitiveArrayBuilder::<T>::new(self.len()));
        }

        match self.null_count() {
            0 => {
                indices.zip(0..self.len()).for_each(|(row, index)| {
                    builders[index].append_value(array.value(row as usize));
                });
            }
            _ => {
                indices.zip(0..self.len()).for_each(|(row, index)| {
                    if self.is_null(row as usize) {
                        builders[index].append_null();
                    } else {
                        builders[index].append_value(array.value(row as usize));
                    }
                });
            }
        }

        Ok(builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect())
    }
}

impl ArrayScatter for DFUtf8Array {
    unsafe fn scatter_unchecked(
        &self,
        indices: &mut dyn Iterator<Item = u32>,
        scattered_size: usize,
    ) -> Result<Vec<Self>>
    where
        Self: std::marker::Sized,
    {
        let array = self.downcast_ref();
        let mut builders = Vec::with_capacity(scattered_size);

        for _i in 0..scattered_size {
            builders.push(Utf8ArrayBuilder::new(
                self.len(),
                self.get_array_memory_size(),
            ));
        }

        match self.null_count() {
            0 => {
                indices.zip(0..self.len()).for_each(|(row, index)| {
                    builders[index].append_value(array.value(row as usize));
                });
            }
            _ => {
                indices.zip(0..self.len()).for_each(|(row, index)| {
                    if self.is_null(row as usize) {
                        builders[index].append_null();
                    } else {
                        builders[index].append_value(array.value(row as usize));
                    }
                });
            }
        }

        Ok(builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect())
    }
}

impl ArrayScatter for DFBooleanArray {
    unsafe fn scatter_unchecked(
        &self,
        indices: &mut dyn Iterator<Item = u32>,
        scattered_size: usize,
    ) -> Result<Vec<Self>>
    where
        Self: std::marker::Sized,
    {
        let array = self.downcast_ref();
        let mut builders = Vec::with_capacity(scattered_size);

        for _i in 0..scattered_size {
            builders.push(BooleanArrayBuilder::new(self.len()));
        }

        match self.null_count() {
            0 => {
                indices.zip(0..self.len()).for_each(|(row, index)| {
                    builders[index].append_value(array.value(row as usize));
                });
            }
            _ => {
                indices.zip(0..self.len()).for_each(|(row, index)| {
                    if self.is_null(row as usize) {
                        builders[index].append_null();
                    } else {
                        builders[index].append_value(array.value(row as usize));
                    }
                });
            }
        }

        Ok(builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect())
    }
}

impl ArrayScatter for DFListArray {
    unsafe fn scatter_unchecked(
        &self,
        indices: &mut dyn Iterator<Item = u32>,
        scattered_size: usize,
    ) -> Result<Vec<Self>>
    where
        Self: std::marker::Sized,
    {
        let mut builders = Vec::with_capacity(scattered_size);

        let capacity = get_iter_capacity(&indices);
        for _i in 0..scattered_size {
            let builder = get_list_builder(&self.sub_data_type(), capacity * 5, capacity);

            builders.push(builder);
        }

        let taker = self.take_rand();

        match self.null_count() {
            0 => {
                indices.zip(0..self.len()).for_each(|(row, index)| {
                    builders[index].append_series(&taker.get_unchecked(row as usize));
                });
            }
            _ => {
                indices.zip(0..self.len()).for_each(|(row, index)| {
                    if self.is_null(row as usize) {
                        builders[index].append_null();
                    } else {
                        builders[index].append_series(&taker.get_unchecked(row as usize));
                    }
                });
            }
        }

        Ok(builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect())
    }
}

impl ArrayScatter for DFNullArray {}
impl ArrayScatter for DFBinaryArray {}
impl ArrayScatter for DFStructArray {}
