// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::arrays::get_list_builder;
use crate::arrays::BinaryArrayBuilder;
use crate::arrays::BooleanArrayBuilder;
use crate::arrays::DataArray;
use crate::arrays::PrimitiveArrayBuilder;
use crate::arrays::Utf8ArrayBuilder;
use crate::prelude::*;
use crate::utils::get_iter_capacity;
use crate::*;

pub trait ArrayScatter: Debug {
    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.

    /// scatter_unchecked() partitions the input array into multiple arrays.
    /// _indices: an iterateor of vector whose length is the same as the array.
    /// The element of _indices indicates which group the corresponding row
    /// in the input array belongs to.
    /// _scattered_size: the number of partitions
    ///
    /// Example: if the input array has four rows [1, 2, 3, 4] and
    /// _indices = [0, 1, 0, 1] and _scatter_size = 2,
    /// then the output would be a vector of two arrays: [1, 3] and [2, 4].
    unsafe fn scatter_unchecked(
        &self,
        _indices: &mut dyn Iterator<Item = u64>,
        _scattered_size: usize,
    ) -> Result<Vec<Self>>
    where
        Self: std::marker::Sized,
    {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported apply scatter_unchecked operation for {:?}",
            self,
        )))
    }
}

impl<T> ArrayScatter for DataArray<T>
where T: DFNumericType
{
    unsafe fn scatter_unchecked(
        &self,
        indices: &mut dyn Iterator<Item = u64>,
        scattered_size: usize,
    ) -> Result<Vec<Self>>
    where
        Self: std::marker::Sized,
    {
        let array = self.downcast_ref();
        let mut builders = Vec::with_capacity(scattered_size);

        for _i in 0..scattered_size {
            builders.push(PrimitiveArrayBuilder::<T>::with_capacity(self.len()));
        }

        match self.null_count() {
            0 => {
                indices.zip(0..self.len()).for_each(|(index, row)| {
                    builders[index as usize].append_value(array.value(row));
                });
            }
            _ => {
                indices.zip(0..self.len()).for_each(|(index, row)| {
                    if self.is_null(row) {
                        builders[index as usize].append_null();
                    } else {
                        builders[index as usize].append_value(array.value(row));
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
        indices: &mut dyn Iterator<Item = u64>,
        scattered_size: usize,
    ) -> Result<Vec<Self>>
    where
        Self: std::marker::Sized,
    {
        let array = self.downcast_ref();
        let mut builders = Vec::with_capacity(scattered_size);

        for _i in 0..scattered_size {
            builders.push(Utf8ArrayBuilder::with_capacity(
                self.len(),
            ));
        }

        match self.null_count() {
            0 => {
                indices.zip(0..self.len()).for_each(|(index, row)| {
                    builders[index as usize].append_value(array.value(row));
                });
            }
            _ => {
                indices.zip(0..self.len()).for_each(|(index, row)| {
                    if self.is_null(row) {
                        builders[index as usize].append_null();
                    } else {
                        builders[index as usize].append_value(array.value(row));
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
        indices: &mut dyn Iterator<Item = u64>,
        scattered_size: usize,
    ) -> Result<Vec<Self>>
    where
        Self: std::marker::Sized,
    {
        let array = self.downcast_ref();
        let mut builders = Vec::with_capacity(scattered_size);

        for _i in 0..scattered_size {
            builders.push(BooleanArrayBuilder::with_capacity(self.len()));
        }

        match self.null_count() {
            0 => {
                indices.zip(0..self.len()).for_each(|(index, row)| {
                    builders[index as usize].append_value(array.value(row));
                });
            }
            _ => {
                indices.zip(0..self.len()).for_each(|(index, row)| {
                    if self.is_null(row) {
                        builders[index as usize].append_null();
                    } else {
                        builders[index as usize].append_value(array.value(row));
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
        indices: &mut dyn Iterator<Item = u64>,
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
                indices.zip(0..self.len()).for_each(|(index, row)| {
                    builders[index as usize].append_series(&taker.get_unchecked(row));
                });
            }
            _ => {
                indices.zip(0..self.len()).for_each(|(index, row)| {
                    if self.is_null(row) {
                        builders[index as usize].append_null();
                    } else {
                        builders[index as usize].append_series(&taker.get_unchecked(row));
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

impl ArrayScatter for DFBinaryArray {
    unsafe fn scatter_unchecked(
        &self,
        indices: &mut dyn Iterator<Item = u64>,
        scattered_size: usize,
    ) -> Result<Vec<Self>>
    where
        Self: std::marker::Sized,
    {
        let mut builders = Vec::with_capacity(scattered_size);
        let guess_scattered_len = ((self.len() as f64) * 1.1 / (scattered_size as f64)) as usize;
        for _i in 0..scattered_size {
            let builder = BinaryArrayBuilder::with_capacity(guess_scattered_len);
            builders.push(builder);
        }

        let binary_data = self.downcast_ref();
        for (i, index) in indices.enumerate() {
            if !self.is_null(i as usize) {
                builders[index as usize].append_value(binary_data.value(i as usize));
            } else {
                builders[index as usize].append_null();
            }
        }

        Ok(builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect())
    }
}

impl ArrayScatter for DFNullArray {}
impl ArrayScatter for DFStructArray {}
