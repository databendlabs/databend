// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::*;
use common_exception::Result;
use common_io::prelude::*;

use super::ArrayDeserializer;
use crate::arrays::DataArray;
use crate::prelude::*;
use crate::utils::get_iter_capacity;
use crate::utils::NoNull;

pub struct PrimitiveArrayBuilder<T>
where
    T: DFNumericType,
    T::Native: Default,
{
    builder: MutablePrimitiveArray<T::Native>,
}

impl<T> ArrayBuilder<T::Native, T> for PrimitiveArrayBuilder<T>
where
    T: DFNumericType,
    T::Native: Default,
{
    /// Appends a value of type `T` into the builder
    #[inline]
    fn append_value(&mut self, v: T::Native) {
        self.builder.push(Some(v))
    }

    /// Appends a null slot into the builder
    #[inline]
    fn append_null(&mut self) {
        self.builder.push_null();
    }

    fn finish(&mut self) -> DataArray<T> {
        let array = self.builder.as_arc();

        array.into()
    }
}

impl<T> ArrayDeserializer for PrimitiveArrayBuilder<T>
where
    T: DFNumericType,
    T::Native: Unmarshal<T::Native> + StatBuffer,
    DataArray<T>: IntoSeries,
{
    fn de(&mut self, reader: &mut &[u8]) -> Result<()> {
        let value: T::Native = reader.read_scalar()?;
        self.append_value(value);
        Ok(())
    }

    fn de_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: T::Native = reader.read_scalar()?;
            self.append_value(value);
        }
        Ok(())
    }

    fn finish_to_series(&mut self) -> Series {
        self.finish().into_series()
    }
}

impl<T> PrimitiveArrayBuilder<T>
where T: DFNumericType
{
    pub fn with_capacity(capacity: usize) -> Self {
        PrimitiveArrayBuilder {
            builder: MutablePrimitiveArray::<T::Native>::with_capacity(capacity),
        }
    }
}

impl<T> NewDataArray<T, T::Native> for DataArray<T>
where T: DFNumericType
{
    fn new_from_slice(v: &[T::Native]) -> Self {
        Self::new_from_iter(v.iter().copied())
    }

    fn new_from_opt_slice(opt_v: &[Option<T::Native>]) -> Self {
        Self::new_from_opt_iter(opt_v.iter().copied())
    }

    fn new_from_opt_iter(it: impl Iterator<Item = Option<T::Native>>) -> DataArray<T> {
        let mut builder = PrimitiveArrayBuilder::with_capacity(get_iter_capacity(&it));
        it.for_each(|opt| builder.append_option(opt));
        builder.finish()
    }

    /// Create a new DataArray from an iterator.
    fn new_from_iter(it: impl Iterator<Item = T::Native>) -> DataArray<T> {
        let ca: NoNull<DataArray<_>> = it.collect();
        ca.into_inner()
    }
}
