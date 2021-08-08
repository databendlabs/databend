// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::*;
use common_exception::Result;
use common_io::prelude::*;

use super::ArrayDeserializer;
use crate::prelude::*;
use crate::utils::get_iter_capacity;
use crate::BooleanType;
use crate::DFBooleanArray;

pub struct BooleanArrayBuilder {
    builder: MutableBooleanArray,
}

impl ArrayBuilder<bool, BooleanType> for BooleanArrayBuilder {
    /// Appends a value of type `T` into the builder
    #[inline]
    fn append_value(&mut self, v: bool) {
        self.builder.push(Some(v))
    }

    /// Appends a null slot into the builder
    #[inline]
    fn append_null(&mut self) {
        self.builder.push_null();
    }

    fn finish(&mut self) -> DFBooleanArray {
        let array = self.builder.as_arc();
        array.into()
    }
}

impl ArrayDeserializer for BooleanArrayBuilder {
    fn de(&mut self, reader: &mut &[u8]) -> Result<()> {
        let value: bool = reader.read_scalar()?;
        self.append_value(value);
        Ok(())
    }

    fn de_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: bool = reader.read_scalar()?;
            self.append_value(value);
        }

        Ok(())
    }

    fn finish_to_series(&mut self) -> Series {
        self.finish().into_series()
    }
}

impl BooleanArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        BooleanArrayBuilder {
            builder: MutableBooleanArray::with_capacity(capacity),
        }
    }
}

impl NewDataArray<BooleanType, bool> for DFBooleanArray {
    fn new_from_slice(v: &[bool]) -> Self {
        Self::new_from_iter(v.iter().copied())
    }

    fn new_from_opt_slice(opt_v: &[Option<bool>]) -> Self {
        Self::new_from_opt_iter(opt_v.iter().copied())
    }

    fn new_from_opt_iter(it: impl Iterator<Item = Option<bool>>) -> DFBooleanArray {
        let mut builder = BooleanArrayBuilder::with_capacity(get_iter_capacity(&it));
        it.for_each(|opt| builder.append_option(opt));
        builder.finish()
    }

    /// Create a new DataArray from an iterator.
    fn new_from_iter(it: impl Iterator<Item = bool>) -> DFBooleanArray {
        it.collect()
    }
}
