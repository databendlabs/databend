// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::*;
use common_exception::Result;
use common_io::prelude::BinaryRead;

use super::ArrayDeserializer;
use crate::prelude::*;
use crate::utils::get_iter_capacity;
use crate::DFUtf8Array;
use crate::Utf8Type;

pub struct Utf8ArrayBuilder {
    pub builder: MutableUtf8Array<i64>,
}

impl Utf8ArrayBuilder {
    /// Create a new UtfArrayBuilder
    ///
    /// # Arguments
    ///
    /// * `capacity` - Number of string elements in the final array.
    pub fn with_capacity(bytes_capacity: usize) -> Self {
        Utf8ArrayBuilder {
            builder: MutableUtf8Array::with_capacity(bytes_capacity),
        }
    }

    /// Appends a value of type `T` into the builder
    #[inline]
    pub fn append_value<S: AsRef<str>>(&mut self, v: S) {
        self.builder.push(Some(v))
    }

    /// Appends a null slot into the builder
    #[inline]
    pub fn append_null(&mut self) {
        self.builder.push_null();
    }

    #[inline]
    pub fn append_option<S: AsRef<str>>(&mut self, opt: Option<S>) {
        match opt {
            Some(s) => self.append_value(s.as_ref()),
            None => self.append_null(),
        }
    }

    pub fn finish(&mut self) -> DFUtf8Array {
        let array = self.builder.as_arc();
        array.into()
    }
}

impl ArrayDeserializer for Utf8ArrayBuilder {
    fn de(&mut self, reader: &mut &[u8]) -> Result<()> {
        let value: String = reader.read_string()?;
        self.append_value(value);
        Ok(())
    }

    fn de_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        for row in 0..rows {
            let mut reader = &reader[step * row..];
            let value: String = reader.read_string()?;
            self.append_value(&value);
        }
        Ok(())
    }

    fn finish_to_series(&mut self) -> Series {
        self.finish().into_series()
    }
}

impl<S> NewDataArray<Utf8Type, S> for DFUtf8Array
where S: AsRef<str>
{
    fn new_from_slice(v: &[S]) -> Self {
        let values_size = v.iter().fold(0, |acc, s| acc + s.as_ref().len());
        let mut builder = Utf8ArrayBuilder::with_capacity(values_size);
        v.iter().for_each(|val| {
            builder.append_value(val.as_ref());
        });

        builder.finish()
    }

    fn new_from_opt_slice(opt_v: &[Option<S>]) -> Self {
        let values_size = opt_v.iter().fold(0, |acc, s| match s {
            Some(s) => acc + s.as_ref().len(),
            None => acc,
        });
        let mut builder = Utf8ArrayBuilder::with_capacity(values_size);
        opt_v.iter().for_each(|opt| match opt {
            Some(v) => builder.append_value(v.as_ref()),
            None => builder.append_null(),
        });
        builder.finish()
    }

    fn new_from_opt_iter(it: impl Iterator<Item = Option<S>>) -> Self {
        let cap = get_iter_capacity(&it);
        let mut builder = Utf8ArrayBuilder::with_capacity(cap * 5);
        it.for_each(|opt| builder.append_option(opt));
        builder.finish()
    }

    /// Create a new DataArray from an iterator.
    fn new_from_iter(it: impl Iterator<Item = S>) -> Self {
        let cap = get_iter_capacity(&it);
        let mut builder = Utf8ArrayBuilder::with_capacity(cap * 5);
        it.for_each(|v| builder.append_value(v));
        builder.finish()
    }
}
