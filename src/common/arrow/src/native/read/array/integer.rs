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

use std::convert::TryInto;
use std::io::Cursor;
use std::marker::PhantomData;

use crate::arrow::array::Array;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Result;
use crate::native::compression::integer::decompress_integer;
use crate::native::compression::integer::IntegerType;
use crate::native::nested::InitNested;
use crate::native::nested::NestedState;
use crate::native::read::read_basic::*;
use crate::native::read::BufReader;
use crate::native::read::NativeReadBuf;
use crate::native::read::PageIterator;
use crate::native::PageMeta;

#[derive(Debug)]
pub struct IntegerNestedIter<I, T>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: IntegerType,
{
    iter: I,
    data_type: DataType,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
    _phantom: PhantomData<T>,
}

impl<I, T> IntegerNestedIter<I, T>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: IntegerType,
{
    pub fn new(iter: I, data_type: DataType, init: Vec<InitNested>) -> Self {
        Self {
            iter,
            data_type,
            init,
            scratch: vec![],
            _phantom: PhantomData,
        }
    }
}

impl<I, T> IntegerNestedIter<I, T>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: IntegerType,
    Vec<u8>: TryInto<T::Bytes>,
{
    fn deserialize(
        &mut self,
        num_values: u64,
        buffer: Vec<u8>,
    ) -> Result<(NestedState, Box<dyn Array>)> {
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let (nested, validity) = read_nested(&mut reader, &self.init, num_values as usize)?;
        let length = num_values as usize;

        let mut values = Vec::with_capacity(length);
        decompress_integer(&mut reader, length, &mut values, &mut self.scratch)?;
        assert_eq!(values.len(), length);

        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let array = PrimitiveArray::<T>::try_new(self.data_type.clone(), values.into(), validity)?;

        Ok((nested, Box::new(array) as Box<dyn Array>))
    }
}

impl<I, T> Iterator for IntegerNestedIter<I, T>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: IntegerType,
    Vec<u8>: TryInto<T::Bytes>,
{
    type Item = Result<(NestedState, Box<dyn Array>)>;

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self.iter.nth(n) {
            Some(Ok((num_values, buffer))) => Some(self.deserialize(num_values, buffer)),
            Some(Err(err)) => Some(Result::Err(err)),
            None => None,
        }
    }

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok((num_values, buffer))) => Some(self.deserialize(num_values, buffer)),
            Some(Err(err)) => Some(Result::Err(err)),
            None => None,
        }
    }
}

pub fn read_nested_integer<T: IntegerType, R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    init: Vec<InitNested>,
    page_metas: Vec<PageMeta>,
) -> Result<Vec<(NestedState, Box<dyn Array>)>> {
    let mut scratch = vec![];
    let mut results = Vec::with_capacity(page_metas.len());
    for page_meta in page_metas {
        let num_values = page_meta.num_values as usize;
        let (nested, validity) = read_nested(reader, &init, num_values)?;

        let mut values = Vec::with_capacity(num_values);
        decompress_integer(reader, num_values, &mut values, &mut scratch)?;

        let array = PrimitiveArray::<T>::try_new(data_type.clone(), values.into(), validity)?;
        results.push((nested, Box::new(array) as Box<dyn Array>));
    }
    Ok(results)
}
