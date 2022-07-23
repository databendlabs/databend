// Copyright 2022 Datafuse Labs.
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

use std::ops::Range;

use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::trusted_len::TrustedLen;

use crate::property::Domain;
use crate::property::StringDomain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::util::buffer_into_mut;
use crate::values::Column;
use crate::values::Scalar;

pub struct StringType;

impl ValueType for StringType {
    type Scalar = Vec<u8>;
    type ScalarRef<'a> = &'a [u8];
    type Column = (Buffer<u8>, Buffer<u64>);
    type Domain = StringDomain;

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar.to_vec()
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        scalar
    }
}

impl ArgType for StringType {
    type ColumnIterator<'a> = StringIterator<'a>;
    type ColumnBuilder = StringColumnBuilder;

    fn data_type() -> DataType {
        DataType::String
    }

    fn try_downcast_scalar<'a>(scalar: &'a Scalar) -> Option<Self::ScalarRef<'a>> {
        scalar.as_string().map(Vec::as_slice)
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        col.as_string()
            .map(|(data, offsets)| (data.clone(), offsets.clone()))
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_string().map(StringDomain::clone)
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::String(scalar)
    }

    fn upcast_column((data, offsets): Self::Column) -> Column {
        Column::String { data, offsets }
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::String(domain)
    }

    fn full_domain(_: &GenericMap) -> Self::Domain {
        StringDomain {
            min: vec![],
            max: None,
        }
    }

    fn column_len<'a>((_, offsets): &'a Self::Column) -> usize {
        offsets.len() - 1
    }

    fn index_column<'a>(
        (data, offsets): &'a Self::Column,
        index: usize,
    ) -> Option<Self::ScalarRef<'a>> {
        if index + 1 < offsets.len() {
            Some(&data[(offsets[index] as usize)..(offsets[index + 1] as usize)])
        } else {
            None
        }
    }

    fn slice_column<'a>((data, offsets): &'a Self::Column, range: Range<usize>) -> Self::Column {
        let offsets = offsets
            .clone()
            .slice(range.start, range.end - range.start + 1);
        (data.clone(), offsets)
    }

    fn iter_column<'a>((data, offsets): &'a Self::Column) -> Self::ColumnIterator<'a> {
        StringIterator {
            data,
            offsets: offsets.windows(2),
        }
    }

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        StringColumnBuilder::with_capacity(0, capacity)
    }

    fn column_to_builder((data, offsets): Self::Column) -> Self::ColumnBuilder {
        StringColumnBuilder {
            data: buffer_into_mut(data),
            offsets: offsets.to_vec(),
        }
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.put_slice(item);
        builder.commit_row();
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.commit_row();
    }

    fn append_builder(builder: &mut Self::ColumnBuilder, other_builder: &Self::ColumnBuilder) {
        builder.append(other_builder)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }
}

pub struct StringIterator<'a> {
    data: &'a Buffer<u8>,
    offsets: std::slice::Windows<'a, u64>,
}

impl<'a> Iterator for StringIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.offsets
            .next()
            .map(|range| &self.data[(range[0] as usize)..(range[1] as usize)])
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.offsets.size_hint()
    }
}

unsafe impl<'a> TrustedLen for StringIterator<'a> {}

#[derive(Debug, Clone)]
pub struct StringColumnBuilder {
    pub data: Vec<u8>,
    pub offsets: Vec<u64>,
}

impl StringColumnBuilder {
    pub fn with_capacity(data_capacity: usize, offsets_capactiy: usize) -> Self {
        let mut offsets = Vec::with_capacity(offsets_capactiy);
        offsets.push(0);
        StringColumnBuilder {
            data: Vec::with_capacity(data_capacity),
            offsets,
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn put_u8(&mut self, item: u8) {
        self.data.push(item);
    }

    pub fn put_char(&mut self, item: char) {
        self.data
            .extend_from_slice(item.encode_utf8(&mut [0; 4]).as_bytes());
    }

    pub fn put_str(&mut self, item: &str) {
        self.data.extend_from_slice(item.as_bytes());
    }

    pub fn put_slice(&mut self, item: &[u8]) {
        self.data.extend_from_slice(item);
    }

    pub fn commit_row(&mut self) {
        self.offsets.push(self.data.len() as u64);
    }

    pub fn append(&mut self, other: &Self) {
        self.data.extend_from_slice(&other.data);
        let start = self.offsets.last().cloned().unwrap();
        self.offsets
            .extend(other.offsets.iter().skip(1).map(|offset| start + offset));
    }

    pub fn build(self) -> (Buffer<u8>, Buffer<u64>) {
        (self.data.into(), self.offsets.into())
    }

    pub fn build_scalar(self) -> Vec<u8> {
        assert_eq!(self.offsets.len(), 2);
        self.data[(self.offsets[0] as usize)..(self.offsets[1] as usize)].to_vec()
    }

    pub fn from_scalar(data: Vec<u8>) -> Self {
        StringColumnBuilder {
            offsets: vec![0, data.len() as u64],
            data,
        }
    }

    pub fn try_from_transform<F>(
        src: StringIterator<'_>,
        estimate_bytes: usize,
        mut f: F,
    ) -> Result<StringColumnBuilder, String>
    where
        F: FnMut(&[u8], &mut [u8]) -> Result<usize, String>,
    {
        let mut values: Vec<u8> = Vec::with_capacity(estimate_bytes);
        let mut offsets: Vec<u64> = Vec::with_capacity(src.size_hint().0 + 1);
        offsets.push(0);

        let mut offset: usize = 0;
        unsafe {
            for x in src {
                let bytes = std::slice::from_raw_parts_mut(
                    values.as_mut_ptr().add(offset),
                    values.capacity() - offset,
                );

                match f(x, bytes) {
                    Ok(l) => {
                        offset += l;
                        offsets.push(offset as u64);
                    }

                    Err(e) => return Err(e),
                }
            }
            values.set_len(offset);
            values.shrink_to_fit();

            Ok(StringColumnBuilder {
                data: values,
                offsets,
            })
        }
    }
}

pub fn try_transform_scalar<F>(
    val: &[u8],
    estimate_bytes: usize,
    mut func: F,
) -> Result<Vec<u8>, String>
where
    F: FnMut(&[u8], &mut [u8]) -> Result<usize, String>,
{
    let mut buf = Vec::with_capacity(estimate_bytes);
    unsafe {
        let bytes = std::slice::from_raw_parts_mut(buf.as_mut_ptr(), buf.capacity());
        let len = func(val, bytes)?;
        buf.set_len(len);
    }

    Ok(buf)
}
