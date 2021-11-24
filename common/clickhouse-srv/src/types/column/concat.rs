// Copyright 2021 Datafuse Labs.
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

use std::iter;

use super::column_data::ArcColumnData;
use super::column_data::BoxColumnData;
use super::column_data::ColumnData;
use crate::binary::Encoder;
use crate::errors::Error;
use crate::errors::FromSqlError;
use crate::errors::Result;
use crate::types::SqlType;
use crate::types::Value;
use crate::types::ValueRef;

pub struct ConcatColumnData {
    data: Vec<ArcColumnData>,
    index: Vec<usize>,
}

impl ConcatColumnData {
    pub fn concat(data: Vec<ArcColumnData>) -> Self {
        Self::check_columns(&data);

        let index = build_index(data.iter().map(|x| x.len()));
        Self { data, index }
    }

    fn check_columns(data: &[ArcColumnData]) {
        match data.first() {
            None => panic!("data should not be empty."),
            Some(first) => {
                for column in data.iter().skip(1) {
                    if first.sql_type() != column.sql_type() {
                        panic!(
                            "all columns should have the same type ({:?} != {:?}).",
                            first.sql_type(),
                            column.sql_type()
                        );
                    }
                }
            }
        }
    }
}

impl ColumnData for ConcatColumnData {
    fn sql_type(&self) -> SqlType {
        self.data[0].sql_type()
    }

    fn save(&self, _: &mut Encoder, _: usize, _: usize) {
        unimplemented!()
    }

    fn len(&self) -> usize {
        *self.index.last().unwrap()
    }

    fn push(&mut self, _value: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        let chunk_index = find_chunk(&self.index, index);
        let chunk = &self.data[chunk_index];
        chunk.at(index - self.index[chunk_index])
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }

    unsafe fn get_internal(&self, pointers: &[*mut *const u8], level: u8) -> Result<()> {
        if level == 0xff {
            *pointers[0] = &self.data as *const Vec<ArcColumnData> as *mut u8;
            Ok(())
        } else {
            Err(Error::FromSql(FromSqlError::UnsupportedOperation))
        }
    }
}

pub fn build_index<'a, I>(sizes: I) -> Vec<usize>
where I: iter::Iterator<Item = usize> + 'a {
    let mut acc = 0;
    let mut index = vec![acc];

    for size in sizes {
        acc += size;
        index.push(acc);
    }

    index
}

pub fn find_chunk(index: &[usize], ix: usize) -> usize {
    let mut lo = 0_usize;
    let mut hi = index.len() - 1;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;

        if index[lo] == index[lo + 1] {
            lo += 1;
            continue;
        }

        if ix < index[mid] {
            hi = mid;
        } else if ix >= index[mid + 1] {
            lo = mid + 1;
        } else {
            return mid;
        }
    }

    0
}
