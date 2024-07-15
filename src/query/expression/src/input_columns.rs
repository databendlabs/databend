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

use std::ops::Index;
use std::ops::Range;
use std::slice::SliceIndex;

use enum_as_inner::EnumAsInner;

use crate::types::DataType;
use crate::Column;
use crate::DataBlock;

#[derive(Copy, Clone, EnumAsInner)]
pub enum InputColumns<'a> {
    Slice(&'a [Column]),
    Block(BlockProxy<'a>),
}

impl Index<usize> for InputColumns<'_> {
    type Output = Column;

    fn index(&self, index: usize) -> &Self::Output {
        match self {
            Self::Slice(slice) => slice.index(index),
            Self::Block(BlockProxy { args, data }) => {
                data.get_by_offset(args[index]).value.as_column().unwrap()
            }
        }
    }
}

impl<'a> InputColumns<'a> {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Slice(s) => s.len(),
            Self::Block(BlockProxy { args, .. }) => args.len(),
        }
    }

    pub fn slice<I>(&self, index: I) -> InputColumns<'_>
    where I: SliceIndex<[usize], Output = [usize]> + SliceIndex<[Column], Output = [Column]> {
        match self {
            Self::Slice(s) => Self::Slice(&s[index]),
            Self::Block(BlockProxy { args, data }) => Self::Block(BlockProxy {
                args: &args[index],
                data,
            }),
        }
    }

    pub fn iter(&self) -> InputColumnsIter {
        match self {
            Self::Slice(s) => InputColumnsIter {
                iter: 0..s.len(),
                this: self,
            },
            Self::Block(BlockProxy { args, .. }) => InputColumnsIter {
                iter: 0..args.len(),
                this: self,
            },
        }
    }

    pub fn new_block_proxy(args: &'a [usize], data: &'a DataBlock) -> InputColumns<'a> {
        Self::Block(BlockProxy { args, data })
    }
}

pub struct InputColumnsIter<'a> {
    iter: Range<usize>,
    this: &'a InputColumns<'a>,
}

unsafe impl<'a> std::iter::TrustedLen for InputColumnsIter<'a> {}

impl<'a> Iterator for InputColumnsIter<'a> {
    type Item = &'a Column;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|index| self.this.index(index))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    fn count(self) -> usize
    where Self: Sized {
        self.iter.count()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.iter.nth(n).map(|index| self.this.index(index))
    }

    fn last(self) -> Option<Self::Item>
    where Self: Sized {
        self.iter.last().map(|index| self.this.index(index))
    }
}

impl<'a> From<&'a [Column]> for InputColumns<'a> {
    fn from(value: &'a [Column]) -> Self {
        InputColumns::Slice(value)
    }
}

impl<'a, const N: usize> From<&'a [Column; N]> for InputColumns<'a> {
    fn from(value: &'a [Column; N]) -> Self {
        InputColumns::Slice(value.as_slice())
    }
}

impl<'a> From<&'a Vec<Column>> for InputColumns<'a> {
    fn from(value: &'a Vec<Column>) -> Self {
        InputColumns::Slice(value)
    }
}

#[derive(Copy, Clone)]
pub struct BlockProxy<'a> {
    args: &'a [usize],
    data: &'a DataBlock,
}

impl<'a> BlockProxy<'a> {
    pub fn data_types(&self) -> Vec<&'a DataType> {
        let Self { args, data } = self;
        args.iter()
            .map(|&i| &data.get_by_offset(i).data_type)
            .collect::<Vec<_>>()
    }
}
