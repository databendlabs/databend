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

use crate::BlockEntry;
use crate::DataBlock;

#[derive(Debug, Clone, Copy)]
pub struct ProjectedBlock<'a> {
    map: Option<&'a [usize]>,
    entries: &'a [BlockEntry],
}

impl Index<usize> for ProjectedBlock<'_> {
    type Output = BlockEntry;

    fn index(&self, index: usize) -> &Self::Output {
        match &self.map {
            Some(map) => self.entries.index(map[index]),
            None => self.entries.index(index),
        }
    }
}

impl<'a> ProjectedBlock<'a> {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match &self.map {
            Some(map) => map.len(),
            None => self.entries.len(),
        }
    }

    pub fn slice<I>(&self, index: I) -> ProjectedBlock<'_>
    where I: SliceIndex<[usize], Output = [usize]> + SliceIndex<[BlockEntry], Output = [BlockEntry]>
    {
        match &self.map {
            Some(map) => Self {
                map: Some(&map[index]),
                entries: self.entries,
            },
            None => Self {
                map: None,
                entries: &self.entries[index],
            },
        }
    }

    pub fn iter(&self) -> EntriesIter<'_> {
        match &self.map {
            Some(map) => EntriesIter {
                iter: 0..map.len(),
                this: self,
            },
            None => EntriesIter {
                iter: 0..self.entries.len(),
                this: self,
            },
        }
    }

    pub fn project(map: &'a [usize], data: &'a DataBlock) -> ProjectedBlock<'a> {
        Self {
            map: Some(map),
            entries: data.columns(),
        }
    }

    pub fn num_rows(&self) -> usize {
        match self.entries.is_empty() {
            true => 0,
            false => self.entries[0].len(),
        }
    }
}

pub struct EntriesIter<'a> {
    iter: Range<usize>,
    this: &'a ProjectedBlock<'a>,
}

unsafe impl std::iter::TrustedLen for EntriesIter<'_> {}

impl<'a> Iterator for EntriesIter<'a> {
    type Item = &'a BlockEntry;

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

impl<'a> From<&'a [BlockEntry]> for ProjectedBlock<'a> {
    fn from(value: &'a [BlockEntry]) -> Self {
        ProjectedBlock {
            map: None,
            entries: value,
        }
    }
}

impl<'a, const N: usize> From<&'a [BlockEntry; N]> for ProjectedBlock<'a> {
    fn from(value: &'a [BlockEntry; N]) -> Self {
        ProjectedBlock {
            map: None,
            entries: value.as_slice(),
        }
    }
}

impl<'a> From<&'a Vec<BlockEntry>> for ProjectedBlock<'a> {
    fn from(value: &'a Vec<BlockEntry>) -> Self {
        ProjectedBlock {
            map: None,
            entries: value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ProjectedBlock;
    use crate::DataBlock;
    use crate::FromData;
    use crate::types::*;

    #[test]
    fn test_input_columns() {
        let strings = (0..10).map(|i: i32| i.to_string()).collect::<Vec<String>>();
        let nums = (0..10).collect::<Vec<_>>();
        let bools = (0..10).map(|i: usize| i % 2 == 0).collect();

        let columns = vec![
            StringType::from_data(strings),
            Int32Type::from_data(nums),
            BooleanType::from_data(bools),
        ];
        let block = DataBlock::new_from_columns(columns);

        let proxy = ProjectedBlock::project(&[1], &block);
        assert_eq!(proxy.len(), 1);

        let proxy = ProjectedBlock::project(&[2, 0, 1], &block);
        assert_eq!(proxy.len(), 3);
        assert!(proxy[0].data_type().is_boolean());
        assert!(proxy[1].data_type().is_string());
        assert!(proxy[2].data_type().is_number());

        assert_eq!(proxy.iter().count(), 3);

        let mut iter = proxy.iter();
        assert_eq!(iter.size_hint(), (3, Some(3)));
        let entry = iter.nth(1);
        assert!(entry.unwrap().data_type().is_string());

        assert_eq!(iter.size_hint(), (1, Some(1)));
        assert_eq!(iter.count(), 1);

        assert!(proxy.iter().last().unwrap().data_type().is_number());
        assert_eq!(proxy.iter().count(), 3);
        assert_eq!(proxy.iter().size_hint(), (3, Some(3)));

        let s = proxy.slice(..1);
        assert_eq!(s.len(), 1);
        assert!(s[0].data_type().is_boolean());

        let s = proxy.slice(1..=1);
        assert_eq!(s.len(), 1);
        assert!(s[0].data_type().is_string());
    }
}
