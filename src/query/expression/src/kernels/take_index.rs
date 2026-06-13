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

use std::iter::Map;
use std::iter::RepeatN;
use std::ops::Range;
use std::slice::Iter;

use databend_common_base::vec_ext::VecExt;

pub trait TakeIndex {
    type Iter<'a>: Iterator<Item = usize>
    where Self: 'a;

    fn iter(&self) -> Self::Iter<'_>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn take_primitive_types<T: Copy>(&self, slice: &[T], builder: &mut Vec<T>) {
        builder.extend(
            self.iter()
                .map(|index| unsafe { *slice.get_unchecked(index) }),
        );
    }
}

impl TakeIndex for [u32] {
    type Iter<'a> = Map<Iter<'a, u32>, fn(&u32) -> usize>;

    fn iter(&self) -> Self::Iter<'_> {
        self.iter().map(|v| *v as usize)
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl TakeIndex for [u64] {
    type Iter<'a> = Map<Iter<'a, u64>, fn(&u64) -> usize>;

    fn iter(&self) -> Self::Iter<'_> {
        self.iter().map(|v| *v as usize)
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl TakeIndex for Range<u32> {
    type Iter<'a> = Map<Range<u32>, fn(u32) -> usize>;

    fn iter(&self) -> Self::Iter<'_> {
        self.clone().map(|index| index as usize)
    }

    fn len(&self) -> usize {
        std::iter::ExactSizeIterator::len(self)
    }

    fn take_primitive_types<T: Copy>(&self, slice: &[T], builder: &mut Vec<T>) {
        unsafe {
            builder.extend_from_slice_unchecked(&slice[self.start as usize..self.end as usize])
        };
    }
}

pub struct RepeatIndex {
    pub row: u32,
    pub count: u32,
}

impl TakeIndex for RepeatIndex {
    type Iter<'a> = RepeatN<usize>;

    fn iter(&self) -> Self::Iter<'_> {
        std::iter::repeat_n(self.row as usize, self.count as usize)
    }

    fn len(&self) -> usize {
        self.count as _
    }

    fn take_primitive_types<T: Copy>(&self, slice: &[T], builder: &mut Vec<T>) {
        if self.count == 1 {
            unsafe {
                builder.push_unchecked(slice[self.row as usize]);
            }
            return;
        }

        // Using the doubling method to copy the max segment memory.
        // [___________] => [x__________] => [xx_________] => [xxxx_______] => [xxxxxxxx___]
        // Since cnt > 0, then 31 - cnt.leading_zeros() >= 0.
        let max_segment = 1 << (31 - self.count.leading_zeros());
        let base_pos = builder.len();
        unsafe {
            builder.push_unchecked(slice[self.row as usize]);
        }

        let mut cur_segment = 1;
        while cur_segment < max_segment {
            builder.extend_from_within(base_pos..base_pos + cur_segment);
            cur_segment <<= 1;
        }

        // Copy the remaining memory directly.
        // [xxxxxxxxxx____] => [xxxxxxxxxxxxxx]
        //  ^^^^ ---> ^^^^
        let remain = self.count as usize - max_segment;
        if remain > 0 {
            builder.extend_from_within(base_pos..base_pos + remain)
        }
    }
}
