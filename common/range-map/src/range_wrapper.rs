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

use core::cmp::Ordering;
use core::ops::Range;
use std::fmt::Debug;

#[derive(Eq, Debug, Clone)]
pub struct RangeWrapper<T> {
    pub range: Range<T>,
}

impl<T> RangeWrapper<T>
where T: Eq + Ord
{
    pub fn new(range: Range<T>) -> RangeWrapper<T> {
        RangeWrapper { range }
    }

    // compare to the range which start == end
    pub fn compare_to_point(&self, other: &RangeWrapper<T>) -> Ordering {
        assert!(other.range.start == other.range.end);
        if self.range.end.cmp(&other.range.end) == Ordering::Greater {
            return Ordering::Greater;
        }
        Ordering::Less
    }
}

impl<T> ToString for RangeWrapper<T>
where T: Debug
{
    fn to_string(&self) -> String {
        format!("{:?}-{:?}", self.range.start, self.range.end)
    }
}

impl<T> PartialEq for RangeWrapper<T>
where T: Eq
{
    fn eq(&self, other: &RangeWrapper<T>) -> bool {
        self.range.start == other.range.start && self.range.end == other.range.end
    }
}

impl<T> Ord for RangeWrapper<T>
where T: Ord + std::fmt::Debug + Copy
{
    fn cmp(&self, other: &RangeWrapper<T>) -> Ordering {
        // first handle the case one of the range has only one point(start == end)
        if other.range.start == other.range.end {
            return self.compare_to_point(other);
        } else if self.range.start == self.range.end {
            return Ordering::reverse(other.compare_to_point(self));
        }
        // use (start,end) tuple cmp result
        (self.range.start, self.range.end).cmp(&(other.range.start, other.range.end))
    }
}

impl<T> PartialOrd for RangeWrapper<T>
where T: Ord + std::fmt::Debug + Copy
{
    fn partial_cmp(&self, other: &RangeWrapper<T>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
