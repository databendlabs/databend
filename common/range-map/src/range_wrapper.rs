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

#[derive(Eq, Debug, Clone)]
pub struct RangeWrapper<T> {
    pub range: Range<T>,
}

impl<T> RangeWrapper<T> {
    pub fn new(range: Range<T>) -> RangeWrapper<T> {
        RangeWrapper { range }
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
where T: Ord + std::fmt::Debug
{
    fn cmp(&self, other: &RangeWrapper<T>) -> Ordering {
        let start = self.range.start.cmp(&other.range.start);
        let end = self.range.end.cmp(&other.range.end);

        if other.range.start == other.range.end {
            if end == Ordering::Less {
                return Ordering::Less;
            }

            return Ordering::Greater;
        }

        match start {
            Ordering::Less => match end {
                Ordering::Less => {
                    return Ordering::Less;
                }
                Ordering::Equal => {
                    return Ordering::Less;
                }
                Ordering::Greater => {
                    return Ordering::Greater;
                }
            },
            Ordering::Equal => {
                return end;
            }
            Ordering::Greater => match end {
                Ordering::Less => {
                    return Ordering::Less;
                }
                Ordering::Equal => {
                    return Ordering::Greater;
                }
                Ordering::Greater => {
                    return Ordering::Greater;
                }
            },
        }
    }
}

impl<T> PartialOrd for RangeWrapper<T>
where T: Ord + std::fmt::Debug
{
    fn partial_cmp(&self, other: &RangeWrapper<T>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// test if or not RangeWrapper satisfy reflexive property
    #[test]
    fn test_range_wrapper_reflexive_property() {
        let tests = vec![
            RangeWrapper::new(2..4),
            RangeWrapper::new(0..1),
            RangeWrapper::new(1..2),
            RangeWrapper::new(2..3),
            RangeWrapper::new(3..4),
            RangeWrapper::new(1..4),
            RangeWrapper::new(1..5),
            RangeWrapper::new(2..6),
            RangeWrapper::new(3..6),
            RangeWrapper::new(4..6),
            RangeWrapper::new(5..6),
        ];
        for i in tests.clone().iter() {
            for j in tests.clone().iter() {
                let ret_i_j = i.cmp(j);
                let ret_j_i = j.cmp(i);

                println!(
                    "i:{:?}, j:{:?}, ret_i_j: {:?}, ret_j_i: {:?}",
                    i, j, ret_i_j, ret_j_i
                );

                match ret_i_j {
                    Ordering::Equal => {
                        assert_eq!(ret_j_i, Ordering::Equal);
                    }
                    Ordering::Less => {
                        assert_eq!(ret_j_i, Ordering::Greater);
                    }
                    Ordering::Greater => {
                        assert_eq!(ret_j_i, Ordering::Less);
                    }
                }
            }
        }
    }
}
