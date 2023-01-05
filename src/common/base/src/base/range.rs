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

use std::cmp::PartialOrd;
use std::ops::*;

/// More precise intersection of two ranges from point of the first range
#[derive(Debug, PartialEq)]
pub enum Intersection {
    /// The self is below the other
    Bellow,
    /// The self is below but overlaping
    BellowOverlap,
    /// The self is within the other
    Within,
    /// The self is same as the other
    Same,
    /// The self is over the other, the other is within the self
    Over,
    /// The self is above but overlaping
    AboveOverlap,
    /// The self is above the other
    Above,
}

impl Intersection {
    /// Test if there is any intersection
    pub fn is_any(&self) -> bool {
        !matches!(self, Intersection::Bellow | Intersection::Above)
    }
    /// Test if the range is fully within the other
    pub fn is_within(&self) -> bool {
        matches!(self, Intersection::Within | Intersection::Same)
    }

    /// Test if the range is fully over the other
    pub fn is_over(&self) -> bool {
        matches!(self, Intersection::Over | Intersection::Same)
    }
}

pub trait Intersect<T: PartialOrd, U: RangeBounds<T>>: RangeBounds<T> {
    /// Test two ranges for an intersection
    fn intersect(&self, other: &U) -> Intersection;
    fn is_overlap(&self, other: &U) -> bool {
        self.intersect(other).is_any()
    }
}

impl<T: PartialOrd> Intersect<T, Range<T>> for Range<T> {
    fn intersect(&self, other: &Range<T>) -> Intersection {
        if self.end == other.end {
            if self.start < other.start {
                Intersection::Over
            } else if self.start > other.start {
                Intersection::Within
            } else {
                Intersection::Same
            }
        } else if self.end < other.end {
            if self.end <= other.start {
                Intersection::Bellow
            } else if self.start < other.start {
                Intersection::BellowOverlap
            } else {
                Intersection::Within
            }
        } else if self.start < other.end {
            if self.start <= other.start {
                Intersection::Over
            } else {
                Intersection::AboveOverlap
            }
        } else {
            Intersection::Above
        }
    }
}

impl<T: PartialOrd> Intersect<T, RangeFrom<T>> for Range<T> {
    fn intersect(&self, other: &RangeFrom<T>) -> Intersection {
        if self.end <= other.start {
            Intersection::Bellow
        } else if self.start < other.start {
            Intersection::BellowOverlap
        } else {
            Intersection::Within
        }
    }
}

impl<T: PartialOrd> Intersect<T, RangeFull> for Range<T> {
    fn intersect(&self, _: &RangeFull) -> Intersection {
        Intersection::Same
    }
}

impl<T: PartialOrd> Intersect<T, RangeTo<T>> for Range<T> {
    fn intersect(&self, other: &RangeTo<T>) -> Intersection {
        if self.start >= other.end {
            Intersection::Above
        } else if self.end > other.end {
            Intersection::AboveOverlap
        } else {
            Intersection::Within
        }
    }
}

impl<T: PartialOrd> Intersect<T, RangeInclusive<T>> for RangeInclusive<T> {
    fn intersect(&self, other: &RangeInclusive<T>) -> Intersection {
        if self.end() == other.end() {
            if self.start() < other.start() {
                //  ----
                //   ---
                Intersection::Over
            } else if self.start() > other.start() {
                //  ----
                // -----
                Intersection::Within
            } else {
                // -----
                // -----
                Intersection::Same
            }
        } else if self.end() < other.end() {
            if self.end() < other.start() {
                // ---
                //    --
                Intersection::Bellow
            } else if self.start() < other.start() {
                //  ---
                //    ---
                Intersection::BellowOverlap
            } else {
                //    --
                //   ----
                Intersection::Within
            }
        } else if self.start() < other.end() {
            if self.start() <= other.start() {
                //    --------
                //    -----
                Intersection::Over
            } else {
                Intersection::AboveOverlap
            }

            // else if self.start < other.end {
            // if self.start <= other.start {
            //     Intersection::Over
            // } else {
            //     Intersection::AboveOverlap
            // }
        } else {
            //     ---
            // ---
            Intersection::Above
        }
    }
}
