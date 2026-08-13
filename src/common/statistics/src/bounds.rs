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

use std::ops::Bound;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::Datum;

/// A finite, non-NULL closed interval in the statistics value space.
#[derive(Debug, Clone, PartialEq)]
pub struct StatBounds {
    lower_bound: Datum,
    upper_bound: Datum,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StatRangeBounds {
    Bounds(StatBounds),
    Empty,
    Imprecise,
}

impl StatBounds {
    pub fn new(lower_bound: Datum, upper_bound: Datum) -> Result<Self> {
        if !lower_bound.type_comparable(&upper_bound) {
            return Err(ErrorCode::InvalidArgument(format!(
                "statistics bounds are not comparable: {lower_bound:?} and {upper_bound:?}"
            )));
        }
        if lower_bound.compare(&upper_bound)?.is_gt() {
            return Err(ErrorCode::InvalidArgument(format!(
                "statistics lower bound {lower_bound:?} exceeds upper bound {upper_bound:?}"
            )));
        }
        Ok(Self {
            lower_bound,
            upper_bound,
        })
    }

    pub fn from_range_constraint(
        min: &Datum,
        max: &Datum,
        lower: &Bound<Datum>,
        upper: &Bound<Datum>,
    ) -> Result<StatRangeBounds> {
        let new_min = match lower {
            Bound::Unbounded => Some(min.clone()),
            Bound::Included(datum) => Datum::max(Some(min.clone()), Some(datum.clone())),
            Bound::Excluded(datum) => {
                if datum.compare(max)? != std::cmp::Ordering::Less {
                    return Ok(StatRangeBounds::Empty);
                }
                if datum.compare(min)? == std::cmp::Ordering::Less {
                    Some(min.clone())
                } else {
                    let datum = match datum {
                        Datum::Bool(false) => Some(Datum::Bool(true)),
                        Datum::Int(value) => value.checked_add(1).map(Datum::Int),
                        Datum::UInt(value) => value.checked_add(1).map(Datum::UInt),
                        // Column stats store closed bounds. For types without
                        // a representable adjacent value, keep the literal as
                        // a coarse bound for the strict predicate.
                        Datum::Float(_) | Datum::Bytes(_) => Some(datum.clone()),
                        Datum::Bool(true) => None,
                    };
                    if datum.is_none() {
                        return Ok(StatRangeBounds::Imprecise);
                    };
                    datum
                }
            }
        };
        let new_max = match upper {
            Bound::Unbounded => Some(max.clone()),
            Bound::Included(datum) => Datum::min(Some(max.clone()), Some(datum.clone())),
            Bound::Excluded(datum) => {
                if datum.compare(min)? != std::cmp::Ordering::Greater {
                    return Ok(StatRangeBounds::Empty);
                }
                if datum.compare(max)? == std::cmp::Ordering::Greater {
                    Some(max.clone())
                } else {
                    let datum = match datum {
                        Datum::Bool(false) => None,
                        Datum::Bool(true) => Some(Datum::Bool(false)),
                        Datum::Int(value) => value.checked_sub(1).map(Datum::Int),
                        Datum::UInt(value) => value.checked_sub(1).map(Datum::UInt),
                        // See the lower-bound case above.
                        Datum::Float(_) | Datum::Bytes(_) => Some(datum.clone()),
                    };
                    if datum.is_none() {
                        return Ok(StatRangeBounds::Imprecise);
                    };
                    datum
                }
            }
        };

        let (Some(new_min), Some(new_max)) = (new_min, new_max) else {
            return Ok(StatRangeBounds::Empty);
        };
        if new_min.compare(&new_max)? == std::cmp::Ordering::Greater {
            return Ok(StatRangeBounds::Empty);
        }

        Ok(StatRangeBounds::Bounds(Self {
            lower_bound: new_min,
            upper_bound: new_max,
        }))
    }

    pub fn lower_bound(&self) -> &Datum {
        &self.lower_bound
    }

    pub fn upper_bound(&self) -> &Datum {
        &self.upper_bound
    }

    pub fn into_parts(self) -> (Datum, Datum) {
        (self.lower_bound, self.upper_bound)
    }
}

impl TryFrom<(Datum, Datum)> for StatBounds {
    type Error = ErrorCode;

    fn try_from((lower_bound, upper_bound): (Datum, Datum)) -> Result<Self> {
        Self::new(lower_bound, upper_bound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_new_stat_bounds() {
        assert!(StatBounds::new(Datum::Int(1), Datum::Int(2)).is_ok());
        assert!(StatBounds::new(Datum::Int(2), Datum::Int(1)).is_err());
        assert!(StatBounds::new(Datum::Bool(false), Datum::Int(1)).is_err());
    }
}
