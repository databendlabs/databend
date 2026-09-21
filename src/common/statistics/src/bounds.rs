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
use crate::F64;
use crate::NumericHistogramType;
use crate::NumericRange;
use crate::TypedHistogramBounds;

/// A closed interval over non-NULL values in the statistics value space.
#[derive(Debug, Clone, PartialEq)]
pub enum StatBounds {
    Bool { min: bool, max: bool },
    Int { min: i64, max: i64 },
    UInt { min: u64, max: u64 },
    Float { min: F64, max: F64 },
    Bytes { min: Vec<u8>, max: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum StatRangeBounds {
    Bounds(StatBounds),
    Empty,
    Imprecise,
}

impl StatBounds {
    pub fn new(lower_bound: Datum, upper_bound: Datum) -> Result<Self> {
        match (lower_bound, upper_bound) {
            (Datum::Bool(min), Datum::Bool(max)) => Self::new_bool(min, max),
            (Datum::Int(min), Datum::Int(max)) => Self::new_int(min, max),
            (Datum::UInt(min), Datum::UInt(max)) => Self::new_uint(min, max),
            (Datum::Float(min), Datum::Float(max)) => Self::new_float(min, max),
            (Datum::Bytes(min), Datum::Bytes(max)) => Self::new_bytes(min, max),
            (min, max) => Err(ErrorCode::InvalidArgument(format!(
                "statistics bounds are not the same type: {min:?} and {max:?}"
            ))),
        }
    }

    pub fn new_bool(min: bool, max: bool) -> Result<Self> {
        if min & !max {
            return Err(Self::invalid_order(Datum::Bool(min), Datum::Bool(max)));
        }
        Ok(Self::Bool { min, max })
    }

    pub fn new_int(min: i64, max: i64) -> Result<Self> {
        if min > max {
            return Err(Self::invalid_order(Datum::Int(min), Datum::Int(max)));
        }
        Ok(Self::Int { min, max })
    }

    pub fn new_uint(min: u64, max: u64) -> Result<Self> {
        if min > max {
            return Err(Self::invalid_order(Datum::UInt(min), Datum::UInt(max)));
        }
        Ok(Self::UInt { min, max })
    }

    pub fn new_float(min: F64, max: F64) -> Result<Self> {
        if min > max {
            return Err(Self::invalid_order(Datum::Float(min), Datum::Float(max)));
        }
        Ok(Self::Float { min, max })
    }

    pub fn new_bytes(min: Vec<u8>, max: Vec<u8>) -> Result<Self> {
        if min > max {
            return Err(Self::invalid_order(
                Datum::Bytes(min.clone()),
                Datum::Bytes(max.clone()),
            ));
        }
        Ok(Self::Bytes { min, max })
    }

    fn invalid_order(lower_bound: Datum, upper_bound: Datum) -> ErrorCode {
        ErrorCode::InvalidArgument(format!(
            "statistics lower bound {lower_bound:?} exceeds upper bound {upper_bound:?}"
        ))
    }

    pub fn restrict_by_range(&self, lower: &Bound<Datum>, upper: &Bound<Datum>) -> StatRangeBounds {
        match self {
            StatBounds::Bool { min, max } => restrict_bool_range(*min, *max, lower, upper),
            StatBounds::Int { min, max } => restrict_int_range(*min, *max, lower, upper),
            StatBounds::UInt { min, max } => restrict_uint_range(*min, *max, lower, upper),
            StatBounds::Float { min, max } => restrict_float_range(*min, *max, lower, upper),
            StatBounds::Bytes { min, max } => restrict_bytes_range(min, max, lower, upper),
        }
    }

    pub fn contains_datum(&self, datum: &Datum) -> bool {
        match (self, datum) {
            (StatBounds::Bool { min, max }, Datum::Bool(value)) => min <= value && value <= max,
            (StatBounds::Int { min, max }, Datum::Int(value)) => min <= value && value <= max,
            (StatBounds::Int { min, max }, Datum::UInt(value)) => {
                if let Ok(value) = i64::try_from(*value) {
                    min <= &value && &value <= max
                } else {
                    false
                }
            }
            (StatBounds::UInt { min, max }, Datum::UInt(value)) => min <= value && value <= max,
            (StatBounds::UInt { min, max }, Datum::Int(value)) => {
                if let Ok(value) = u64::try_from(*value) {
                    min <= &value && &value <= max
                } else {
                    false
                }
            }
            (StatBounds::Float { min, max }, Datum::Float(value)) => min <= value && value <= max,
            (StatBounds::Float { min, max }, Datum::Int(value)) => {
                let value = F64::from(*value as f64);
                min <= &value && &value <= max
            }
            (StatBounds::Float { min, max }, Datum::UInt(value)) => {
                let value = F64::from(*value as f64);
                min <= &value && &value <= max
            }
            (StatBounds::Bytes { min, max }, Datum::Bytes(value)) => min <= value && value <= max,
            (
                StatBounds::Int { .. } | StatBounds::UInt { .. },
                Datum::Float(_) | Datum::Bool(_) | Datum::Bytes(_),
            )
            | (StatBounds::Float { .. }, Datum::Bool(_) | Datum::Bytes(_))
            | (StatBounds::Bool { .. } | StatBounds::Bytes { .. }, _) => false,
        }
    }

    pub fn display_parts(&self) -> (String, String) {
        match self {
            StatBounds::Bool { min, max } => (min.to_string(), max.to_string()),
            StatBounds::Int { min, max } => (min.to_string(), max.to_string()),
            StatBounds::UInt { min, max } => (min.to_string(), max.to_string()),
            StatBounds::Float { min, max } => (min.to_string(), max.to_string()),
            StatBounds::Bytes { min, max } => (
                String::from_utf8_lossy(min).into_owned(),
                String::from_utf8_lossy(max).into_owned(),
            ),
        }
    }

    pub fn debug_parts(&self) -> (String, String) {
        match self {
            StatBounds::Bool { min, max } => (format!("Bool({min:?})"), format!("Bool({max:?})")),
            StatBounds::Int { min, max } => (format!("Int({min:?})"), format!("Int({max:?})")),
            StatBounds::UInt { min, max } => (format!("UInt({min:?})"), format!("UInt({max:?})")),
            StatBounds::Float { min, max } => {
                (format!("Float({min:?})"), format!("Float({max:?})"))
            }
            StatBounds::Bytes { min, max } => {
                (format!("Bytes({min:?})"), format!("Bytes({max:?})"))
            }
        }
    }

    pub fn has_intersection(&self, other: &StatBounds) -> bool {
        self.intersection(other).is_some()
    }

    pub fn intersection(&self, other: &StatBounds) -> Option<StatBounds> {
        match (self, other) {
            (
                StatBounds::Bool {
                    min: left_min,
                    max: left_max,
                },
                StatBounds::Bool {
                    min: right_min,
                    max: right_max,
                },
            ) => {
                let min = (*left_min).max(*right_min);
                let max = (*left_max).min(*right_max);
                if min & !max {
                    None
                } else {
                    Some(StatBounds::Bool { min, max })
                }
            }
            (
                StatBounds::Int {
                    min: left_min,
                    max: left_max,
                },
                StatBounds::Int {
                    min: right_min,
                    max: right_max,
                },
            ) => {
                let bounds = TypedHistogramBounds::new(*left_min, *left_max)
                    .intersection(&TypedHistogramBounds::new(*right_min, *right_max))?;
                Some(StatBounds::Int {
                    min: *bounds.lower_bound(),
                    max: *bounds.upper_bound(),
                })
            }
            (
                StatBounds::UInt {
                    min: left_min,
                    max: left_max,
                },
                StatBounds::UInt {
                    min: right_min,
                    max: right_max,
                },
            ) => {
                let bounds = TypedHistogramBounds::new(*left_min, *left_max)
                    .intersection(&TypedHistogramBounds::new(*right_min, *right_max))?;
                Some(StatBounds::UInt {
                    min: *bounds.lower_bound(),
                    max: *bounds.upper_bound(),
                })
            }
            (
                StatBounds::Float {
                    min: left_min,
                    max: left_max,
                },
                StatBounds::Float {
                    min: right_min,
                    max: right_max,
                },
            ) => {
                let bounds = TypedHistogramBounds::new(*left_min, *left_max)
                    .intersection(&TypedHistogramBounds::new(*right_min, *right_max))?;
                Some(StatBounds::Float {
                    min: *bounds.lower_bound(),
                    max: *bounds.upper_bound(),
                })
            }
            (
                StatBounds::Bytes {
                    min: left_min,
                    max: left_max,
                },
                StatBounds::Bytes {
                    min: right_min,
                    max: right_max,
                },
            ) => {
                let bounds =
                    TypedHistogramBounds::new(left_min.clone(), left_max.clone()).intersection(
                        &TypedHistogramBounds::new(right_min.clone(), right_max.clone()),
                    )?;
                Some(StatBounds::Bytes {
                    min: bounds.lower_bound().clone(),
                    max: bounds.upper_bound().clone(),
                })
            }
            _ => None,
        }
    }

    pub fn numeric_intersection(
        &self,
        other: &StatBounds,
        return_type: NumericHistogramType,
    ) -> Result<Option<(StatBounds, StatBounds)>> {
        let (Some(left), Some(right)) = (
            numeric_range(self, return_type),
            numeric_range(other, return_type),
        ) else {
            return Ok(None);
        };
        let Some(intersection) = left.intersection(right)? else {
            return Ok(None);
        };
        let Some(left) = intersection.restrict_stat_bounds(self)? else {
            return Ok(None);
        };
        let Some(right) = intersection.restrict_stat_bounds(other)? else {
            return Ok(None);
        };
        Ok(Some((left, right)))
    }

    pub fn is_disjoint(&self, other: &Self) -> Result<bool> {
        if !matches!(
            (self, other),
            (StatBounds::Bool { .. }, StatBounds::Bool { .. })
                | (StatBounds::Int { .. }, StatBounds::Int { .. })
                | (StatBounds::UInt { .. }, StatBounds::UInt { .. })
                | (StatBounds::Float { .. }, StatBounds::Float { .. })
                | (StatBounds::Bytes { .. }, StatBounds::Bytes { .. })
        ) {
            return Err(ErrorCode::InvalidArgument(format!(
                "cannot compare statistics bounds of different types: {self:?} and {other:?}"
            )));
        }
        Ok(self.intersection(other).is_none())
    }

    pub fn union(self, other: Self) -> Result<Self> {
        match (self, other) {
            (
                StatBounds::Bool {
                    min: left_min,
                    max: left_max,
                },
                StatBounds::Bool {
                    min: right_min,
                    max: right_max,
                },
            ) => Ok(StatBounds::Bool {
                min: left_min.min(right_min),
                max: left_max.max(right_max),
            }),
            (
                StatBounds::Int {
                    min: left_min,
                    max: left_max,
                },
                StatBounds::Int {
                    min: right_min,
                    max: right_max,
                },
            ) => Ok(StatBounds::Int {
                min: left_min.min(right_min),
                max: left_max.max(right_max),
            }),
            (
                StatBounds::UInt {
                    min: left_min,
                    max: left_max,
                },
                StatBounds::UInt {
                    min: right_min,
                    max: right_max,
                },
            ) => Ok(StatBounds::UInt {
                min: left_min.min(right_min),
                max: left_max.max(right_max),
            }),
            (
                StatBounds::Float {
                    min: left_min,
                    max: left_max,
                },
                StatBounds::Float {
                    min: right_min,
                    max: right_max,
                },
            ) => Ok(StatBounds::Float {
                min: left_min.min(right_min),
                max: left_max.max(right_max),
            }),
            (
                StatBounds::Bytes {
                    min: left_min,
                    max: left_max,
                },
                StatBounds::Bytes {
                    min: right_min,
                    max: right_max,
                },
            ) => Ok(StatBounds::Bytes {
                min: left_min.min(right_min),
                max: left_max.max(right_max),
            }),
            (left, right) => Err(ErrorCode::InvalidArgument(format!(
                "cannot union statistics bounds of different types: {left:?} and {right:?}"
            ))),
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            StatBounds::Int { .. } | StatBounds::UInt { .. } | StatBounds::Float { .. }
        )
    }

    pub fn finite_ndv_upper(&self) -> Option<f64> {
        match self {
            StatBounds::Bool { min, max } => Some(if min == max { 1.0 } else { 2.0 }),
            StatBounds::Int { min, max } => Some((*max as i128 - *min as i128 + 1) as f64),
            StatBounds::UInt { min, max } => Some((*max as u128 - *min as u128 + 1) as f64),
            StatBounds::Float { min, max } => (min == max).then_some(1.0),
            StatBounds::Bytes { min, max } => (min == max).then_some(1.0),
        }
    }
}

fn numeric_range(bounds: &StatBounds, return_type: NumericHistogramType) -> Option<NumericRange> {
    match bounds {
        StatBounds::Int { min, max } => Some(return_type.project_range(min, max)),
        StatBounds::UInt { min, max } => Some(return_type.project_range(min, max)),
        StatBounds::Float { min, max } => Some(return_type.project_range(min, max)),
        StatBounds::Bool { .. } | StatBounds::Bytes { .. } => None,
    }
}

fn restrict_bool_range(
    min: bool,
    max: bool,
    lower: &Bound<Datum>,
    upper: &Bound<Datum>,
) -> StatRangeBounds {
    let new_min = match lower {
        Bound::Unbounded => min,
        Bound::Included(Datum::Bool(value)) => min.max(*value),
        Bound::Included(_) => return StatRangeBounds::Imprecise,
        Bound::Excluded(Datum::Bool(false)) => min.max(true),
        Bound::Excluded(Datum::Bool(true)) => return StatRangeBounds::Empty,
        Bound::Excluded(_) => return StatRangeBounds::Imprecise,
    };
    let new_max = match upper {
        Bound::Unbounded => max,
        Bound::Included(Datum::Bool(value)) => max.min(*value),
        Bound::Included(_) => return StatRangeBounds::Imprecise,
        Bound::Excluded(Datum::Bool(false)) => return StatRangeBounds::Empty,
        Bound::Excluded(Datum::Bool(true)) => max.min(false),
        Bound::Excluded(_) => return StatRangeBounds::Imprecise,
    };
    if new_min & !new_max {
        return StatRangeBounds::Empty;
    }
    StatRangeBounds::Bounds(StatBounds::Bool {
        min: new_min,
        max: new_max,
    })
}

fn restrict_int_range(
    min: i64,
    max: i64,
    lower: &Bound<Datum>,
    upper: &Bound<Datum>,
) -> StatRangeBounds {
    let new_min = match lower {
        Bound::Unbounded => min,
        Bound::Included(Datum::Int(value)) => min.max(*value),
        Bound::Included(Datum::UInt(value)) => {
            let Ok(value) = i64::try_from(*value) else {
                return StatRangeBounds::Empty;
            };
            min.max(value)
        }
        Bound::Included(Datum::Float(_)) => return StatRangeBounds::Imprecise,
        Bound::Included(_) => return StatRangeBounds::Imprecise,
        Bound::Excluded(Datum::Int(value)) => {
            if *value >= max {
                return StatRangeBounds::Empty;
            }
            min.max(value + 1)
        }
        Bound::Excluded(Datum::UInt(value)) => {
            let Ok(value) = i64::try_from(*value) else {
                return StatRangeBounds::Empty;
            };
            if value >= max {
                return StatRangeBounds::Empty;
            }
            min.max(value + 1)
        }
        Bound::Excluded(Datum::Float(_)) => return StatRangeBounds::Imprecise,
        Bound::Excluded(_) => return StatRangeBounds::Imprecise,
    };
    let new_max = match upper {
        Bound::Unbounded => max,
        Bound::Included(Datum::Int(value)) => max.min(*value),
        Bound::Included(Datum::UInt(value)) => {
            let Ok(value) = i64::try_from(*value) else {
                return if new_min > max {
                    StatRangeBounds::Empty
                } else {
                    StatRangeBounds::Bounds(StatBounds::Int { min: new_min, max })
                };
            };
            max.min(value)
        }
        Bound::Included(Datum::Float(_)) => return StatRangeBounds::Imprecise,
        Bound::Included(_) => return StatRangeBounds::Imprecise,
        Bound::Excluded(Datum::Int(value)) => {
            if *value <= min {
                return StatRangeBounds::Empty;
            }
            max.min(value - 1)
        }
        Bound::Excluded(Datum::UInt(value)) => {
            if *value == 0 {
                let new_max = max.min(-1);
                return if new_min > new_max {
                    StatRangeBounds::Empty
                } else {
                    StatRangeBounds::Bounds(StatBounds::Int {
                        min: new_min,
                        max: new_max,
                    })
                };
            }
            let Ok(value) = i64::try_from(*value) else {
                return if new_min > max {
                    StatRangeBounds::Empty
                } else {
                    StatRangeBounds::Bounds(StatBounds::Int { min: new_min, max })
                };
            };
            max.min(value - 1)
        }
        Bound::Excluded(Datum::Float(_)) => return StatRangeBounds::Imprecise,
        Bound::Excluded(_) => return StatRangeBounds::Imprecise,
    };
    if new_min > new_max {
        return StatRangeBounds::Empty;
    }
    StatRangeBounds::Bounds(StatBounds::Int {
        min: new_min,
        max: new_max,
    })
}

fn restrict_uint_range(
    min: u64,
    max: u64,
    lower: &Bound<Datum>,
    upper: &Bound<Datum>,
) -> StatRangeBounds {
    let new_min = match lower {
        Bound::Unbounded => min,
        Bound::Included(Datum::UInt(value)) => min.max(*value),
        Bound::Included(Datum::Int(value)) if *value <= 0 => min,
        Bound::Included(Datum::Int(value)) => min.max(*value as u64),
        Bound::Included(Datum::Float(_)) => return StatRangeBounds::Imprecise,
        Bound::Included(_) => return StatRangeBounds::Imprecise,
        Bound::Excluded(Datum::UInt(value)) => {
            if *value >= max {
                return StatRangeBounds::Empty;
            }
            min.max(value + 1)
        }
        Bound::Excluded(Datum::Int(value)) if *value < 0 => min,
        Bound::Excluded(Datum::Int(value)) => {
            let value = *value as u64;
            if value >= max {
                return StatRangeBounds::Empty;
            }
            min.max(value + 1)
        }
        Bound::Excluded(Datum::Float(_)) => return StatRangeBounds::Imprecise,
        Bound::Excluded(_) => return StatRangeBounds::Imprecise,
    };
    let new_max = match upper {
        Bound::Unbounded => max,
        Bound::Included(Datum::UInt(value)) => max.min(*value),
        Bound::Included(Datum::Int(value)) if *value < 0 => return StatRangeBounds::Empty,
        Bound::Included(Datum::Int(value)) => max.min(*value as u64),
        Bound::Included(Datum::Float(_)) => return StatRangeBounds::Imprecise,
        Bound::Included(_) => return StatRangeBounds::Imprecise,
        Bound::Excluded(Datum::UInt(value)) => {
            if *value <= min {
                return StatRangeBounds::Empty;
            }
            max.min(value - 1)
        }
        Bound::Excluded(Datum::Int(value)) if *value <= 0 => return StatRangeBounds::Empty,
        Bound::Excluded(Datum::Int(value)) => max.min(*value as u64 - 1),
        Bound::Excluded(Datum::Float(_)) => return StatRangeBounds::Imprecise,
        Bound::Excluded(_) => return StatRangeBounds::Imprecise,
    };
    if new_min > new_max {
        return StatRangeBounds::Empty;
    }
    StatRangeBounds::Bounds(StatBounds::UInt {
        min: new_min,
        max: new_max,
    })
}

fn restrict_float_range(
    min: F64,
    max: F64,
    lower: &Bound<Datum>,
    upper: &Bound<Datum>,
) -> StatRangeBounds {
    let new_min = match lower {
        Bound::Unbounded => min,
        Bound::Included(value) => match float_bound_value(value) {
            Some(value) => min.max(value),
            None => return StatRangeBounds::Imprecise,
        },
        Bound::Excluded(value) => {
            let Some(value) = float_bound_value(value) else {
                return StatRangeBounds::Imprecise;
            };
            // Column stats store closed bounds. Float has no representable
            // adjacent value here, so keep the literal as a coarse closed bound
            // unless it already empties the existing endpoint.
            if value >= max {
                return StatRangeBounds::Empty;
            }
            min.max(value)
        }
    };
    let new_max = match upper {
        Bound::Unbounded => max,
        Bound::Included(value) => match float_bound_value(value) {
            Some(value) => max.min(value),
            None => return StatRangeBounds::Imprecise,
        },
        Bound::Excluded(value) => {
            let Some(value) = float_bound_value(value) else {
                return StatRangeBounds::Imprecise;
            };
            if value <= min {
                return StatRangeBounds::Empty;
            }
            max.min(value)
        }
    };
    if new_min > new_max {
        return StatRangeBounds::Empty;
    }
    StatRangeBounds::Bounds(StatBounds::Float {
        min: new_min,
        max: new_max,
    })
}

fn float_bound_value(datum: &Datum) -> Option<F64> {
    match datum {
        Datum::Float(value) => Some(*value),
        Datum::Int(value) => Some(F64::from(*value as f64)),
        Datum::UInt(value) => Some(F64::from(*value as f64)),
        Datum::Bool(_) | Datum::Bytes(_) => None,
    }
}

fn restrict_bytes_range(
    min: &[u8],
    max: &[u8],
    lower: &Bound<Datum>,
    upper: &Bound<Datum>,
) -> StatRangeBounds {
    let new_min = match lower {
        Bound::Unbounded => min.to_vec(),
        Bound::Included(Datum::Bytes(value)) => bytes_lower_bound(min, value),
        Bound::Excluded(Datum::Bytes(value)) => {
            // See restrict_float_range: keep the literal as a coarse closed bound
            // unless the excluded endpoint already empties the range.
            if value.as_slice() >= max {
                return StatRangeBounds::Empty;
            }
            bytes_lower_bound(min, value)
        }
        Bound::Included(_) | Bound::Excluded(_) => return StatRangeBounds::Imprecise,
    };
    let new_max = match upper {
        Bound::Unbounded => max.to_vec(),
        Bound::Included(Datum::Bytes(value)) => bytes_upper_bound(max, value),
        Bound::Excluded(Datum::Bytes(value)) => {
            if value.as_slice() <= min {
                return StatRangeBounds::Empty;
            }
            bytes_upper_bound(max, value)
        }
        Bound::Included(_) | Bound::Excluded(_) => return StatRangeBounds::Imprecise,
    };
    if new_min > new_max {
        return StatRangeBounds::Empty;
    }
    StatRangeBounds::Bounds(StatBounds::Bytes {
        min: new_min,
        max: new_max,
    })
}

fn bytes_lower_bound(min: &[u8], value: &[u8]) -> Vec<u8> {
    if min > value {
        min.to_vec()
    } else {
        value.to_vec()
    }
}

fn bytes_upper_bound(max: &[u8], value: &[u8]) -> Vec<u8> {
    if max < value {
        max.to_vec()
    } else {
        value.to_vec()
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
        assert_eq!(
            StatBounds::new(Datum::UInt(1), Datum::UInt(2)).unwrap(),
            StatBounds::UInt { min: 1, max: 2 }
        );
    }

    #[test]
    fn test_finite_ndv_upper_uses_typed_bounds() {
        assert_eq!(
            StatBounds::Int { min: -2, max: 2 }.finite_ndv_upper(),
            Some(5.0)
        );
        assert_eq!(
            StatBounds::Int {
                min: i64::MIN,
                max: i64::MAX,
            }
            .finite_ndv_upper(),
            Some(2_f64.powi(64))
        );
        assert_eq!(
            StatBounds::UInt {
                min: u64::MIN,
                max: u64::MAX,
            }
            .finite_ndv_upper(),
            Some(2_f64.powi(64))
        );
        assert_eq!(
            StatBounds::Bytes {
                min: b"x".to_vec(),
                max: b"x".to_vec(),
            }
            .finite_ndv_upper(),
            Some(1.0)
        );
    }

    #[test]
    fn test_union_and_disjoint_use_typed_bounds() -> Result<()> {
        let left = StatBounds::Int { min: 1, max: 3 };
        let overlapping = StatBounds::Int { min: 3, max: 5 };
        let disjoint = StatBounds::Int { min: 6, max: 8 };

        assert!(!left.is_disjoint(&overlapping)?);
        assert!(left.is_disjoint(&disjoint)?);
        assert_eq!(left.clone().union(disjoint)?, StatBounds::Int {
            min: 1,
            max: 8
        });
        assert!(
            left.is_disjoint(&StatBounds::UInt { min: 1, max: 3 })
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn test_stat_bounds_numeric_intersection() {
        let left = StatBounds::new(Datum::UInt(0), Datum::UInt(10)).unwrap();
        let right = StatBounds::new(Datum::UInt(5), Datum::UInt(15)).unwrap();

        assert!(left.has_intersection(&right));
        assert_eq!(
            left.intersection(&right),
            Some(StatBounds::UInt { min: 5, max: 10 })
        );
    }

    #[test]
    fn test_stat_bounds_keeps_mixed_numeric_intersection_explicit() -> Result<()> {
        let left = StatBounds::new(Datum::UInt(0), Datum::UInt(10)).unwrap();
        let right = StatBounds::new(Datum::Int(5), Datum::Int(15)).unwrap();

        assert!(!left.has_intersection(&right));
        assert_eq!(left.intersection(&right), None);
        assert_eq!(
            left.numeric_intersection(&right, NumericHistogramType::Int)?,
            Some((StatBounds::UInt { min: 5, max: 10 }, StatBounds::Int {
                min: 5,
                max: 10
            }))
        );
        Ok(())
    }

    #[test]
    fn test_stat_bounds_numeric_intersection_returns_none_for_disjoint_bounds() -> Result<()> {
        let left = StatBounds::new(Datum::UInt(u64::MAX), Datum::UInt(u64::MAX)).unwrap();
        let right = StatBounds::new(Datum::Int(0), Datum::Int(0)).unwrap();

        assert_eq!(
            left.numeric_intersection(&right, NumericHistogramType::Int)?,
            None
        );
        Ok(())
    }

    #[test]
    fn test_numeric_comparison_restricts_each_original_bounds_type() -> Result<()> {
        let left = StatBounds::Int { min: -10, max: 10 };
        let right = StatBounds::UInt { min: 5, max: 15 };
        let (left_intersection, right_intersection) = left
            .numeric_intersection(&right, NumericHistogramType::Int)?
            .expect("mixed integer bounds should intersect");

        assert_eq!(left_intersection, StatBounds::Int { min: 5, max: 10 });
        assert_eq!(right_intersection, StatBounds::UInt { min: 5, max: 10 });
        Ok(())
    }

    #[test]
    fn test_float_comparison_preserves_integer_cast_collisions() -> Result<()> {
        let integer = StatBounds::Int {
            min: 9_007_199_254_740_992,
            max: 9_007_199_254_740_993,
        };
        let comparison = StatBounds::Float {
            min: F64::from(9_007_199_254_740_992.0),
            max: F64::from(9_007_199_254_740_992.0),
        };

        let (integer_intersection, float_intersection) = integer
            .numeric_intersection(&comparison, NumericHistogramType::Float)?
            .expect("integer bounds should intersect the float comparison bounds");
        assert_eq!(integer_intersection, integer);
        assert_eq!(float_intersection, comparison);
        Ok(())
    }

    #[test]
    fn test_contains_datum() {
        let bounds = StatBounds::new(Datum::Int(1), Datum::Int(3)).unwrap();
        assert!(bounds.contains_datum(&Datum::Int(1)));
        assert!(bounds.contains_datum(&Datum::Int(2)));
        assert!(bounds.contains_datum(&Datum::Int(3)));
        assert!(bounds.contains_datum(&Datum::UInt(2)));
        assert!(!bounds.contains_datum(&Datum::Int(0)));
        assert!(!bounds.contains_datum(&Datum::Int(4)));
        assert!(!bounds.contains_datum(&Datum::UInt(i64::MAX as u64 + 1)));
        assert!(
            !StatBounds::new(Datum::UInt(1), Datum::UInt(3))
                .unwrap()
                .contains_datum(&Datum::Int(-1))
        );
    }

    #[test]
    fn test_restrict_int_range_with_uint_edges() -> Result<()> {
        let bounds = StatBounds::new(Datum::Int(-10), Datum::Int(10))?;

        assert_eq!(
            bounds.restrict_by_range(
                &Bound::Included(Datum::UInt(i64::MAX as u64 + 1)),
                &Bound::Unbounded
            ),
            StatRangeBounds::Empty
        );
        assert_eq!(
            bounds.restrict_by_range(
                &Bound::Unbounded,
                &Bound::Included(Datum::UInt(i64::MAX as u64 + 1))
            ),
            StatRangeBounds::Bounds(StatBounds::Int { min: -10, max: 10 })
        );
        assert_eq!(
            bounds.restrict_by_range(&Bound::Unbounded, &Bound::Excluded(Datum::UInt(0))),
            StatRangeBounds::Bounds(StatBounds::Int { min: -10, max: -1 })
        );
        assert_eq!(
            bounds.restrict_by_range(&Bound::Excluded(Datum::UInt(u64::MAX)), &Bound::Unbounded),
            StatRangeBounds::Empty
        );
        assert_eq!(
            bounds.restrict_by_range(
                &Bound::Included(Datum::Float(F64::from(1.5))),
                &Bound::Unbounded
            ),
            StatRangeBounds::Imprecise
        );
        Ok(())
    }

    #[test]
    fn test_restrict_uint_range_with_int_edges() -> Result<()> {
        let bounds = StatBounds::new(Datum::UInt(1), Datum::UInt(10))?;

        assert_eq!(
            bounds.restrict_by_range(&Bound::Excluded(Datum::Int(-1)), &Bound::Unbounded),
            StatRangeBounds::Bounds(StatBounds::UInt { min: 1, max: 10 })
        );
        assert_eq!(
            bounds.restrict_by_range(&Bound::Unbounded, &Bound::Included(Datum::Int(-1))),
            StatRangeBounds::Empty
        );
        assert_eq!(
            bounds.restrict_by_range(&Bound::Unbounded, &Bound::Excluded(Datum::Int(0))),
            StatRangeBounds::Empty
        );
        assert_eq!(
            bounds.restrict_by_range(&Bound::Included(Datum::Int(5)), &Bound::Unbounded),
            StatRangeBounds::Bounds(StatBounds::UInt { min: 5, max: 10 })
        );
        assert_eq!(
            bounds.restrict_by_range(&Bound::Unbounded, &Bound::Excluded(Datum::Int(5))),
            StatRangeBounds::Bounds(StatBounds::UInt { min: 1, max: 4 })
        );
        Ok(())
    }

    #[test]
    fn test_restrict_float_singleton_excluded_endpoint_is_empty() -> Result<()> {
        let bounds = StatBounds::new(Datum::Float(F64::from(1.0)), Datum::Float(F64::from(1.0)))?;
        let value = Datum::Float(F64::from(1.0));

        assert_eq!(
            bounds.restrict_by_range(&Bound::Excluded(value.clone()), &Bound::Unbounded),
            StatRangeBounds::Empty
        );
        assert_eq!(
            bounds.restrict_by_range(&Bound::Unbounded, &Bound::Excluded(value.clone())),
            StatRangeBounds::Empty
        );
        assert_eq!(
            bounds.restrict_by_range(&Bound::Included(value.clone()), &Bound::Unbounded),
            StatRangeBounds::Bounds(StatBounds::Float {
                min: F64::from(1.0),
                max: F64::from(1.0),
            })
        );

        let range = StatBounds::new(Datum::Float(F64::from(1.0)), Datum::Float(F64::from(3.0)))?;
        assert_eq!(
            range.restrict_by_range(&Bound::Excluded(value), &Bound::Unbounded),
            StatRangeBounds::Bounds(StatBounds::Float {
                min: F64::from(1.0),
                max: F64::from(3.0),
            })
        );
        Ok(())
    }

    #[test]
    fn test_restrict_bytes_singleton_excluded_endpoint_is_empty() -> Result<()> {
        let bounds = StatBounds::new(Datum::Bytes(b"x".to_vec()), Datum::Bytes(b"x".to_vec()))?;
        let value = Datum::Bytes(b"x".to_vec());

        assert_eq!(
            bounds.restrict_by_range(&Bound::Excluded(value.clone()), &Bound::Unbounded),
            StatRangeBounds::Empty
        );
        assert_eq!(
            bounds.restrict_by_range(&Bound::Unbounded, &Bound::Excluded(value.clone())),
            StatRangeBounds::Empty
        );
        assert_eq!(
            bounds.restrict_by_range(&Bound::Included(value.clone()), &Bound::Unbounded),
            StatRangeBounds::Bounds(StatBounds::Bytes {
                min: b"x".to_vec(),
                max: b"x".to_vec(),
            })
        );

        let range = StatBounds::new(Datum::Bytes(b"a".to_vec()), Datum::Bytes(b"z".to_vec()))?;
        assert_eq!(
            range.restrict_by_range(&Bound::Excluded(value), &Bound::Unbounded),
            StatRangeBounds::Bounds(StatBounds::Bytes {
                min: b"x".to_vec(),
                max: b"z".to_vec(),
            })
        );
        Ok(())
    }
}
