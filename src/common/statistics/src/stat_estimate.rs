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

use std::fmt;

#[derive(Clone, Copy, PartialEq)]
pub struct StatEstimate {
    pub lower: f64,
    pub expected: f64,
    pub upper: f64,
}

impl fmt::Debug for StatEstimate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.lower == self.expected && self.expected == self.upper {
            write!(f, "{:?}", self.expected)
        } else {
            write!(
                f,
                "~{:?}[{:?}, {:?}]",
                self.expected, self.lower, self.upper
            )
        }
    }
}

impl StatEstimate {
    pub fn check_consistency(&self) -> Result<(), String> {
        if !self.lower.is_finite() || !self.expected.is_finite() || !self.upper.is_finite() {
            return Err(format!("estimate must be finite: {:?}", self));
        }
        if self.lower < 0.0 {
            return Err(format!(
                "estimate lower bound must be non-negative: {:?}",
                self
            ));
        }
        if self.lower > self.expected || self.expected > self.upper {
            return Err(format!("estimate bounds are inconsistent: {:?}", self));
        }
        Ok(())
    }

    /// Constructs an estimate from already-derived bounds.
    ///
    /// Clamping belongs in the operation that changes the estimate, not here.
    pub fn new(lower: f64, expected: f64, upper: f64) -> Self {
        let estimate = Self {
            lower,
            expected,
            upper,
        };
        debug_assert!(
            estimate.check_consistency().is_ok(),
            "invalid estimate: {:?}",
            estimate
        );
        estimate
    }

    /// Constructs an exact estimate from a known non-negative value.
    pub fn exact(value: f64) -> Self {
        let estimate = Self {
            lower: value,
            expected: value,
            upper: value,
        };
        debug_assert!(
            estimate.check_consistency().is_ok(),
            "invalid exact estimate: {:?}",
            estimate
        );
        estimate
    }

    pub fn reduce(self, upper: f64) -> Self {
        let upper = upper.max(0.0);
        Self::new(
            self.lower.min(upper),
            self.expected.min(upper),
            self.upper.min(upper),
        )
    }

    pub fn reduce_by_selectivity(self, selectivity: f64) -> Self {
        let selectivity = selectivity.clamp(0.0, 1.0);
        Self::new(
            self.lower * selectivity,
            self.expected * selectivity,
            self.upper * selectivity,
        )
    }

    pub fn min(self, other: Self) -> Self {
        StatEstimate::new(
            self.lower.min(other.lower),
            self.expected.min(other.expected),
            self.upper.min(other.upper),
        )
    }

    pub(crate) fn add(self, other: Self) -> Self {
        Self::new(
            self.lower + other.lower,
            self.expected + other.expected,
            self.upper + other.upper,
        )
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum StatCount {
    Exact(u64),
    Estimate { expected: f64, upper: f64 },
}

impl fmt::Debug for StatCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatCount::Exact(value) => write!(f, "{value:?}"),
            StatCount::Estimate { expected, upper } => {
                if expected == upper {
                    write!(f, "~{expected:?}")
                } else {
                    write!(f, "~{expected:?}[..{upper:?}]")
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum StatCardinality {
    Exact(u64),
    Estimate(f64),
}

impl StatCardinality {
    pub fn exact(value: u64) -> Self {
        Self::Exact(value)
    }

    pub fn estimate(value: f64) -> Self {
        let cardinality = Self::Estimate(value);
        debug_assert!(
            cardinality.check_consistency().is_ok(),
            "invalid cardinality estimate: {:?}",
            cardinality
        );
        cardinality
    }

    pub fn value(self) -> f64 {
        match self {
            StatCardinality::Exact(value) => value as f64,
            StatCardinality::Estimate(value) => value,
        }
    }

    pub fn exact_value(self) -> Option<u64> {
        match self {
            StatCardinality::Exact(value) => Some(value),
            StatCardinality::Estimate(_) => None,
        }
    }

    pub fn is_zero(self) -> bool {
        self.value() == 0.0
    }

    pub fn as_null_count(self) -> StatCount {
        match self {
            StatCardinality::Exact(value) => StatCount::exact(value),
            StatCardinality::Estimate(value) => StatCount::estimate(value, f64::INFINITY),
        }
    }

    pub fn check_consistency(self) -> Result<(), String> {
        match self {
            StatCardinality::Exact(_) => Ok(()),
            StatCardinality::Estimate(value) => {
                if !value.is_finite() {
                    return Err(format!("cardinality estimate must be finite: {self:?}"));
                }
                if value < 0.0 {
                    return Err(format!(
                        "cardinality estimate must be non-negative: {self:?}"
                    ));
                }
                Ok(())
            }
        }
    }
}

impl StatCount {
    pub fn exact(value: u64) -> Self {
        Self::Exact(value)
    }

    pub fn expected(self) -> f64 {
        match self {
            StatCount::Exact(value) => value as f64,
            StatCount::Estimate { expected, .. } => expected,
        }
    }

    pub fn upper(self) -> f64 {
        match self {
            StatCount::Exact(value) => value as f64,
            StatCount::Estimate { upper, .. } => upper,
        }
    }

    pub fn exact_value(self) -> Option<u64> {
        match self {
            StatCount::Exact(value) => Some(value),
            StatCount::Estimate { .. } => None,
        }
    }

    /// Constructs an estimated count. `upper` may be infinite, but the
    /// expected value must be finite and already within bounds.
    pub fn estimate(expected: f64, upper: f64) -> Self {
        let count = Self::Estimate { expected, upper };
        debug_assert!(
            count.check_consistency().is_ok(),
            "invalid count estimate: {:?}",
            count
        );
        count
    }

    pub fn check_consistency(self) -> Result<(), String> {
        match self {
            StatCount::Exact(_) => Ok(()),
            StatCount::Estimate { expected, upper } => {
                if !expected.is_finite() {
                    return Err(format!("count expected must be finite: {self:?}"));
                }
                if upper.is_nan() {
                    return Err(format!("count upper must not be NaN: {self:?}"));
                }
                if expected < 0.0 {
                    return Err(format!("count expected must be non-negative: {self:?}"));
                }
                if upper < expected {
                    return Err(format!("count upper must not be below expected: {self:?}"));
                }
                Ok(())
            }
        }
    }

    pub fn reduce(self, upper: f64) -> Self {
        let upper = upper.max(0.0);
        match self {
            StatCount::Exact(0) => StatCount::Exact(0),
            _ => StatCount::estimate(self.expected().min(upper), self.upper().min(upper)),
        }
    }

    pub fn reduce_by_selectivity(self, selectivity: f64) -> Self {
        let selectivity = selectivity.clamp(0.0, 1.0);
        if selectivity == 1.0 {
            return self;
        }
        if selectivity == 0.0 {
            return StatCount::Exact(0);
        }
        let expected = self.expected() * selectivity;
        let upper = self.upper() * selectivity;
        if upper == 0.0 {
            StatCount::Exact(0)
        } else {
            StatCount::estimate(expected, upper)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stat_count_keeps_exactness_explicit() {
        assert_eq!(StatCount::exact(10).exact_value(), Some(10));
        assert_eq!(
            StatCount::exact(10).reduce_by_selectivity(0.5),
            StatCount::estimate(5.0, 5.0)
        );
        assert_eq!(
            StatCount::exact(10).reduce(5.0),
            StatCount::estimate(5.0, 5.0)
        );
        assert_eq!(
            StatCount::estimate(10.0, f64::INFINITY).reduce_by_selectivity(0.0),
            StatCount::exact(0)
        );
    }
}
