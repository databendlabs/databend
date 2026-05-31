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

pub fn estimate_distinct_count_after_row_scale(
    num_values: f64,
    num_distinct: f64,
    row_scale: f64,
) -> f64 {
    if num_values <= 0.0 || num_distinct <= 0.0 || row_scale <= 0.0 {
        return 0.0;
    }

    let scaled_num_values = num_values * row_scale;
    if row_scale >= 1.0 {
        return num_distinct.min(scaled_num_values);
    }

    let rows_per_distinct = num_values / num_distinct;
    let survival_probability = 1.0 - (1.0 - row_scale).powf(rows_per_distinct);
    (num_distinct * survival_probability)
        .min(num_distinct)
        .min(scaled_num_values)
}

#[must_use]
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
        debug_assert!(
            !upper.is_nan() && upper >= 0.0,
            "invalid reduction upper bound: {upper:?}"
        );
        Self::new(
            self.lower.min(upper),
            self.expected.min(upper),
            self.upper.min(upper),
        )
    }

    pub fn reduce_by_selectivity(self, selectivity: f64) -> Self {
        debug_assert!(
            selectivity.is_finite() && (0.0..=1.0).contains(&selectivity),
            "invalid selectivity: {selectivity:?}"
        );
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

#[must_use]
#[derive(Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NdvEstimate {
    pub expected: Option<f64>,
    pub upper: f64,
}

impl fmt::Debug for NdvEstimate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.expected {
            Some(expected) if expected == self.upper => write!(f, "{:?}", expected),
            Some(expected) => write!(f, "~{:?}[..{:?}]", expected, self.upper),
            None => write!(f, "?[..{:?}]", self.upper),
        }
    }
}

impl NdvEstimate {
    pub fn check_consistency(&self) -> Result<(), String> {
        if self.expected.is_some_and(|expected| !expected.is_finite()) || !self.upper.is_finite() {
            return Err(format!("NDV estimate must be finite: {:?}", self));
        }
        if self.expected.is_some_and(|expected| expected < 0.0) || self.upper < 0.0 {
            return Err(format!("NDV estimate must be non-negative: {:?}", self));
        }
        if self.expected.is_some_and(|expected| expected > self.upper) {
            return Err(format!("NDV estimate bounds are inconsistent: {:?}", self));
        }
        Ok(())
    }

    pub fn new(expected: f64, upper: f64) -> Self {
        let estimate = Self {
            expected: Some(expected),
            upper,
        };
        debug_assert!(
            estimate.check_consistency().is_ok(),
            "invalid NDV estimate: {:?}",
            estimate
        );
        estimate
    }

    pub fn exact(value: f64) -> Self {
        Self::new(value, value)
    }

    pub fn upper_bound(upper: f64) -> Self {
        let estimate = Self {
            expected: None,
            upper,
        };
        debug_assert!(
            estimate.check_consistency().is_ok(),
            "invalid NDV estimate: {:?}",
            estimate
        );
        estimate
    }

    pub fn is_upper_only(self) -> bool {
        self.expected.is_none()
    }

    pub fn reduce(self, upper: f64) -> Self {
        debug_assert!(
            !upper.is_nan() && upper >= 0.0,
            "invalid NDV reduction upper bound: {upper:?}"
        );
        let upper = self.upper.min(upper);
        match self.expected {
            Some(expected) => Self::new(expected.min(upper), upper),
            None => Self::upper_bound(upper),
        }
    }

    pub fn reduce_by_selectivity(self, num_values: f64, selectivity: f64) -> Self {
        debug_assert!(
            selectivity.is_finite() && (0.0..=1.0).contains(&selectivity),
            "invalid selectivity: {selectivity:?}"
        );
        debug_assert!(
            num_values.is_finite() && num_values >= 0.0,
            "invalid NDV scaling row count: {num_values:?}"
        );
        let upper = self.upper.min(num_values * selectivity);
        if let Some(expected) = self.expected {
            let expected =
                estimate_distinct_count_after_row_scale(num_values, expected, selectivity);
            Self::new(expected.min(upper), upper)
        } else {
            Self::upper_bound(upper)
        }
    }

    pub fn min(self, other: Self) -> Self {
        let upper = self.upper.min(other.upper);
        match (self.expected, other.expected) {
            (Some(left), Some(right)) => Self::new(left.min(right).min(upper), upper),
            _ => Self::upper_bound(upper),
        }
    }

    pub(crate) fn add(self, other: Self) -> Self {
        let upper = self.upper + other.upper;
        match (self.expected, other.expected) {
            (Some(left), Some(right)) => Self::new(left + right, upper),
            _ => Self::upper_bound(upper),
        }
    }
}

#[must_use]
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

#[must_use]
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
        debug_assert!(
            !upper.is_nan() && upper >= 0.0,
            "invalid count reduction upper bound: {upper:?}"
        );
        match self {
            StatCount::Exact(0) => StatCount::Exact(0),
            _ => StatCount::estimate(self.expected().min(upper), self.upper().min(upper)),
        }
    }

    pub fn reduce_by_selectivity(self, selectivity: f64) -> Self {
        debug_assert!(
            selectivity.is_finite() && (0.0..=1.0).contains(&selectivity),
            "invalid count selectivity: {selectivity:?}"
        );
        if self == StatCount::Exact(0) {
            return self;
        }
        match selectivity {
            1.0 => self,
            0.0 => StatCount::estimate(0.0, 0.0),
            _ => {
                let expected = self.expected() * selectivity;
                let upper = self.upper() * selectivity;
                StatCount::estimate(expected, upper)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::NdvEstimate;
    use super::estimate_distinct_count_after_row_scale;

    #[test]
    fn test_distinct_count_row_scale_estimate() {
        let estimate = estimate_distinct_count_after_row_scale(100.0, 10.0, 0.25);

        assert!((estimate - 9.436864852905273).abs() < 1e-9);
    }

    #[test]
    fn test_ndv_selectivity_uses_row_scale_estimate() {
        let ndv = NdvEstimate::exact(10.0).reduce_by_selectivity(100.0, 0.25);

        assert_eq!(ndv.upper, 10.0);
        assert!((ndv.expected.unwrap() - 9.436864852905273).abs() < 1e-9);
    }

    #[test]
    fn test_upper_only_ndv_does_not_gain_expected_by_reduction() {
        let ndv = NdvEstimate::upper_bound(100.0).reduce(10.0);

        assert_eq!(ndv.upper, 10.0);
        assert!(ndv.is_upper_only());
        assert_eq!(ndv.expected, None);
    }

    #[test]
    fn test_upper_only_ndv_min_with_expected_preserves_upper_only() {
        let ndv = NdvEstimate::exact(20.0).min(NdvEstimate::upper_bound(10.0));

        assert_eq!(ndv, NdvEstimate::upper_bound(10.0));

        let ndv = NdvEstimate::upper_bound(10.0).min(NdvEstimate::exact(20.0));

        assert_eq!(ndv, NdvEstimate::upper_bound(10.0));
    }
}
