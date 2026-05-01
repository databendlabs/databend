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

    pub fn new(lower: f64, expected: f64, upper: f64) -> Self {
        let lower = lower.max(0.0);
        let upper = upper.max(lower);
        let expected = expected.clamp(lower, upper);
        Self {
            lower,
            expected,
            upper,
        }
    }

    pub fn exact(value: f64) -> Self {
        let value = value.max(0.0);
        Self {
            lower: value,
            expected: value,
            upper: value,
        }
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
