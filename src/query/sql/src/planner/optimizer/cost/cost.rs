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

use std::fmt::Display;
use std::ops::Add;
use std::ops::AddAssign;

use databend_common_exception::Result;

use crate::optimizer::MExpr;
use crate::optimizer::Memo;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RequiredProperty;
use crate::IndexType;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Cost(pub f64);

impl<T> From<T> for Cost
where T: Into<f64>
{
    fn from(t: T) -> Self {
        Cost(t.into())
    }
}

impl Display for Cost {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:.3}", self.0)
    }
}

impl Add for Cost {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Cost(self.0 + rhs.0)
    }
}

impl AddAssign for Cost {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

pub trait CostModel: Send {
    /// Compute cost of given `MExpr`(children are not encapsulated).
    fn compute_cost(&self, memo: &Memo, m_expr: &MExpr) -> Result<Cost>;
}

/// Context of best cost within a group.
#[derive(Debug, Clone)]
pub struct CostContext {
    pub group_index: IndexType,
    pub expr_index: IndexType,
    pub cost: Cost,
    pub physical_prop: PhysicalProperty,
    pub children_required_props: Vec<RequiredProperty>,
}
