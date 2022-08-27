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

use common_exception::Result;

use super::JoinType;
use crate::sql::optimizer::Distribution;
use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RequiredProperty;
use crate::sql::plans::LogicalOperator;
use crate::sql::plans::Operator;
use crate::sql::plans::PhysicalOperator;
use crate::sql::plans::RelOp;
use crate::sql::plans::Scalar;
use crate::sql::IndexType;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PhysicalHashJoin {
    pub build_keys: Vec<Scalar>,
    pub probe_keys: Vec<Scalar>,
    pub other_conditions: Vec<Scalar>,
    pub join_type: JoinType,
    pub marker_index: Option<IndexType>,
    pub from_correlated_subquery: bool,
}

impl Operator for PhysicalHashJoin {
    fn rel_op(&self) -> RelOp {
        RelOp::PhysicalHashJoin
    }

    fn is_physical(&self) -> bool {
        true
    }

    fn is_logical(&self) -> bool {
        false
    }

    fn as_physical(&self) -> Option<&dyn PhysicalOperator> {
        Some(self)
    }

    fn as_logical(&self) -> Option<&dyn LogicalOperator> {
        None
    }
}

impl PhysicalOperator for PhysicalHashJoin {
    fn derive_physical_prop<'a>(&self, rel_expr: &RelExpr<'a>) -> Result<PhysicalProperty> {
        let probe_prop = rel_expr.derive_physical_prop_child(0)?;
        let build_prop = rel_expr.derive_physical_prop_child(1)?;

        match (&probe_prop.distribution, &build_prop.distribution) {
            // If the distribution of probe side is Random, we will pass through
            // the distribution of build side.
            (Distribution::Random, _) => Ok(PhysicalProperty {
                distribution: build_prop.distribution.clone(),
            }),

            (Distribution::Hash(probe_hash_keys), Distribution::Hash(build_hash_keys)) => {
                Ok(PhysicalProperty {
                    distribution: Distribution::Hash(
                        probe_hash_keys
                            .iter()
                            .chain(build_hash_keys.iter())
                            .cloned()
                            .collect(),
                    ),
                })
            }

            // Otherwise pass through probe side.
            _ => Ok(PhysicalProperty {
                distribution: probe_prop.distribution.clone(),
            }),
        }
    }

    fn compute_required_prop_child<'a>(
        &self,
        rel_expr: &RelExpr<'a>,
        child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();

        let probe_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        let build_physical_prop = rel_expr.derive_physical_prop_child(1)?;

        if probe_physical_prop.distribution == Distribution::Serial
            || build_physical_prop.distribution == Distribution::Serial
        {
            // TODO(leiysky): we can enforce redistribution here
            required.distribution = Distribution::Serial;
        } else {
            // A simple heuristic, we will always enforce `Hash` distribution for join
            if child_index == 0 {
                required.distribution = Distribution::Hash(self.probe_keys.clone());
            } else {
                required.distribution = Distribution::Hash(self.build_keys.clone());
            }
        }

        Ok(required)
    }
}
