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

use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Exchange {
    Broadcast,
    Merge,
    MergeSort, // For distributed sort
    NodeToNodeHash(Vec<ScalarExpr>),
}

impl Operator for Exchange {
    fn rel_op(&self) -> RelOp {
        RelOp::Exchange
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        if let Exchange::NodeToNodeHash(hash_keys) = self {
            Box::new(hash_keys.iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        rel_expr.derive_relational_prop_child(0)
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: match self {
                Exchange::NodeToNodeHash(hash_keys) => {
                    Distribution::NodeToNodeHash(hash_keys.clone())
                }
                Exchange::Broadcast => Distribution::Broadcast,
                Exchange::Merge | Exchange::MergeSort => Distribution::Serial,
            },
        })
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        rel_expr.derive_cardinality_child(0)
    }
}
