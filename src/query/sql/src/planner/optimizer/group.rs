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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::optimizer::cost::CostContext;
use crate::optimizer::m_expr::MExpr;
use crate::optimizer::property::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::IndexType;

/// State of a `Group`
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum GroupState {
    Init,
    Explored,
}

impl GroupState {
    pub fn explored(&self) -> bool {
        matches!(self, GroupState::Explored)
    }
}

/// `Group` is a set of logically equivalent relational expressions represented with `MExpr`.
#[derive(Clone)]
pub struct Group {
    pub(crate) group_index: IndexType,
    pub(crate) m_exprs: Vec<MExpr>,

    /// Relational property shared by expressions in a same `Group`
    pub(crate) relational_prop: Arc<RelationalProperty>,

    /// Mapping from required property to best expression.
    /// This will be updated when optimizing a group
    pub(crate) best_props: HashMap<RequiredProperty, CostContext>,

    /// Stat info shared by expressions in a same `Group`
    pub(crate) stat_info: Arc<StatInfo>,

    pub(crate) state: GroupState,
}

impl Group {
    pub fn create(
        index: IndexType,
        relational_prop: Arc<RelationalProperty>,
        stat_info: Arc<StatInfo>,
    ) -> Self {
        Group {
            group_index: index,
            m_exprs: vec![],
            relational_prop,
            best_props: Default::default(),
            stat_info,
            state: GroupState::Init,
        }
    }

    pub fn group_index(&self) -> IndexType {
        self.group_index
    }

    pub fn num_exprs(&self) -> usize {
        self.m_exprs.len()
    }

    pub fn insert(&mut self, m_expr: MExpr) -> Result<()> {
        self.m_exprs.push(m_expr);
        Ok(())
    }

    pub fn set_state(&mut self, state: GroupState) {
        self.state = state;
    }

    pub fn m_expr(&self, index: IndexType) -> Result<&MExpr> {
        self.m_exprs
            .get(index)
            .ok_or_else(|| ErrorCode::Internal(format!("MExpr index {} not found", index)))
    }

    pub fn m_expr_mut(&mut self, index: IndexType) -> Result<&mut MExpr> {
        self.m_exprs
            .get_mut(index)
            .ok_or_else(|| ErrorCode::Internal(format!("MExpr index {} not found", index)))
    }

    pub fn best_prop(&self, required_prop: &RequiredProperty) -> Option<&CostContext> {
        self.best_props.get(required_prop)
    }

    pub fn best_prop_mut(&mut self, required_prop: &RequiredProperty) -> Option<&mut CostContext> {
        self.best_props.get_mut(required_prop)
    }

    /// Update the best cost of a required property.
    pub fn update_best_cost(&mut self, prop: &RequiredProperty, ccx: CostContext) {
        self.best_props
            .entry(prop.clone())
            .and_modify(|c| {
                if ccx.cost < c.cost {
                    *c = ccx.clone();
                }
            })
            .or_insert(ccx);
    }
}
