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

use databend_common_exception::Result;
use educe::Educe;

use crate::optimizer::cascades::tasks::SharedCounter;
use crate::optimizer::cascades::CascadesOptimizer;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleFactory;
use crate::optimizer::RuleID;
use crate::IndexType;

#[derive(Educe)]
#[educe(Debug)]
pub struct ApplyRuleTask {
    #[educe(Debug(ignore))]
    pub rule_id: RuleID,
    pub target_group_index: IndexType,
    pub m_expr_index: IndexType,

    pub parent: Option<SharedCounter>,
}

impl ApplyRuleTask {
    pub fn new(rule_id: RuleID, target_group_index: IndexType, m_expr_index: IndexType) -> Self {
        Self {
            rule_id,
            target_group_index,
            m_expr_index,
            parent: None,
        }
    }

    pub fn with_parent(mut self, parent: SharedCounter) -> Self {
        self.parent = Some(parent);
        self
    }

    pub fn execute(self, optimizer: &mut CascadesOptimizer) -> Result<()> {
        let group = optimizer.memo.group(self.target_group_index)?;
        let m_expr = group.m_expr(self.m_expr_index)?;
        let mut state = TransformResult::new();
        let rule = RuleFactory::create_rule(self.rule_id, optimizer.metadata.clone())?;
        m_expr.apply_rule(&optimizer.memo, &rule, &mut state)?;
        optimizer.insert_from_transform_state(self.target_group_index, state)?;

        Ok(())
    }
}
