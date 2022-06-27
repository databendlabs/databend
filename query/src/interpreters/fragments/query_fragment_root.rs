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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;

use crate::api::MergeExchange;
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::interpreters::fragments::query_fragment::QueryFragment;
use crate::interpreters::fragments::query_fragment_actions::QueryFragmentAction;
use crate::interpreters::fragments::query_fragment_actions::QueryFragmentActions;
use crate::interpreters::fragments::query_fragment_actions::QueryFragmentsActions;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct RootQueryFragment {
    ctx: Arc<QueryContext>,
    node: PlanNode,
    input: Box<dyn QueryFragment>,
}

impl RootQueryFragment {
    pub fn create(
        input: Box<dyn QueryFragment>,
        ctx: Arc<QueryContext>,
        node: &PlanNode,
    ) -> Result<Box<dyn QueryFragment>> {
        Ok(Box::new(RootQueryFragment {
            input,
            ctx,
            node: node.clone(),
        }))
    }
}

impl QueryFragment for RootQueryFragment {
    fn distribute_query(&self) -> Result<bool> {
        self.input.distribute_query()
    }

    fn get_out_partition(&self) -> Result<PartitionState> {
        Ok(PartitionState::NotPartition)
    }

    fn finalize(&self, actions: &mut QueryFragmentsActions) -> Result<()> {
        self.input.finalize(actions)?;
        let input_actions = actions.get_root_actions()?;
        let fragment_id = self.ctx.get_fragment_id();
        let mut fragment_actions = QueryFragmentActions::create(false, fragment_id);

        if PartitionState::NotPartition == self.input.get_out_partition()? {
            if input_actions.get_actions().is_empty() {
                return Err(ErrorCode::LogicalError(
                    "Logical error, input actions is empty.",
                ));
            }

            let action = &input_actions.get_actions()[0];

            fragment_actions.add_action(QueryFragmentAction::create(
                actions.get_local_executor(),
                self.input.rewrite_remote_plan(&self.node, &action.node)?,
            ));
        } else {
            // This is an implicit stage. We run remaining plans on the current hosts
            for action in input_actions.get_actions() {
                fragment_actions.add_action(QueryFragmentAction::create(
                    action.executor.clone(),
                    self.input.rewrite_remote_plan(&self.node, &action.node)?,
                ));
            }

            fragment_actions.set_exchange(MergeExchange::create(actions.get_local_executor()));
        }

        match input_actions.exchange_actions {
            true => actions.add_fragment_actions(fragment_actions),
            false => actions.update_root_fragment_actions(fragment_actions),
        }
    }

    fn rewrite_remote_plan(&self, node: &PlanNode, _: &PlanNode) -> Result<PlanNode> {
        // Do nothing, We will not call this method on RootQueryFragment
        Ok(node.clone())
    }
}
