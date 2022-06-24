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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::PlanRewriter;
use common_planners::RemotePlan;
use common_planners::StageKind;
use common_planners::StagePlan;

use crate::api::MergeExchange;
use crate::api::ShuffleDataExchange;
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::interpreters::fragments::query_fragment::QueryFragment;
use crate::interpreters::fragments::query_fragment_actions::QueryFragmentAction;
use crate::interpreters::fragments::query_fragment_actions::QueryFragmentActions;
use crate::interpreters::fragments::query_fragment_actions::QueryFragmentsActions;
use crate::sessions::QueryContext;

pub struct StageQueryFragment {
    id: usize,
    stage: StagePlan,
    ctx: Arc<QueryContext>,
    input: Box<dyn QueryFragment>,
}

impl StageQueryFragment {
    pub fn create(
        ctx: Arc<QueryContext>,
        node: &StagePlan,
        input: Box<dyn QueryFragment>,
    ) -> Result<Box<dyn QueryFragment>> {
        let id = ctx.get_fragment_id();
        Ok(Box::new(StageQueryFragment {
            id,
            stage: node.clone(),
            ctx,
            input,
        }))
    }
}

impl QueryFragment for StageQueryFragment {
    fn distribute_query(&self) -> Result<bool> {
        Ok(true)
    }

    fn get_out_partition(&self) -> Result<PartitionState> {
        match self.stage.kind {
            StageKind::Normal => Ok(PartitionState::HashPartition),
            StageKind::Expansive => Ok(PartitionState::HashPartition),
            StageKind::Merge => Ok(PartitionState::NotPartition),
        }
    }

    fn finalize(&self, actions: &mut QueryFragmentsActions) -> Result<()> {
        self.input.finalize(actions)?;
        let input_actions = actions.get_root_actions()?;
        let mut fragment_actions = QueryFragmentActions::create(true, self.id);

        if self.input.get_out_partition()? == PartitionState::NotPartition {
            if input_actions.get_actions().is_empty() {
                return Err(ErrorCode::LogicalError(
                    "Logical error, input actions is empty.",
                ));
            }

            let action = &input_actions.get_actions()[0];
            let fragment_action = QueryFragmentAction::create(
                actions.get_local_executor(),
                self.input
                    .rewrite_remote_plan(&self.stage.input, &action.node)?,
            );

            fragment_actions.add_action(fragment_action);
        } else {
            // We run exchange data on the current hosts
            for action in input_actions.get_actions() {
                let fragment_action = QueryFragmentAction::create(
                    action.executor.clone(),
                    self.input
                        .rewrite_remote_plan(&self.stage.input, &action.node)?,
                );

                fragment_actions.add_action(fragment_action);
            }
        }

        fragment_actions.set_exchange(match self.stage.kind {
            StageKind::Expansive => unimplemented!(),
            StageKind::Merge => MergeExchange::create(actions.get_local_executor()),
            StageKind::Normal => ShuffleDataExchange::create(
                actions.get_executors(),
                self.stage.scatters_expr.clone(),
            ),
        });

        match input_actions.exchange_actions {
            true => actions.add_fragment_actions(fragment_actions),
            false => actions.update_root_fragment_actions(fragment_actions),
        }
    }

    fn rewrite_remote_plan(&self, node: &PlanNode, _: &PlanNode) -> Result<PlanNode> {
        let query_id = self.ctx.get_id();
        let mut stage_rewrite = StageRewrite::create(query_id, self.id);
        stage_rewrite.rewrite_plan_node(node)
    }
}

struct StageRewrite {
    query_id: String,
    fragment_id: usize,
}

impl StageRewrite {
    pub fn create(query_id: String, fragment_id: usize) -> StageRewrite {
        StageRewrite {
            query_id,
            fragment_id,
        }
    }
}

impl PlanRewriter for StageRewrite {
    fn rewrite_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<PlanNode> {
        PlanBuilder::from(&self.rewrite_plan_node(&plan.input)?)
            .aggregate_partial(&plan.aggr_expr, &plan.group_expr)?
            .build()
    }

    fn rewrite_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<PlanNode> {
        let schema = plan.schema_before_group_by.clone();
        let new_input = self.rewrite_plan_node(&plan.input)?;

        PlanBuilder::from(&new_input)
            .aggregate_final(schema, &plan.aggr_expr, &plan.group_expr)?
            .build()
    }

    fn rewrite_stage(&mut self, stage: &StagePlan) -> Result<PlanNode> {
        Ok(PlanNode::Remote(RemotePlan::create_v2(
            stage.schema(),
            self.query_id.to_owned(),
            self.fragment_id,
        )))
    }
}

impl Debug for StageQueryFragment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StageQueryFragment")
            .field("input", &self.input)
            .finish()
    }
}
