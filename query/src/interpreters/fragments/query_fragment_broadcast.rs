use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use common_datavalues::DataSchemaRef;
use common_planners::{AggregatorFinalPlan, AggregatorPartialPlan, AlterTableClusterKeyPlan, AlterUserPlan, AlterUserUDFPlan, AlterViewPlan, BroadcastPlan, CallPlan, CopyPlan, CreateDatabasePlan, CreateRolePlan, CreateTablePlan, CreateUserPlan, CreateUserStagePlan, CreateUserUDFPlan, CreateViewPlan, DeletePlan, DescribeTablePlan, DescribeUserStagePlan, DropDatabasePlan, DropRolePlan, DropTableClusterKeyPlan, DropTablePlan, DropUserPlan, DropUserStagePlan, DropUserUDFPlan, DropViewPlan, EmptyPlan, ExistsTablePlan, ExplainPlan, Expression, ExpressionPlan, Expressions, FilterPlan, GrantPrivilegePlan, GrantRolePlan, HavingPlan, InsertPlan, KillPlan, LimitByPlan, LimitPlan, ListPlan, OptimizeTablePlan, PlanBuilder, PlanNode, PlanRewriter, ProjectionPlan, ReadDataSourcePlan, RemotePlan, RemoveUserStagePlan, RenameDatabasePlan, RenameTablePlan, RevokePrivilegePlan, RevokeRolePlan, SelectPlan, SettingPlan, ShowCreateDatabasePlan, ShowCreateTablePlan, ShowPlan, SinkPlan, SortPlan, SubQueriesSetPlan, TruncateTablePlan, UndropDatabasePlan, UndropTablePlan, UseDatabasePlan, WindowFuncPlan};
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::interpreters::fragments::QueryFragment;
use crate::interpreters::{QueryFragmentAction, QueryFragmentActions, QueryFragmentsActions};
use crate::sessions::QueryContext;
use common_exception::{ErrorCode, Result};
use crate::api::BroadcastExchange;

pub struct BroadcastQueryFragment {
    id: usize,
    ctx: Arc<QueryContext>,
    broadcast_plan: BroadcastPlan,
    input: Box<dyn QueryFragment>,
}

impl BroadcastQueryFragment {
    pub fn create(
        ctx: Arc<QueryContext>,
        node: &BroadcastPlan,
        input: Box<dyn QueryFragment>,
    ) -> Result<Box<dyn QueryFragment>> {
        let id = ctx.get_and_inc_fragment_id();
        Ok(Box::new(BroadcastQueryFragment {
            id,
            ctx,
            input,
            broadcast_plan: node.clone(),
        }))
    }
}

impl QueryFragment for BroadcastQueryFragment {
    fn distribute_query(&self) -> Result<bool> {
        Ok(true)
    }

    fn get_out_partition(&self) -> Result<PartitionState> {
        Ok(PartitionState::Broadcast)
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
                    .rewrite_remote_plan(&self.broadcast_plan.input, &action.node)?,
            );

            fragment_actions.add_action(fragment_action);
        } else {
            // We run exchange data on the current hosts
            for action in input_actions.get_actions() {
                let fragment_action = QueryFragmentAction::create(
                    action.executor.clone(),
                    self.input
                        .rewrite_remote_plan(&self.broadcast_plan.input, &action.node)?,
                );

                fragment_actions.add_action(fragment_action);
            }
        }

        fragment_actions.set_exchange(BroadcastExchange::create(actions.get_executors()));

        match input_actions.exchange_actions {
            true => actions.add_fragment_actions(fragment_actions),
            false => actions.update_root_fragment_actions(fragment_actions),
        }
    }

    fn rewrite_remote_plan(&self, node: &PlanNode, _: &PlanNode) -> Result<PlanNode> {
        let query_id = self.ctx.get_id();
        let mut stage_rewrite = BroadcastRewrite::create(query_id, self.id);
        stage_rewrite.rewrite_plan_node(node)
    }
}

struct BroadcastRewrite {
    query_id: String,
    fragment_id: usize,
}

impl BroadcastRewrite {
    pub fn create(query_id: String, fragment_id: usize) -> BroadcastRewrite {
        BroadcastRewrite {
            query_id,
            fragment_id,
        }
    }
}

impl PlanRewriter for BroadcastRewrite {
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

    fn rewrite_broadcast(&mut self, broadcast: &BroadcastPlan) -> Result<PlanNode> {
        Ok(PlanNode::Remote(RemotePlan::create_v2(
            broadcast.schema(),
            self.query_id.to_owned(),
            self.fragment_id,
        )))
    }
}

impl Debug for BroadcastQueryFragment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}
