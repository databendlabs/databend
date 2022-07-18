use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use common_planners::{PlanNode, SubQueriesSetPlan};
use crate::interpreters::fragments::QueryFragment;
use crate::sessions::QueryContext;
use common_exception::Result;
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::interpreters::QueryFragmentsActions;

pub struct SubQueriesFragment {
    // id: usize,
    node: SubQueriesSetPlan,
    ctx: Arc<QueryContext>,
    input: Box<dyn QueryFragment>,
    subqueries_fragment: HashMap<String, Box<dyn QueryFragment>>,
}

impl SubQueriesFragment {
    pub fn create(
        ctx: Arc<QueryContext>,
        node: &SubQueriesSetPlan,
        input: Box<dyn QueryFragment>,
        subqueries_fragment: HashMap<String, Box<dyn QueryFragment>>,
    ) -> Result<Box<dyn QueryFragment>> {
        Ok(Box::new(SubQueriesFragment {
            ctx,
            input,
            subqueries_fragment,
            node: node.clone(),
        }))
    }
}

impl QueryFragment for SubQueriesFragment {
    fn distribute_query(&self) -> Result<bool> {
        if self.input.distribute_query()? {
            return Ok(true);
        }

        for (_name, fragment) in &self.subqueries_fragment {
            if fragment.distribute_query()? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn get_out_partition(&self) -> Result<PartitionState> {
        self.input.get_out_partition()
    }

    fn finalize(&self, actions: &mut QueryFragmentsActions) -> Result<()> {
        self.input.finalize(actions)?;
        let root_actions = actions.pop_root_actions().unwrap();

        for (_name, subquery_fragment) in &self.subqueries_fragment {
            let ctx = self.ctx.clone();
            let mut fragments_actions = QueryFragmentsActions::create(ctx);
            subquery_fragment.finalize(&mut fragments_actions)?;

            while let Some(fragment_actions) = fragments_actions.pop_root_actions() {
                actions.add_fragment_actions(fragment_actions)?;
            }
        }

        actions.add_fragment_actions(root_actions)?;
        Ok(())
    }

    fn rewrite_remote_plan(&self, node: &PlanNode, new: &PlanNode) -> Result<PlanNode> {
        todo!()
    }
}

impl Debug for SubQueriesFragment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}
