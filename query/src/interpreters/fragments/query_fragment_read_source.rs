use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::interpreters::fragments::query_fragment::QueryFragment;
use common_exception::Result;
use common_planners::{EmptyPlan, Partitions, PlanNode, ReadDataSourcePlan};
use crate::api::FlightAction;
use crate::interpreters::fragments::query_fragment_actions::{QueryFragmentAction, QueryFragmentActions, QueryFragmentsActions};
use crate::sessions::QueryContext;

pub struct ReadDatasourceQueryFragment {
    ctx: Arc<QueryContext>,
    read_data_source: ReadDataSourcePlan,
}

impl ReadDatasourceQueryFragment {
    pub fn create(ctx: Arc<QueryContext>, plan: &ReadDataSourcePlan) -> Result<Box<dyn QueryFragment>> {
        Ok(Box::new(ReadDatasourceQueryFragment { ctx, read_data_source: plan.clone() }))
    }
}

impl ReadDatasourceQueryFragment {
    pub fn repartition(&self, destinations_id: &[String]) -> Vec<Partitions> {
        // We always put adjacent partitions in the same node
        let partitions = &self.read_data_source.parts;
        let parts_per_node = partitions.len() / destinations_id.len();

        let mut nodes_parts = Vec::with_capacity(destinations_id.len());
        for index in 0..destinations_id.len() {
            let begin = parts_per_node * index;
            let end = parts_per_node * (index + 1);
            let node_parts = partitions[begin..end].to_vec();

            nodes_parts.push(node_parts);
        }

        // For some irregular partitions, we assign them to the head nodes
        let begin = parts_per_node * destinations_id.len();
        let remain_cluster_parts = &partitions[begin..];
        for index in 0..remain_cluster_parts.len() {
            nodes_parts[index].push(remain_cluster_parts[index].clone());
        }

        nodes_parts
    }
}

impl QueryFragment for ReadDatasourceQueryFragment {
    fn get_out_partition(&self) -> Result<PartitionState> {
        let read_table = self.ctx.build_table_from_source_plan(&self.read_data_source)?;

        match read_table.is_local() {
            true => Ok(PartitionState::NotPartition),
            false => Ok(PartitionState::Random),
        }
    }

    fn finalize(&self, actions: &mut QueryFragmentsActions) -> Result<()> {
        let mut fragment_actions = QueryFragmentActions::create();

        match self.get_out_partition()? {
            PartitionState::NotPartition => {
                fragment_actions.add_action(QueryFragmentAction::create(
                    actions.get_local_destination_id(),
                    PlanNode::ReadSource(self.read_data_source.clone()),
                ));
            }
            _ => {
                let destinations_id = actions.get_destinations_id();
                let new_partitions = self.repartition(&destinations_id);

                for (index, destinations_id) in destinations_id.iter().enumerate() {
                    fragment_actions.add_action(QueryFragmentAction::create(
                        destinations_id.to_owned(),
                        PlanNode::ReadSource(ReadDataSourcePlan {
                            parts: new_partitions[index].to_owned(),
                            source_info: self.read_data_source.source_info.clone(),
                            scan_fields: self.read_data_source.scan_fields.clone(),
                            statistics: self.read_data_source.statistics.clone(),
                            description: self.read_data_source.description.clone(),
                            tbl_args: self.read_data_source.tbl_args.clone(),
                            push_downs: self.read_data_source.push_downs.clone(),
                        }),
                    ))
                }
            }
        }

        actions.add_fragment_actions(fragment_actions)
    }
}

impl Debug for ReadDatasourceQueryFragment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadDatasourceQueryFragment")
            .finish()
    }
}
