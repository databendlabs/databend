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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_functions::aggregates::assert_arguments;
use databend_common_meta_types::NodeInfo;
use databend_common_sql::executor::physical_plans::{BroadcastSink, CompactSource};
use databend_common_sql::executor::physical_plans::ConstantTableScan;
use databend_common_sql::executor::physical_plans::CopyIntoTable;
use databend_common_sql::executor::physical_plans::CopyIntoTableSource;
use databend_common_sql::executor::physical_plans::Exchange;
use databend_common_sql::executor::physical_plans::ExchangeSink;
use databend_common_sql::executor::physical_plans::ExchangeSource;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::HashJoin;
use databend_common_sql::executor::physical_plans::MutationSource;
use databend_common_sql::executor::physical_plans::Recluster;
use databend_common_sql::executor::physical_plans::ReplaceInto;
use databend_common_sql::executor::physical_plans::TableScan;
use databend_common_sql::executor::physical_plans::UnionAll;
use databend_common_sql::executor::{DeriveHandle, PhysicalPlanDynExt, PhysicalPlanMeta, PhysicalPlanVisitor};
use databend_common_sql::executor::IPhysicalPlan;

use crate::clusters::ClusterHelper;
use crate::schedulers::fragments::plan_fragment::FragmentType;
use crate::schedulers::PlanFragment;
use crate::servers::flight::v1::exchange::BroadcastExchange;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::MergeExchange;
use crate::servers::flight::v1::exchange::ShuffleDataExchange;
use crate::sessions::QueryContext;
use crate::sql::executor::physical_plans::Mutation;
use crate::sql::executor::PhysicalPlan;

/// Visitor to split a `PhysicalPlan` into fragments.
pub struct Fragmenter {
    ctx: Arc<QueryContext>,
    state: State,
    query_id: String,
    fragments: Vec<PlanFragment>,
}

/// A state to track if is visiting a source fragment, useful when building fragments.
///
/// SelectLeaf: visiting a source fragment of select statement.
///
/// DeleteLeaf: visiting a source fragment of delete statement.
///
/// Replace: visiting a fragment that contains a replace into plan.
#[derive(Clone, Debug, Eq, PartialEq)]
enum State {
    SelectLeaf,
    MutationSource,
    ReplaceInto,
    Compact,
    Recluster,
    Other,
}

impl Fragmenter {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        let query_id = ctx.get_id();

        Ok(Self {
            ctx,
            fragments: vec![],
            state: State::Other,
            query_id,
        })
    }

    /// Get ids of executor nodes.
    /// This method is basically copied from `QueryFragmentActions::get_executors()`.
    pub fn get_executors(ctx: Arc<QueryContext>) -> Vec<String> {
        let cluster_nodes = Self::get_executors_nodes(ctx);

        cluster_nodes.iter().map(|node| &node.id).cloned().collect()
    }

    pub fn get_executors_nodes(ctx: Arc<QueryContext>) -> Vec<Arc<NodeInfo>> {
        ctx.get_cluster().get_nodes()
    }

    pub fn get_local_executor(ctx: Arc<QueryContext>) -> String {
        ctx.get_cluster().local_id()
    }

    pub fn get_exchange(
        ctx: Arc<QueryContext>,
        plan: &PhysicalPlan,
    ) -> Result<Option<DataExchange>> {
        match plan {
            PhysicalPlan::ExchangeSink(plan) => match plan.kind {
                FragmentKind::Normal => Ok(Some(ShuffleDataExchange::create(
                    Self::get_executors(ctx),
                    plan.keys.clone(),
                ))),
                FragmentKind::Merge => Ok(Some(MergeExchange::create(
                    Self::get_local_executor(ctx),
                    plan.ignore_exchange,
                    plan.allow_adjust_parallelism,
                ))),
                FragmentKind::Expansive => {
                    Ok(Some(BroadcastExchange::create(Self::get_executors(ctx))))
                }
                _ => Ok(None),
            },
            _ => Ok(None),
        }
    }

    pub fn build_fragment(mut self, plan: &Box<dyn IPhysicalPlan>) -> Result<Vec<PlanFragment>> {
        let mut fragments = HashMap::new();
        let mut handle = FragmentDeriveHandle::new(self.query_id.clone(), self.ctx.clone(), &mut fragments);
        let root = plan.derive_with(&mut handle);

        let mut fragment_type = FragmentType::Root;
        if let Some(_broadcast_sink) = plan.downcast_ref::<BroadcastSink>() {
            fragment_type = FragmentType::Intermediate;
        }

        fragments.insert(self.ctx.get_fragment_id(), PlanFragment {
            plan: root,
            fragment_type,
            fragment_id: self.ctx.get_fragment_id(),
            exchange: None,
            query_id: self.query_id.clone(),
            source_fragments: self.fragments,
        });

        let edges = Self::collect_fragments_edge(fragments.values());

        for (source, target) in edges {
            if let Some(exchange_sink) = fragments[&source].plan.downcast_mut_ref::<ExchangeSink>() {
                exchange_sink.destination_fragment_id = target;
            }
        }

        Ok(fragments.into_values().collect::<Vec<_>>())
    }

    fn collect_fragments_edge<'a>(iter: impl Iterator<Item=&'a PlanFragment>) -> HashMap<usize, usize> {
        struct EdgeVisitor {
            target_fragment_id: usize,
            map: HashMap<usize, usize>,
        }

        impl PhysicalPlanVisitor for EdgeVisitor {
            fn visit(&mut self, plan: &Box<dyn IPhysicalPlan>) -> Result<()> {
                if let Some(v) = plan.downcast_ref::<ExchangeSource>() {
                    if let Some(v) = self.map.insert(v.source_fragment_id, self.target_fragment_id) {
                        assert_eq!(v, self.target_fragment_id);
                    }
                }

                Ok(())
            }
        }

        let mut edges = HashMap::new();
        for fragment in iter {
            let mut visitor = Box::new(EdgeVisitor { map: HashMap::new(), target_fragment_id: fragment.fragment_id });
            fragment.plan.visit(&mut visitor).unwrap();
            edges.extend(visitor.map.into_iter())
        }

        edges
    }
}

struct FragmentDeriveHandle<'a> {
    state: State,
    query_id: String,
    ctx: Arc<QueryContext>,
    fragments: &'a mut HashMap<usize, PlanFragment>,
}

impl<'a> FragmentDeriveHandle<'a> {
    pub fn new(query_id: String, ctx: Arc<QueryContext>, fragments: &'a mut HashMap<usize, PlanFragment>) -> Box<dyn DeriveHandle> {
        Box::new(FragmentDeriveHandle {
            ctx,
            query_id,
            fragments,
            state: State::Other,
        })
    }
}

impl<'a> DeriveHandle for FragmentDeriveHandle<'a> {
    fn derive(
        &mut self,
        v: &Box<dyn IPhysicalPlan>,
        children: Vec<Box<dyn IPhysicalPlan>>,
    ) -> std::result::Result<Box<dyn IPhysicalPlan>, Vec<Box<dyn IPhysicalPlan>>> {
        if let Some(_recluster) = v.downcast_ref::<Recluster>() {
            self.state = State::Recluster;
        } else if let Some(_table_scan) = v.downcast_ref::<TableScan>() {
            self.state = State::SelectLeaf;
        } else if let Some(_const_table_scan) = v.downcast_ref::<ConstantTableScan>() {
            self.state = State::SelectLeaf;
        } else if let Some(_compact_source) = v.downcast_ref::<CompactSource>() {
            self.state = State::Compact;
        } else if let Some(_replace_into) = v.downcast_ref::<ReplaceInto>() {
            self.state = State::ReplaceInto;
        } else if let Some(_mutation_source) = v.downcast_ref::<MutationSource>() {
            self.state = State::MutationSource;
        }

        if let Some(exchange) = v.downcast_ref::<Exchange>() {
            let input = children.remove(0);
            let input_schema = input.output_schema()?;

            let plan_id = v.get_id();
            let source_fragment_id = self.ctx.get_fragment_id();

            let plan = Box::new(ExchangeSink {
                input,
                schema: input_schema.clone(),
                kind: exchange.kind.clone(),
                keys: exchange.keys.clone(),

                query_id: self.query_id.clone(),

                // We will connect the fragments later, so we just
                // set the fragment id to a invalid value here.
                destination_fragment_id: usize::MAX,
                ignore_exchange: exchange.ignore_exchange,
                allow_adjust_parallelism: exchange.allow_adjust_parallelism,
                meta: PhysicalPlanMeta::with_plan_id("ExchangeSink", plan_id),
            });

            let fragment_type = match self.state {
                State::SelectLeaf => FragmentType::Source,
                State::MutationSource => FragmentType::MutationSource,
                State::Other => FragmentType::Intermediate,
                State::ReplaceInto => FragmentType::ReplaceInto,
                State::Compact => FragmentType::Compact,
                State::Recluster => FragmentType::Recluster,
            };

            self.state = State::Other;
            let exchange = Self::get_exchange(self.ctx.clone(), &plan)?;

            let source_fragment = PlanFragment {
                plan,
                exchange,
                fragment_type,
                source_fragments: vec![],
                fragment_id: source_fragment_id,
                query_id: self.query_id.clone(),
            };

            self.fragments.insert(source_fragment_id, source_fragment);

            return Ok(Box::new(ExchangeSource {
                schema: input_schema,
                query_id: self.query_id.clone(),

                source_fragment_id,
                meta: PhysicalPlanMeta::with_plan_id("ExchangeSource", plan_id),
            }));
        }

        Err(children)
    }
}
// impl PhysicalPlanReplacer for Fragmenter {

//     fn replace_hash_join(&mut self, plan: &HashJoin) -> Result<PhysicalPlan> {
//         let mut fragments = vec![];
//         let build_input = self.replace(plan.build.as_ref())?;
//
//         // Consume current fragments to prevent them being consumed by `probe_input`.
//         fragments.append(&mut self.fragments);
//         let probe_input = self.replace(plan.probe.as_ref())?;
//         fragments.append(&mut self.fragments);
//
//         self.fragments = fragments;
//
//         Ok(PhysicalPlan::HashJoin(HashJoin {
//             plan_id: plan.plan_id,
//             projections: plan.projections.clone(),
//             probe_projections: plan.probe_projections.clone(),
//             build_projections: plan.build_projections.clone(),
//             build: Box::new(build_input),
//             probe: Box::new(probe_input),
//             build_keys: plan.build_keys.clone(),
//             probe_keys: plan.probe_keys.clone(),
//             is_null_equal: plan.is_null_equal.clone(),
//             non_equi_conditions: plan.non_equi_conditions.clone(),
//             join_type: plan.join_type.clone(),
//             marker_index: plan.marker_index,
//             from_correlated_subquery: plan.from_correlated_subquery,
//             probe_to_build: plan.probe_to_build.clone(),
//             output_schema: plan.output_schema.clone(),
//             need_hold_hash_table: plan.need_hold_hash_table,
//             stat_info: plan.stat_info.clone(),
//             single_to_inner: plan.single_to_inner.clone(),
//             build_side_cache_info: plan.build_side_cache_info.clone(),
//             runtime_filter: plan.runtime_filter.clone(),
//             broadcast_id: plan.broadcast_id,
//         }))
//     }
//
//     fn replace_union(&mut self, plan: &UnionAll) -> Result<PhysicalPlan> {
//         let mut fragments = vec![];
//         let left_input = self.replace(plan.left.as_ref())?;
//         let left_state = self.state.clone();
//
//         // Consume current fragments to prevent them being consumed by `right_input`.
//         fragments.append(&mut self.fragments);
//         let right_input = self.replace(plan.right.as_ref())?;
//         let right_state = self.state.clone();
//
//         fragments.append(&mut self.fragments);
//         self.fragments = fragments;
//
//         // If any of the input is a source fragment, the union all is a source fragment.
//         if left_state == State::SelectLeaf || right_state == State::SelectLeaf {
//             self.state = State::SelectLeaf;
//         } else {
//             self.state = State::Other;
//         }
//
//         Ok(PhysicalPlan::UnionAll(UnionAll {
//             left: Box::new(left_input),
//             right: Box::new(right_input),
//             ..plan.clone()
//         }))
//     }
// }
