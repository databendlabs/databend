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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_types::NodeInfo;
use databend_common_sql::executor::physical_plans::CompactSource;
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
use databend_common_sql::executor::PhysicalPlanReplacer;

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
    fragments: Vec<PlanFragment>,
    query_id: String,
    state: State,
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

    pub fn build_fragment(mut self, plan: &PhysicalPlan) -> Result<PlanFragment> {
        let root = self.replace(plan)?;
        let mut root_fragment = PlanFragment {
            plan: root,
            fragment_type: FragmentType::Root,
            fragment_id: self.ctx.get_fragment_id(),
            exchange: None,
            query_id: self.query_id.clone(),
            source_fragments: self.fragments,
        };
        Self::resolve_fragment_connection(&mut root_fragment);

        Ok(root_fragment)
    }

    fn resolve_fragment_connection(fragment: &mut PlanFragment) {
        for source_fragment in fragment.source_fragments.iter_mut() {
            if let PhysicalPlan::ExchangeSink(ExchangeSink {
                destination_fragment_id,
                ..
            }) = &mut source_fragment.plan
            {
                // Fill the destination_fragment_id with parent fragment id.
                *destination_fragment_id = fragment.fragment_id;
            }
        }
    }
}

impl PhysicalPlanReplacer for Fragmenter {
    fn replace_table_scan(&mut self, plan: &TableScan) -> Result<PhysicalPlan> {
        self.state = State::SelectLeaf;
        Ok(PhysicalPlan::TableScan(plan.clone()))
    }

    fn replace_constant_table_scan(&mut self, plan: &ConstantTableScan) -> Result<PhysicalPlan> {
        self.state = State::SelectLeaf;
        Ok(PhysicalPlan::ConstantTableScan(plan.clone()))
    }

    fn replace_mutation_source(&mut self, plan: &MutationSource) -> Result<PhysicalPlan> {
        self.state = State::MutationSource;
        Ok(PhysicalPlan::MutationSource(plan.clone()))
    }

    fn replace_mutation(&mut self, plan: &Mutation) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::Mutation(Box::new(Mutation {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    fn replace_replace_into(&mut self, plan: &ReplaceInto) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        self.state = State::ReplaceInto;
        Ok(PhysicalPlan::ReplaceInto(Box::new(ReplaceInto {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    //  TODO(Sky): remove redundant code
    fn replace_copy_into_table(&mut self, plan: &CopyIntoTable) -> Result<PhysicalPlan> {
        match &plan.source {
            CopyIntoTableSource::Stage(_) => {
                self.state = State::SelectLeaf;
                Ok(PhysicalPlan::CopyIntoTable(Box::new(plan.clone())))
            }
            CopyIntoTableSource::Query(query_physical_plan) => {
                let input = self.replace(query_physical_plan)?;
                Ok(PhysicalPlan::CopyIntoTable(Box::new(CopyIntoTable {
                    source: CopyIntoTableSource::Query(Box::new(input)),
                    ..plan.clone()
                })))
            }
        }
    }

    fn replace_recluster(&mut self, plan: &Recluster) -> Result<PhysicalPlan> {
        self.state = State::Recluster;
        Ok(PhysicalPlan::Recluster(Box::new(plan.clone())))
    }

    fn replace_compact_source(&mut self, plan: &CompactSource) -> Result<PhysicalPlan> {
        self.state = State::Compact;
        Ok(PhysicalPlan::CompactSource(Box::new(plan.clone())))
    }

    fn replace_hash_join(&mut self, plan: &HashJoin) -> Result<PhysicalPlan> {
        let mut fragments = vec![];
        let build_input = self.replace(plan.build.as_ref())?;

        // Consume current fragments to prevent them being consumed by `probe_input`.
        fragments.append(&mut self.fragments);
        let probe_input = self.replace(plan.probe.as_ref())?;
        fragments.append(&mut self.fragments);
        self.fragments = fragments;

        Ok(PhysicalPlan::HashJoin(HashJoin {
            plan_id: plan.plan_id,
            projections: plan.projections.clone(),
            probe_projections: plan.probe_projections.clone(),
            build_projections: plan.build_projections.clone(),
            build: Box::new(build_input),
            probe: Box::new(probe_input),
            build_keys: plan.build_keys.clone(),
            probe_keys: plan.probe_keys.clone(),
            is_null_equal: plan.is_null_equal.clone(),
            non_equi_conditions: plan.non_equi_conditions.clone(),
            join_type: plan.join_type.clone(),
            marker_index: plan.marker_index,
            from_correlated_subquery: plan.from_correlated_subquery,
            probe_to_build: plan.probe_to_build.clone(),
            output_schema: plan.output_schema.clone(),
            need_hold_hash_table: plan.need_hold_hash_table,
            stat_info: plan.stat_info.clone(),
            probe_keys_rt: plan.probe_keys_rt.clone(),
            enable_bloom_runtime_filter: plan.enable_bloom_runtime_filter,
            broadcast: plan.broadcast,
            single_to_inner: plan.single_to_inner.clone(),
            build_side_cache_info: plan.build_side_cache_info.clone(),
        }))
    }

    fn replace_union(&mut self, plan: &UnionAll) -> Result<PhysicalPlan> {
        let mut fragments = vec![];
        let left_input = self.replace(plan.left.as_ref())?;
        let left_state = self.state.clone();

        // Consume current fragments to prevent them being consumed by `right_input`.
        fragments.append(&mut self.fragments);
        let right_input = self.replace(plan.right.as_ref())?;
        let right_state = self.state.clone();

        fragments.append(&mut self.fragments);
        self.fragments = fragments;

        // If any of the input is a source fragment, the union all is a source fragment.
        if left_state == State::SelectLeaf || right_state == State::SelectLeaf {
            self.state = State::SelectLeaf;
        } else {
            self.state = State::Other;
        }

        Ok(PhysicalPlan::UnionAll(UnionAll {
            left: Box::new(left_input),
            right: Box::new(right_input),
            ..plan.clone()
        }))
    }

    fn replace_exchange(&mut self, plan: &Exchange) -> Result<PhysicalPlan> {
        // Recursively rewrite input
        let input = self.replace(plan.input.as_ref())?;
        let input_schema = input.output_schema()?;

        let plan_id = plan.plan_id;

        let source_fragment_id = self.ctx.get_fragment_id();
        let plan = PhysicalPlan::ExchangeSink(ExchangeSink {
            // TODO(leiysky): we reuse the plan id here,
            // should generate a new one for the sink.
            plan_id,

            input: Box::new(input),
            schema: input_schema.clone(),
            kind: plan.kind.clone(),
            keys: plan.keys.clone(),

            query_id: self.query_id.clone(),

            // We will connect the fragments later, so we just
            // set the fragment id to a invalid value here.
            destination_fragment_id: usize::MAX,
            ignore_exchange: plan.ignore_exchange,
            allow_adjust_parallelism: plan.allow_adjust_parallelism,
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

        let mut source_fragment = PlanFragment {
            plan,
            fragment_type,

            fragment_id: source_fragment_id,
            exchange,
            query_id: self.query_id.clone(),

            source_fragments: self.fragments.drain(..).collect(),
        };

        // Fill the destination_fragment_id for source fragments of `source_fragment`.
        Self::resolve_fragment_connection(&mut source_fragment);

        self.fragments.push(source_fragment);

        Ok(PhysicalPlan::ExchangeSource(ExchangeSource {
            // TODO(leiysky): we reuse the plan id here,
            // should generate a new one for the source.
            plan_id,

            schema: input_schema,
            query_id: self.query_id.clone(),

            source_fragment_id,
        }))
    }
}
