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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_sql::executor::CopyIntoTablePhysicalPlan;
use common_sql::executor::CopyIntoTableSource;
use common_sql::executor::FragmentKind;
use common_sql::executor::QuerySource;
use common_sql::executor::ReplaceInto;

use crate::api::BroadcastExchange;
use crate::api::DataExchange;
use crate::api::MergeExchange;
use crate::api::ShuffleDataExchange;
use crate::clusters::ClusterHelper;
use crate::schedulers::fragments::plan_fragment::FragmentType;
use crate::schedulers::PlanFragment;
use crate::sessions::QueryContext;
use crate::sql::executor::Exchange;
use crate::sql::executor::ExchangeSink;
use crate::sql::executor::ExchangeSource;
use crate::sql::executor::HashJoin;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PhysicalPlanReplacer;
use crate::sql::executor::TableScan;

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
enum State {
    SelectLeaf,
    DeleteLeaf,
    ReplaceInto,
    Compact,
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
        let cluster = ctx.get_cluster();
        let cluster_nodes = cluster.get_nodes();

        cluster_nodes.iter().map(|node| &node.id).cloned().collect()
    }

    pub fn get_local_executor(ctx: Arc<QueryContext>) -> String {
        ctx.get_cluster().local_id()
    }

    pub fn get_exchange(
        ctx: Arc<QueryContext>,
        plan: &PhysicalPlan,
        from_multiple_nodes: bool,
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
                ))),
                FragmentKind::Expansive => Ok(Some(BroadcastExchange::create(
                    from_multiple_nodes,
                    Self::get_executors(ctx),
                ))),
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

    fn replace_replace_into(
        &mut self,
        plan: &common_sql::executor::ReplaceInto,
    ) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        self.state = State::ReplaceInto;

        Ok(PhysicalPlan::ReplaceInto(Box::new(ReplaceInto {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    //  TODO(Sky): remove rebudant code
    fn replace_copy_into_table(
        &mut self,
        plan: &CopyIntoTablePhysicalPlan,
    ) -> Result<PhysicalPlan> {
        match &plan.source {
            CopyIntoTableSource::Stage(_) => {
                self.state = State::SelectLeaf;
                Ok(PhysicalPlan::CopyIntoTable(Box::new(plan.clone())))
            }
            CopyIntoTableSource::Query(query_ctx) => {
                let input = self.replace(&query_ctx.plan)?;
                Ok(PhysicalPlan::CopyIntoTable(Box::new(
                    CopyIntoTablePhysicalPlan {
                        source: CopyIntoTableSource::Query(Box::new(QuerySource {
                            plan: input,
                            ..*query_ctx.clone()
                        })),
                        ..plan.clone()
                    },
                )))
            }
        }
    }

    fn replace_compact_source(
        &mut self,
        plan: &common_sql::executor::CompactSource,
    ) -> Result<PhysicalPlan> {
        self.state = State::Compact;

        Ok(PhysicalPlan::CompactSource(Box::new(plan.clone())))
    }

    fn replace_delete_source(
        &mut self,
        plan: &common_sql::executor::DeleteSource,
    ) -> Result<PhysicalPlan> {
        self.state = State::DeleteLeaf;

        Ok(PhysicalPlan::DeleteSource(Box::new(plan.clone())))
    }

    fn replace_hash_join(&mut self, plan: &HashJoin) -> Result<PhysicalPlan> {
        let mut fragments = vec![];
        let probe_input = self.replace(plan.probe.as_ref())?;

        // Consume current fragments to prevent them being consumed by `build_input`.
        fragments.append(&mut self.fragments);
        let build_input = self.replace(plan.build.as_ref())?;

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
            non_equi_conditions: plan.non_equi_conditions.clone(),
            join_type: plan.join_type.clone(),
            marker_index: plan.marker_index,
            from_correlated_subquery: plan.from_correlated_subquery,
            probe_to_build: plan.probe_to_build.clone(),
            output_schema: plan.output_schema.clone(),
            contain_runtime_filter: plan.contain_runtime_filter,
            stat_info: plan.stat_info.clone(),
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
        });
        let fragment_type = match self.state {
            State::SelectLeaf => FragmentType::Source,
            State::DeleteLeaf => FragmentType::DeleteLeaf,
            State::Other => FragmentType::Intermediate,
            State::ReplaceInto => FragmentType::ReplaceInto,
            State::Compact => FragmentType::Compact,
        };
        self.state = State::Other;
        let exchange = Self::get_exchange(
            self.ctx.clone(),
            &plan,
            self.fragments
                .iter()
                .all(|fragment| !matches!(&fragment.exchange, Some(DataExchange::Merge(_)))),
        )?;

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
