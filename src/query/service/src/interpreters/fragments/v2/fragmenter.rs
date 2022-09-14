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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_legacy_planners::StageKind;

use super::FragmentType;
use super::PlanFragment;
use crate::api::BroadcastExchange;
use crate::api::DataExchange;
use crate::api::MergeExchange;
use crate::api::ShuffleDataExchangeV2;
use crate::clusters::ClusterHelper;
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

    /// A state to track if is visiting a source pipeline.
    visiting_source_pipeline: bool,
}

impl Fragmenter {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        let query_id = ctx.get_id();

        Ok(Self {
            ctx,
            fragments: vec![],
            visiting_source_pipeline: false,
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

    /// Get ids of current executor node.
    /// This method is basically copied from `QueryFragmentActions::get_executors()`.
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
                StageKind::Normal => Ok(Some(ShuffleDataExchangeV2::create(
                    Self::get_executors(ctx),
                    plan.keys.clone(),
                ))),
                StageKind::Merge => Ok(Some(MergeExchange::create(Self::get_local_executor(ctx)))),
                StageKind::Expansive => Ok(Some(BroadcastExchange::create(
                    from_multiple_nodes,
                    Self::get_executors(ctx),
                ))),
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
        for input in fragment.source_fragments.iter_mut() {
            if let PhysicalPlan::ExchangeSink(ExchangeSink {
                destination_fragment_id,
                ..
            }) = &mut input.plan
            {
                // Fill the destination_fragment_id with parent fragment id.
                *destination_fragment_id = fragment.fragment_id;
            }
        }
    }
}

impl PhysicalPlanReplacer for Fragmenter {
    fn replace_table_scan(&mut self, plan: &TableScan) -> Result<PhysicalPlan> {
        self.visiting_source_pipeline = true;

        Ok(PhysicalPlan::TableScan(plan.clone()))
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
            build: Box::new(build_input),
            probe: Box::new(probe_input),
            build_keys: plan.build_keys.clone(),
            probe_keys: plan.probe_keys.clone(),
            other_conditions: plan.other_conditions.clone(),
            join_type: plan.join_type.clone(),
            marker_index: plan.marker_index,
            from_correlated_subquery: plan.from_correlated_subquery,
        }))
    }

    fn replace_exchange(&mut self, plan: &Exchange) -> Result<PhysicalPlan> {
        // Recursively rewrite input
        let input = self.replace(plan.input.as_ref())?;
        let input_schema = input.output_schema()?;

        let source_fragment_id = self.ctx.get_fragment_id();
        let plan = PhysicalPlan::ExchangeSink(ExchangeSink {
            input: Box::new(input),
            schema: input_schema.clone(),
            kind: plan.kind.clone(),
            keys: plan.keys.clone(),

            destinations: Self::get_executors(self.ctx.clone()),
            query_id: self.query_id.clone(),

            // We will connect the fragments later, so we just
            // set the fragment id to a invalid value here.
            destination_fragment_id: usize::MAX,
        });
        let fragment_type = if self.visiting_source_pipeline {
            debug_assert!(self.fragments.is_empty());
            self.visiting_source_pipeline = false;
            FragmentType::Source
        } else {
            FragmentType::Intermidiate
        };
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
            schema: input_schema,
            query_id: self.query_id.clone(),

            source_fragment_id,
        }))
    }
}
