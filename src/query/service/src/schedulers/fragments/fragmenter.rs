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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::cluster_info::Cluster;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_types::NodeInfo;
use databend_common_sql::executor::physical_plans::FragmentKind;

use crate::clusters::ClusterHelper;
use crate::physical_plans::BroadcastSink;
use crate::physical_plans::CompactSource;
use crate::physical_plans::ConstantTableScan;
use crate::physical_plans::DeriveHandle;
use crate::physical_plans::Exchange;
use crate::physical_plans::ExchangeSink;
use crate::physical_plans::ExchangeSource;
use crate::physical_plans::MutationSource;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanDynExt;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::PhysicalPlanVisitor;
use crate::physical_plans::Recluster;
use crate::physical_plans::ReplaceInto;
use crate::physical_plans::TableScan;
use crate::schedulers::fragments::plan_fragment::FragmentType;
use crate::schedulers::PlanFragment;
use crate::servers::flight::v1::exchange::BroadcastExchange;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::MergeExchange;
use crate::servers::flight::v1::exchange::ShuffleDataExchange;
use crate::sessions::QueryContext;

/// Visitor to split a `PhysicalPlan` into fragments.
pub struct Fragmenter {
    ctx: Arc<QueryContext>,
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

    pub fn build_fragment(self, plan: &PhysicalPlan) -> Result<Vec<PlanFragment>> {
        let mut handle = FragmentDeriveHandle::create(self.query_id.clone(), self.ctx.clone());
        let root = plan.derive_with(&mut handle);
        let mut fragments = {
            let handle = handle
                .as_any()
                .downcast_mut::<FragmentDeriveHandle>()
                .unwrap();
            handle.take_fragments()
        };

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
            let Some(fragment) = fragments.get_mut(&source) else {
                continue;
            };

            if let Some(exchange_sink) = fragment.plan.downcast_mut_ref::<ExchangeSink>() {
                exchange_sink.destination_fragment_id = target;
            }
        }

        Ok(fragments.into_values().collect::<Vec<_>>())
    }

    fn collect_fragments_edge<'a>(
        iter: impl Iterator<Item = &'a PlanFragment>,
    ) -> HashMap<usize, usize> {
        struct EdgeVisitor {
            target_fragment_id: usize,
            map: HashMap<usize, usize>,
        }

        impl EdgeVisitor {
            pub fn create(target_fragment_id: usize) -> Box<dyn PhysicalPlanVisitor> {
                Box::new(EdgeVisitor {
                    target_fragment_id,
                    map: Default::default(),
                })
            }

            pub fn take(&mut self) -> HashMap<usize, usize> {
                std::mem::take(&mut self.map)
            }
        }

        impl PhysicalPlanVisitor for EdgeVisitor {
            fn as_any(&mut self) -> &mut dyn Any {
                self
            }

            fn visit(&mut self, plan: &PhysicalPlan) -> Result<()> {
                if let Some(v) = plan.downcast_ref::<ExchangeSource>() {
                    if let Some(v) = self
                        .map
                        .insert(v.source_fragment_id, self.target_fragment_id)
                    {
                        assert_eq!(v, self.target_fragment_id);
                    }
                }

                Ok(())
            }
        }

        let mut edges = HashMap::new();
        for fragment in iter {
            let mut visitor = EdgeVisitor::create(fragment.fragment_id);
            fragment.plan.visit(&mut visitor).unwrap();
            if let Some(v) = visitor.as_any().downcast_mut::<EdgeVisitor>() {
                edges.extend(v.take().into_iter())
            }
        }

        edges
    }
}

struct FragmentDeriveHandle {
    state: State,
    query_id: String,
    ctx: Arc<QueryContext>,
    fragments: HashMap<usize, PlanFragment>,
}

impl FragmentDeriveHandle {
    pub fn create(query_id: String, ctx: Arc<QueryContext>) -> Box<dyn DeriveHandle> {
        Box::new(FragmentDeriveHandle {
            ctx,
            query_id,
            state: State::Other,
            fragments: HashMap::new(),
        })
    }

    pub fn take_fragments(&mut self) -> HashMap<usize, PlanFragment> {
        std::mem::take(&mut self.fragments)
    }

    pub fn get_exchange(
        cluster: Arc<Cluster>,
        plan: &PhysicalPlan,
    ) -> Result<Option<DataExchange>> {
        let Some(exchange_sink) = plan.downcast_ref::<ExchangeSink>() else {
            return Ok(None);
        };

        let get_executors = |cluster: Arc<Cluster>| {
            let cluster_nodes = cluster.get_nodes();

            cluster_nodes
                .iter()
                .map(|node| &node.id)
                .cloned()
                .collect::<Vec<_>>()
        };

        Ok(match exchange_sink.kind {
            FragmentKind::Normal => Some(ShuffleDataExchange::create(
                get_executors(cluster),
                exchange_sink.keys.clone(),
            )),
            FragmentKind::Merge => Some(MergeExchange::create(
                cluster.local_id(),
                exchange_sink.ignore_exchange,
                exchange_sink.allow_adjust_parallelism,
            )),
            FragmentKind::Expansive => Some(BroadcastExchange::create(get_executors(cluster))),
            FragmentKind::Init => None,
        })
    }
}

impl DeriveHandle for FragmentDeriveHandle {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn derive(
        &mut self,
        v: &PhysicalPlan,
        mut children: Vec<PhysicalPlan>,
    ) -> std::result::Result<PhysicalPlan, Vec<PhysicalPlan>> {
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
            let input_schema = input.output_schema().unwrap();

            let plan_id = v.get_id();
            let source_fragment_id = self.ctx.get_fragment_id();

            let plan: PhysicalPlan = Box::new(ExchangeSink {
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
            let cluster = self.ctx.get_cluster();
            let exchange = Self::get_exchange(cluster, &plan).unwrap();

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
