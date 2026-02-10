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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_base::uniq_id::GlobalUniq;
use databend_common_catalog::cluster_info::Cluster;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_meta_types::NodeInfo;

use crate::clusters::ClusterHelper;
use crate::physical_plans::BroadcastSink;
use crate::physical_plans::CompactSource;
use crate::physical_plans::ConstantTableScan;
use crate::physical_plans::DeriveHandle;
use crate::physical_plans::Exchange;
use crate::physical_plans::ExchangeSink;
use crate::physical_plans::ExchangeSource;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::MaterializedCTE;
use crate::physical_plans::MutationSource;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanCast;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::PhysicalPlanVisitor;
use crate::physical_plans::Recluster;
use crate::physical_plans::ReplaceInto;
use crate::physical_plans::Sequence;
use crate::physical_plans::TableScan;
use crate::physical_plans::VisitorCast;
use crate::schedulers::PlanFragment;
use crate::schedulers::fragments::plan_fragment::FragmentType;
use crate::servers::flight::v1::exchange::BroadcastExchange;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::MergeExchange;
use crate::servers::flight::v1::exchange::NodeToNodeExchange;
use crate::sessions::QueryContext;

/// Visitor to split a `PhysicalPlan` into fragments.
pub struct Fragmenter {
    ctx: Arc<QueryContext>,
    query_id: String,
    fragments: Vec<PlanFragment>,
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
        if BroadcastSink::check_physical_plan(plan) {
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

            if let Some(exchange_sink) = ExchangeSink::from_mut_physical_plan(&mut fragment.plan) {
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
                if let Some(v) = ExchangeSource::from_physical_plan(plan) {
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
    query_id: String,
    ctx: Arc<QueryContext>,
    fragments: BTreeMap<usize, PlanFragment>,
}

impl FragmentDeriveHandle {
    pub fn create(query_id: String, ctx: Arc<QueryContext>) -> Box<dyn DeriveHandle> {
        Box::new(FragmentDeriveHandle {
            ctx,
            query_id,
            fragments: BTreeMap::new(),
        })
    }

    pub fn take_fragments(&mut self) -> BTreeMap<usize, PlanFragment> {
        std::mem::take(&mut self.fragments)
    }

    pub fn get_exchange(
        cluster: Arc<Cluster>,
        plan: &PhysicalPlan,
    ) -> Result<Option<DataExchange>> {
        let Some(exchange_sink) = ExchangeSink::from_physical_plan(plan) else {
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
            FragmentKind::Init => None,
            FragmentKind::Normal => {
                let destination_ids = get_executors(cluster);

                let mut destination_channels = Vec::with_capacity(destination_ids.len());

                for destination in &destination_ids {
                    destination_channels.push((destination.clone(), vec![GlobalUniq::unique()]));
                }

                Some(DataExchange::NodeToNodeExchange(NodeToNodeExchange {
                    destination_ids,
                    destination_channels,
                    shuffle_keys: exchange_sink.keys.clone(),
                    allow_adjust_parallelism: exchange_sink.allow_adjust_parallelism,
                }))
            }
            FragmentKind::Merge => Some(MergeExchange::create(
                cluster.local_id(),
                exchange_sink.ignore_exchange,
                exchange_sink.allow_adjust_parallelism,
            )),
            FragmentKind::Expansive => Some(BroadcastExchange::create(get_executors(cluster))),
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
        if let Some(exchange) = Exchange::from_physical_plan(v) {
            let input = children.remove(0);
            let input_schema = input.output_schema().unwrap();

            let plan_id = v.get_id();
            let source_fragment_id = self.ctx.get_fragment_id();

            let plan: PhysicalPlan = PhysicalPlan::new(ExchangeSink {
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

            let mut visitor = FragmentTypeVisitor::create();
            plan.visit(&mut visitor).unwrap();

            let fragment_type_visitor = FragmentTypeVisitor::from_visitor(&mut visitor);
            let fragment_type = fragment_type_visitor.fragment_type.clone();

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

            return Ok(PhysicalPlan::new(ExchangeSource {
                schema: input_schema,
                query_id: self.query_id.clone(),

                source_fragment_id,
                meta: PhysicalPlanMeta::with_plan_id("ExchangeSource", plan_id),
            }));
        }

        if Sequence::check_physical_plan(v) {
            assert_eq!(children.len(), 2);
            return Ok(children.remove(1));
        }

        if let Some(materialized_cte) = MaterializedCTE::from_physical_plan(v) {
            let plan = materialized_cte.derive(children);

            let mut visitor = FragmentTypeVisitor::create();
            plan.visit(&mut visitor).unwrap();

            let fragment_type_visitor = FragmentTypeVisitor::from_visitor(&mut visitor);
            let fragment_type = fragment_type_visitor.fragment_type.clone();

            let fragment_id = self.ctx.get_fragment_id();
            let fragment = PlanFragment {
                plan: plan.clone(),
                fragment_type,
                fragment_id,
                exchange: None,
                query_id: self.query_id.clone(),
                source_fragments: vec![],
            };

            self.fragments.insert(fragment_id, fragment);
            return Ok(plan);
        }

        Err(children)
    }
}

struct FragmentTypeVisitor {
    fragment_type: FragmentType,
}

impl FragmentTypeVisitor {
    pub fn create() -> Box<dyn PhysicalPlanVisitor> {
        Box::new(FragmentTypeVisitor {
            fragment_type: FragmentType::Intermediate,
        })
    }
}

impl PhysicalPlanVisitor for FragmentTypeVisitor {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn visit(&mut self, v: &PhysicalPlan) -> Result<()> {
        if Recluster::check_physical_plan(v) {
            self.fragment_type = FragmentType::Recluster;
        }

        if TableScan::check_physical_plan(v) {
            self.fragment_type = FragmentType::Source;
        }

        if ConstantTableScan::check_physical_plan(v) {
            self.fragment_type = FragmentType::Source;
        }

        if CompactSource::check_physical_plan(v) {
            self.fragment_type = FragmentType::Compact;
        }

        if ReplaceInto::check_physical_plan(v) {
            self.fragment_type = FragmentType::ReplaceInto;
        }

        if MutationSource::check_physical_plan(v) {
            self.fragment_type = FragmentType::MutationSource;
        }

        Ok(())
    }
}
