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
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use databend_base::uniq_id::GlobalUniq;
use databend_common_catalog::cluster_info::Cluster;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_meta_client::types::NodeInfo;

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
use crate::sessions::TableContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextSettings;

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

        let root_fragment_id = self.ctx.fragment_id().next_fragment_id();
        fragments.insert(root_fragment_id, PlanFragment {
            plan: root,
            fragment_type,
            fragment_id: root_fragment_id,
            exchange: None,
            query_id: self.query_id.clone(),
            source_fragments: self.fragments,
        });

        let edges = Self::collect_fragments_edge(fragments.values());
        let mut target_sources = BTreeMap::<usize, Vec<usize>>::new();

        for (source, target) in edges {
            let fragment = fragments.get_mut(&source).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Fragment edge references missing source fragment {}",
                    source
                ))
            })?;
            let exchange_sink = ExchangeSink::from_mut_physical_plan(&mut fragment.plan)
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "Source fragment {} does not contain an ExchangeSink",
                        source
                    ))
                })?;
            exchange_sink.destination_fragment_id = target;

            target_sources.entry(target).or_default().push(source);
        }

        // Clone dependencies only after their sink destinations have been
        // connected, so source_fragments contains a self-consistent topology.
        let source_ids = target_sources
            .values()
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut source_lookup = BTreeMap::new();
        for source_id in source_ids {
            let mut source_fragment = fragments.get(&source_id).cloned().ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Fragment dependency {} is missing from the fragment map",
                    source_id
                ))
            })?;
            source_fragment.source_fragments.clear();
            source_lookup.insert(source_id, source_fragment);
        }

        for (target, mut sources) in target_sources {
            let fragment = fragments.get_mut(&target).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Fragment edge references missing target fragment {}",
                    target
                ))
            })?;

            sources.sort_unstable();
            let source_fragments = sources
                .into_iter()
                .map(|source| {
                    source_lookup.remove(&source).ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "Fragment dependency {} is missing from the source lookup",
                            source
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            fragment.source_fragments = source_fragments;
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
        num_threads: usize,
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
                    id: GlobalUniq::unique(),
                    destination_ids,
                    destination_channels,
                    shuffle_keys: exchange_sink.keys.clone(),
                    allow_adjust_parallelism: exchange_sink.allow_adjust_parallelism,
                }))
            }
            FragmentKind::GlobalShuffle => {
                let destination_ids = get_executors(cluster.clone());

                let mut destination_channels = Vec::with_capacity(destination_ids.len());

                for destination in &destination_ids {
                    let channels = (0..num_threads).map(|_| GlobalUniq::unique()).collect();
                    destination_channels.push((destination.clone(), channels));
                }

                Some(DataExchange::GlobalShuffleExchange(NodeToNodeExchange {
                    id: GlobalUniq::unique(),
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
            FragmentKind::Expansive => Some(BroadcastExchange::create(
                get_executors(cluster),
                num_threads,
            )),
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
            let source_fragment_id = self.ctx.fragment_id().next_fragment_id();

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
            let max_threads = self.ctx.get_settings().get_max_threads().unwrap() as usize;
            let exchange = Self::get_exchange(cluster, &plan, max_threads).unwrap();

            let source_fragment = PlanFragment {
                plan,
                exchange: exchange.clone(),
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
                source_exchange: exchange,
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

            let fragment_id = self.ctx.fragment_id().next_fragment_id();
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

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;
    use databend_common_expression::DataSchemaRef;
    use databend_common_sql::executor::physical_plans::FragmentKind;

    use super::Fragmenter;
    use crate::physical_plans::ConstantTableScan;
    use crate::physical_plans::Exchange;
    use crate::physical_plans::ExchangeSink;
    use crate::physical_plans::ExchangeSource;
    use crate::physical_plans::PhysicalPlan;
    use crate::physical_plans::PhysicalPlanCast;
    use crate::physical_plans::PhysicalPlanMeta;
    use crate::schedulers::QueryFragmentsActions;
    use crate::servers::flight::v1::exchange::DataExchange;
    use crate::test_kits::ClusterDescriptor;
    use crate::test_kits::TestFixture;

    const MERGE_PLAN_ID: u32 = 11;
    const BROADCAST_PLAN_ID: u32 = 22;

    fn exchange(input: PhysicalPlan, kind: FragmentKind, plan_id: u32) -> PhysicalPlan {
        PhysicalPlan::new(Exchange {
            meta: PhysicalPlanMeta::with_plan_id("Exchange", plan_id),
            input,
            kind,
            keys: vec![],
            ignore_exchange: false,
            allow_adjust_parallelism: true,
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_remote_only_broadcast_fragment_topology() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture
            .new_query_ctx_with_cluster(
                ClusterDescriptor::new()
                    .with_node_info("node-a", "127.0.0.1:9090", "cluster", "warehouse")
                    .with_node_info("node-b", "127.0.0.1:9091", "cluster", "warehouse")
                    .with_node_info("node-c", "127.0.0.1:9092", "cluster", "warehouse")
                    .with_local_id("node-a"),
            )
            .await?;

        // Model the exchange boundary introduced by an IN-subquery global LIMIT:
        // distributed input -> Merge -> Broadcast -> distributed consumer.
        let input = PhysicalPlan::new(ConstantTableScan {
            meta: PhysicalPlanMeta::with_plan_id("ConstantTableScan", 33),
            values: vec![],
            num_rows: 1,
            output_schema: DataSchemaRef::default(),
        });
        let plan = exchange(
            exchange(input, FragmentKind::Merge, MERGE_PLAN_ID),
            FragmentKind::Expansive,
            BROADCAST_PLAN_ID,
        );

        let fragments = Fragmenter::try_create(ctx.clone())?.build_fragment(&plan)?;
        assert_eq!(fragments.len(), 3);
        let mut fragment_ids = fragments
            .iter()
            .map(|fragment| fragment.fragment_id)
            .collect::<Vec<_>>();
        fragment_ids.sort_unstable();
        assert!(fragment_ids.windows(2).all(|ids| ids[1] == ids[0] + 1));

        let merge_fragment = fragments
            .iter()
            .find(|fragment| matches!(fragment.exchange.as_ref(), Some(DataExchange::Merge(_))))
            .expect("expected a merge source fragment");
        let broadcast_fragment = fragments
            .iter()
            .find(|fragment| matches!(fragment.exchange.as_ref(), Some(DataExchange::Broadcast(_))))
            .expect("expected a broadcast intermediate fragment");
        let root_fragment = fragments
            .iter()
            .find(|fragment| fragment.exchange.is_none())
            .expect("expected a root fragment");

        // The fragment that directly consumes a Merge is coordinator-only. This
        // is what makes the global LIMIT run once before its rows are broadcast.
        assert_eq!(broadcast_fragment.source_fragments.len(), 1);
        assert_eq!(
            broadcast_fragment.source_fragments[0].fragment_id,
            merge_fragment.fragment_id
        );
        assert!(
            broadcast_fragment.source_fragments[0]
                .source_fragments
                .is_empty()
        );
        let dependency_sink =
            ExchangeSink::from_physical_plan(&broadcast_fragment.source_fragments[0].plan)
                .expect("dependency clone must retain its ExchangeSink");
        assert_eq!(
            dependency_sink.destination_fragment_id,
            broadcast_fragment.fragment_id
        );
        let mut intermediate_actions = QueryFragmentsActions::create(ctx.clone());
        broadcast_fragment.get_actions(ctx.clone(), &mut intermediate_actions)?;
        let scheduled = intermediate_actions
            .fragments_actions
            .iter()
            .find(|actions| actions.fragment_id == broadcast_fragment.fragment_id)
            .expect("expected actions for the broadcast fragment");
        assert_eq!(scheduled.fragment_actions.len(), 1);
        assert_eq!(scheduled.fragment_actions[0].executor, "node-a");

        let DataExchange::Broadcast(broadcast_exchange) = broadcast_fragment
            .exchange
            .as_ref()
            .expect("broadcast fragment must carry exchange metadata")
        else {
            unreachable!()
        };
        let mut destinations = broadcast_exchange.destination_ids.clone();
        destinations.sort();
        assert_eq!(destinations, vec![
            "node-a".to_string(),
            "node-b".to_string(),
            "node-c".to_string()
        ]);

        // Fragment edges, ExchangeSource metadata, and physical plan IDs must
        // describe the same topology on every executor.
        let merge_sink = ExchangeSink::from_physical_plan(&merge_fragment.plan)
            .expect("merge fragment must end in ExchangeSink");
        assert_eq!(
            merge_sink.destination_fragment_id,
            broadcast_fragment.fragment_id
        );
        assert_eq!(merge_sink.meta.plan_id, MERGE_PLAN_ID);

        let broadcast_sink = ExchangeSink::from_physical_plan(&broadcast_fragment.plan)
            .expect("broadcast fragment must end in ExchangeSink");
        assert_eq!(
            broadcast_sink.destination_fragment_id,
            root_fragment.fragment_id
        );
        assert_eq!(broadcast_sink.meta.plan_id, BROADCAST_PLAN_ID);
        let merge_source = ExchangeSource::from_physical_plan(&broadcast_sink.input)
            .expect("broadcast fragment must consume the merge source");
        assert_eq!(merge_source.source_fragment_id, merge_fragment.fragment_id);
        assert_eq!(merge_source.meta.plan_id, MERGE_PLAN_ID);
        assert_eq!(merge_source.query_id, merge_fragment.query_id);
        assert!(matches!(
            merge_source.source_exchange.as_ref(),
            Some(DataExchange::Merge(_))
        ));

        let broadcast_source = ExchangeSource::from_physical_plan(&root_fragment.plan)
            .expect("root fragment must consume the broadcast source");
        assert_eq!(
            broadcast_source.source_fragment_id,
            broadcast_fragment.fragment_id
        );
        assert_eq!(broadcast_source.meta.plan_id, BROADCAST_PLAN_ID);
        assert_eq!(broadcast_source.query_id, broadcast_fragment.query_id);
        assert!(matches!(
            broadcast_source.source_exchange.as_ref(),
            Some(DataExchange::Broadcast(_))
        ));

        let mut all_actions = QueryFragmentsActions::create(ctx.clone());
        for fragment in &fragments {
            fragment.get_actions(ctx.clone(), &mut all_actions)?;
        }
        assert_eq!(all_actions.get_root_fragment_ids()?, vec![
            root_fragment.fragment_id
        ]);

        Ok(())
    }
}
