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

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfo;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_exception::ErrorCode;
use common_exception::Result;
use common_settings::ReplaceIntoShuffleStrategy;
use common_sql::executor::CopyIntoTable;
use common_sql::executor::CopyIntoTableSource;
use common_sql::executor::Deduplicate;
use common_sql::executor::DeletePartial;
use common_sql::executor::QuerySource;
use common_sql::executor::ReplaceInto;
use common_storages_fuse::operations::Mutation;
use common_storages_fuse::TableContext;
use storages_common_table_meta::meta::BlockSlotDescription;
use storages_common_table_meta::meta::Location;

use crate::api::DataExchange;
use crate::schedulers::Fragmenter;
use crate::schedulers::QueryFragmentAction;
use crate::schedulers::QueryFragmentActions;
use crate::schedulers::QueryFragmentsActions;
use crate::sessions::QueryContext;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PhysicalPlanReplacer;
use crate::sql::executor::TableScan;

/// Type of plan fragment
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FragmentType {
    /// Root fragment of a query plan
    Root,

    /// Intermediate fragment of a query plan,
    /// doesn't contain any `TableScan` operator.
    Intermediate,

    /// Leaf fragment of a query plan, which contains
    /// a `TableScan` operator.
    Source,
    /// Leaf fragment of a delete plan, which contains
    /// a `DeletePartial` operator.
    DeleteLeaf,
    /// Intermediate fragment of a replace into plan, which contains a `ReplaceInto` operator.
    ReplaceInto,
}

#[derive(Clone)]
pub struct PlanFragment {
    pub plan: PhysicalPlan,
    pub fragment_type: FragmentType,
    pub fragment_id: usize,
    pub exchange: Option<DataExchange>,
    pub query_id: String,

    // The fragments to ask data from.
    pub source_fragments: Vec<PlanFragment>,
}

impl PlanFragment {
    pub fn get_actions(
        &self,
        ctx: Arc<QueryContext>,
        actions: &mut QueryFragmentsActions,
    ) -> Result<()> {
        for input in self.source_fragments.iter() {
            input.get_actions(ctx.clone(), actions)?;
        }

        let mut fragment_actions = QueryFragmentActions::create(self.fragment_id);

        match &self.fragment_type {
            FragmentType::Root => {
                let action = QueryFragmentAction::create(
                    Fragmenter::get_local_executor(ctx),
                    self.plan.clone(),
                );
                fragment_actions.add_action(action);
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
            FragmentType::Intermediate => {
                if self
                    .source_fragments
                    .iter()
                    .any(|fragment| matches!(&fragment.exchange, Some(DataExchange::Merge(_))))
                {
                    // If this is a intermediate fragment with merge input,
                    // we will only send it to coordinator node.
                    let action = QueryFragmentAction::create(
                        Fragmenter::get_local_executor(ctx),
                        self.plan.clone(),
                    );
                    fragment_actions.add_action(action);
                } else {
                    // Otherwise distribute the fragment to all the executors.
                    for executor in Fragmenter::get_executors(ctx) {
                        let action = QueryFragmentAction::create(executor, self.plan.clone());
                        fragment_actions.add_action(action);
                    }
                }
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
            FragmentType::Source => {
                // Redistribute partitions
                let mut fragment_actions = self.redistribute_source_fragment(ctx)?;
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
            FragmentType::DeleteLeaf => {
                let mut fragment_actions = self.redistribute_delete_leaf(ctx)?;
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
            FragmentType::ReplaceInto => {
                // Redistribute partitions
                let mut fragment_actions = self.redistribute_replace_into(ctx)?;
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
        }

        Ok(())
    }

    /// Redistribute partitions of current source fragment to executors.
    fn redistribute_source_fragment(&self, ctx: Arc<QueryContext>) -> Result<QueryFragmentActions> {
        if self.fragment_type != FragmentType::Source {
            return Err(ErrorCode::Internal(
                "Cannot redistribute a non-source fragment".to_string(),
            ));
        }

        let read_source = self.get_read_source()?;

        let executors = Fragmenter::get_executors(ctx);
        // Redistribute partitions of ReadDataSourcePlan.
        let mut fragment_actions = QueryFragmentActions::create(self.fragment_id);

        let partitions = &read_source.parts;
        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.iter() {
            let mut new_read_source = read_source.clone();
            new_read_source.parts = parts.clone();
            let mut plan = self.plan.clone();

            // Replace `ReadDataSourcePlan` with rewritten one and generate new fragment for it.
            let mut replace_read_source = ReplaceReadSource {
                source: new_read_source,
            };
            plan = replace_read_source.replace(&plan)?;

            fragment_actions
                .add_action(QueryFragmentAction::create(executor.clone(), plan.clone()));
        }

        Ok(fragment_actions)
    }

    fn redistribute_delete_leaf(&self, ctx: Arc<QueryContext>) -> Result<QueryFragmentActions> {
        let plan = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let plan = match plan.input.as_ref() {
            PhysicalPlan::DeletePartial(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let partitions: &Partitions = &plan.parts;
        let executors = Fragmenter::get_executors(ctx.clone());
        let mut fragment_actions = QueryFragmentActions::create(self.fragment_id);

        // collect partitions by segment index
        // finally, these code will be removed, after the pruning is refactored
        let mut deleted_segments: Vec<Arc<Box<dyn PartInfo>>> = vec![];
        let mut blocks_group_by_segment_index: HashMap<usize, Vec<Arc<Box<dyn PartInfo>>>> =
            HashMap::new();
        for part in &partitions.partitions {
            // safe to unwrap because we know the type of plan is DeletePartial
            let part_downcast: &Mutation = part.as_ref().as_ref().as_any().downcast_ref().unwrap();
            match part_downcast {
                Mutation::MutationDeletedSegment(_) => {
                    deleted_segments.push(part.clone());
                }
                Mutation::MutationPartInfo(part_info) => {
                    let segment_index: usize = part_info.index.segment_idx;
                    let blocks = blocks_group_by_segment_index
                        .entry(segment_index)
                        .or_insert_with(Vec::new);
                    blocks.push(part.clone());
                }
            }
        }
        let segments_reshuffle = Self::reshuffle(
            executors,
            blocks_group_by_segment_index
                .into_iter()
                .map(|x| x.1)
                .collect(),
        )?;
        let mut partition_reshuffle = segments_reshuffle
            .into_iter()
            .map(|(k, v)| {
                let mut parts = vec![];
                for mut segment_parts in v {
                    parts.append(&mut segment_parts);
                }
                (k, parts)
            })
            .collect::<HashMap<_, _>>();
        partition_reshuffle
            .entry(Fragmenter::get_local_executor(ctx))
            .or_insert_with(Vec::new)
            .append(&mut deleted_segments);
        for (executor, parts) in partition_reshuffle.into_iter() {
            let mut plan = self.plan.clone();

            let mut replace_delete_partial = ReplaceDeletePartial {
                partitions: Partitions {
                    kind: PartitionsShuffleKind::Mod,
                    partitions: parts,
                    is_lazy: false,
                },
            };
            plan = replace_delete_partial.replace(&plan)?;

            fragment_actions
                .add_action(QueryFragmentAction::create(executor.clone(), plan.clone()));
        }

        Ok(fragment_actions)
    }

    fn redistribute_replace_into(&self, ctx: Arc<QueryContext>) -> Result<QueryFragmentActions> {
        let plan = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let plan = match plan.input.as_ref() {
            PhysicalPlan::ReplaceInto(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let partitions = &plan.segments;
        let executors = Fragmenter::get_executors(ctx.clone());
        let mut fragment_actions = QueryFragmentActions::create(self.fragment_id);
        let local_id = &ctx.get_cluster().local_id;
        match ctx.get_settings().get_replace_into_shuffle_strategy()? {
            ReplaceIntoShuffleStrategy::SegmentLevelShuffling => {
                let partition_reshuffle = Self::reshuffle(executors, partitions.clone())?;
                for (executor, parts) in partition_reshuffle.iter() {
                    let mut plan = self.plan.clone();
                    let need_insert = executor == local_id;

                    let mut replace_replace_into = ReplaceReplaceInto {
                        partitions: parts.clone(),
                        slot: None,
                        need_insert,
                    };
                    plan = replace_replace_into.replace(&plan)?;

                    fragment_actions
                        .add_action(QueryFragmentAction::create(executor.clone(), plan.clone()));
                }
            }
            ReplaceIntoShuffleStrategy::BlockLevelShuffling => {
                let num_slots = executors.len();
                // assign all the segment locations to each one of the executors,
                // but for each segment, one executor only need to take part of the blocks
                for (executor_idx, executor) in executors.iter().enumerate() {
                    let mut plan = self.plan.clone();
                    let need_insert = executor == local_id;
                    let mut replace_replace_into = ReplaceReplaceInto {
                        partitions: partitions.clone(),
                        slot: Some(BlockSlotDescription {
                            num_slots,
                            slot: executor_idx as u32,
                        }),
                        need_insert,
                    };
                    plan = replace_replace_into.replace(&plan)?;

                    fragment_actions
                        .add_action(QueryFragmentAction::create(executor.clone(), plan.clone()));
                }
            }
        }
        Ok(fragment_actions)
    }

    fn reshuffle<T: Clone>(
        executors: Vec<String>,
        partitions: Vec<T>,
    ) -> Result<HashMap<String, Vec<T>>> {
        let num_parts = partitions.len();
        let num_executors = executors.len();
        let mut executors_sorted = executors;
        executors_sorted.sort();

        let mut parts = partitions
            .into_iter()
            .enumerate()
            .map(|(idx, p)| (idx % num_executors, p))
            .collect::<Vec<_>>();
        parts.sort_by(|a, b| a.0.cmp(&b.0));
        let partitions: Vec<_> = parts.into_iter().map(|x| x.1).collect();

        // parts_per_executor = num_parts / num_executors
        // remain = num_parts % num_executors
        // part distribution:
        //   executor number      | Part number of each executor
        // ------------------------------------------------------
        // num_executors - remain |   parts_per_executor
        //     remain             |   parts_per_executor + 1
        let mut executor_part = HashMap::default();
        for (idx, executor) in executors_sorted.into_iter().enumerate() {
            let begin = num_parts * idx / num_executors;
            let end = num_parts * (idx + 1) / num_executors;
            let parts = if begin == end {
                // reach here only when num_executors > num_parts
                vec![]
            } else {
                partitions[begin..end].to_vec()
            };
            executor_part.insert(executor, parts);
        }

        Ok(executor_part)
    }

    fn get_read_source(&self) -> Result<DataSourcePlan> {
        if self.fragment_type != FragmentType::Source {
            return Err(ErrorCode::Internal(
                "Cannot get read source from a non-source fragment".to_string(),
            ));
        }

        let mut source = vec![];

        let mut collect_read_source = |plan: &PhysicalPlan| match plan {
            PhysicalPlan::TableScan(scan) => source.push(*scan.source.clone()),
            PhysicalPlan::CopyIntoTable(copy) => {
                if let Some(stage) = copy.source.as_stage().cloned() {
                    source.push(*stage);
                }
            }
            _ => {}
        };

        PhysicalPlan::traverse(
            &self.plan,
            &mut |_| true,
            &mut collect_read_source,
            &mut |_| {},
        );

        if source.len() != 1 {
            Err(ErrorCode::Internal(
                "Invalid source fragment with multiple table scan".to_string(),
            ))
        } else {
            Ok(source.remove(0))
        }
    }
}

pub struct ReplaceReadSource {
    pub source: DataSourcePlan,
}

impl PhysicalPlanReplacer for ReplaceReadSource {
    fn replace_table_scan(&mut self, plan: &TableScan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::TableScan(TableScan {
            plan_id: plan.plan_id,
            source: Box::new(self.source.clone()),
            name_mapping: plan.name_mapping.clone(),
            table_index: plan.table_index,
            stat_info: plan.stat_info.clone(),
            internal_column: plan.internal_column.clone(),
        }))
    }

    fn replace_copy_into_table(&mut self, plan: &CopyIntoTable) -> Result<PhysicalPlan> {
        match &plan.source {
            CopyIntoTableSource::Query(query_ctx) => {
                let input = self.replace(&query_ctx.plan)?;
                Ok(PhysicalPlan::CopyIntoTable(Box::new(CopyIntoTable {
                    source: CopyIntoTableSource::Query(Box::new(QuerySource {
                        plan: input,
                        ..*query_ctx.clone()
                    })),
                    ..plan.clone()
                })))
            }
            CopyIntoTableSource::Stage(_) => {
                Ok(PhysicalPlan::CopyIntoTable(Box::new(CopyIntoTable {
                    source: CopyIntoTableSource::Stage(Box::new(self.source.clone())),
                    ..plan.clone()
                })))
            }
        }
    }
}

struct ReplaceDeletePartial {
    pub partitions: Partitions,
}

impl PhysicalPlanReplacer for ReplaceDeletePartial {
    fn replace_delete_partial(&mut self, plan: &DeletePartial) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::DeletePartial(Box::new(DeletePartial {
            parts: self.partitions.clone(),
            ..plan.clone()
        })))
    }
}

struct ReplaceReplaceInto {
    pub partitions: Vec<(usize, Location)>,
    // for standalone mode, slot is None
    pub slot: Option<BlockSlotDescription>,
    pub need_insert: bool,
}

impl PhysicalPlanReplacer for ReplaceReplaceInto {
    fn replace_replace_into(&mut self, plan: &ReplaceInto) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ReplaceInto(ReplaceInto {
            input: Box::new(input),
            need_insert: self.need_insert,
            segments: self.partitions.clone(),
            block_slots: self.slot.clone(),
            ..plan.clone()
        }))
    }

    fn replace_deduplicate(&mut self, plan: &Deduplicate) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::Deduplicate(Deduplicate {
            input: Box::new(input),
            need_insert: self.need_insert,
            table_is_empty: self.partitions.is_empty(),
            ..plan.clone()
        }))
    }
}
