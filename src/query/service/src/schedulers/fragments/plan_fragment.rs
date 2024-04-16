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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Partitions;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;
use databend_common_settings::ReplaceIntoShuffleStrategy;
use databend_common_sql::executor::physical_plans::CompactSource;
use databend_common_sql::executor::physical_plans::ConstantTableScan;
use databend_common_sql::executor::physical_plans::CopyIntoTable;
use databend_common_sql::executor::physical_plans::CopyIntoTableSource;
use databend_common_sql::executor::physical_plans::DeleteSource;
use databend_common_sql::executor::physical_plans::ReclusterSource;
use databend_common_sql::executor::physical_plans::ReclusterTask;
use databend_common_sql::executor::physical_plans::ReplaceDeduplicate;
use databend_common_sql::executor::physical_plans::ReplaceInto;
use databend_common_sql::executor::physical_plans::TableScan;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_table_meta::meta::BlockSlotDescription;
use databend_storages_common_table_meta::meta::Location;

use crate::schedulers::Fragmenter;
use crate::schedulers::QueryFragmentAction;
use crate::schedulers::QueryFragmentActions;
use crate::schedulers::QueryFragmentsActions;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::sessions::QueryContext;
use crate::sql::executor::physical_plans::UpdateSource;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PhysicalPlanReplacer;

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
    /// a `DeleteSource` operator.
    DeleteLeaf,
    /// Intermediate fragment of a replace into plan, which contains a `ReplaceInto` operator.
    ReplaceInto,
    Compact,
    Recluster,
    Update,
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
            }
            FragmentType::Source => {
                // Redistribute partitions
                self.redistribute_source_fragment(ctx, &mut fragment_actions)?;
            }
            FragmentType::DeleteLeaf => {
                self.redistribute_delete_leaf(ctx, &mut fragment_actions)?;
            }
            FragmentType::ReplaceInto => {
                // Redistribute partitions
                self.redistribute_replace_into(ctx, &mut fragment_actions)?;
            }
            FragmentType::Compact => {
                self.redistribute_compact(ctx, &mut fragment_actions)?;
            }
            FragmentType::Recluster => {
                self.redistribute_recluster(ctx, &mut fragment_actions)?;
            }
            FragmentType::Update => {
                self.redistribute_update(ctx, &mut fragment_actions)?;
            }
        }

        if let Some(ref exchange) = self.exchange {
            fragment_actions.set_exchange(exchange.clone());
        }
        actions.add_fragment_actions(fragment_actions)
    }

    /// Redistribute partitions of current source fragment to executors.
    fn redistribute_source_fragment(
        &self,
        ctx: Arc<QueryContext>,
        fragment_actions: &mut QueryFragmentActions,
    ) -> Result<()> {
        if self.fragment_type != FragmentType::Source {
            return Err(ErrorCode::Internal(
                "Cannot redistribute a non-source fragment".to_string(),
            ));
        }

        let data_sources = self.collect_data_sources()?;

        let executors = Fragmenter::get_executors(ctx);

        let mut executor_partitions: HashMap<String, HashMap<u32, DataSource>> = HashMap::new();

        for (plan_id, data_source) in data_sources.iter() {
            match data_source {
                DataSource::Table(data_source_plan) => {
                    // Redistribute partitions of ReadDataSourcePlan.
                    let partitions = &data_source_plan.parts;
                    let partition_reshuffle = partitions.reshuffle(executors.clone())?;
                    for (executor, parts) in partition_reshuffle {
                        let mut source = data_source_plan.clone();
                        source.parts = parts;
                        executor_partitions
                            .entry(executor)
                            .or_default()
                            .insert(*plan_id, DataSource::Table(source));
                    }
                }
                DataSource::ConstTable(values) => {
                    let num_executors = executors.len();
                    let entries = values
                        .columns
                        .iter()
                        .map(|col| BlockEntry::new(col.data_type(), Value::Column(col.clone())))
                        .collect::<Vec<BlockEntry>>();
                    let block = DataBlock::new(entries, values.num_rows);
                    // Scatter the block
                    let mut indices = Vec::with_capacity(values.num_rows);
                    for i in 0..values.num_rows {
                        indices.push((i % num_executors) as u32);
                    }
                    let blocks = block.scatter(&indices, num_executors)?;
                    for (executor, block) in executors.iter().zip(blocks) {
                        let columns = block
                            .columns()
                            .iter()
                            .map(|entry| {
                                entry
                                    .value
                                    .convert_to_full_column(&entry.data_type, block.num_rows())
                            })
                            .collect::<Vec<Column>>();
                        let source = DataSource::ConstTable(ConstTableColumn {
                            columns,
                            num_rows: block.num_rows(),
                        });
                        executor_partitions
                            .entry(executor.clone())
                            .or_default()
                            .insert(*plan_id, source);
                    }
                }
            }
        }

        for (executor, sources) in executor_partitions {
            let mut plan = self.plan.clone();
            // Replace `ReadDataSourcePlan` with rewritten one and generate new fragment for it.
            let mut replace_read_source = ReplaceReadSource { sources };
            plan = replace_read_source.replace(&plan)?;

            fragment_actions
                .add_action(QueryFragmentAction::create(executor.clone(), plan.clone()));
        }

        Ok(())
    }

    fn redistribute_delete_leaf(
        &self,
        ctx: Arc<QueryContext>,
        fragment_actions: &mut QueryFragmentActions,
    ) -> Result<()> {
        let plan = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let plan = match plan.input.as_ref() {
            PhysicalPlan::DeleteSource(plan) => plan,
            _ => unreachable!("logic error"),
        };

        let partitions: &Partitions = &plan.parts;
        let executors = Fragmenter::get_executors(ctx);

        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.into_iter() {
            let mut plan = self.plan.clone();

            let mut replace_delete_source = ReplaceDeleteSource { partitions: parts };
            plan = replace_delete_source.replace(&plan)?;

            fragment_actions.add_action(QueryFragmentAction::create(executor, plan));
        }

        Ok(())
    }

    fn redistribute_update(
        &self,
        ctx: Arc<QueryContext>,
        fragment_actions: &mut QueryFragmentActions,
    ) -> Result<()> {
        let plan = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let plan = match plan.input.as_ref() {
            PhysicalPlan::UpdateSource(plan) => plan,
            _ => unreachable!("logic error"),
        };

        let partitions: &Partitions = &plan.parts;
        let executors = Fragmenter::get_executors(ctx);

        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.into_iter() {
            let mut plan = self.plan.clone();

            let mut replace_update = ReplaceUpdate { partitions: parts };
            plan = replace_update.replace(&plan)?;

            fragment_actions.add_action(QueryFragmentAction::create(executor, plan));
        }

        Ok(())
    }

    fn redistribute_replace_into(
        &self,
        ctx: Arc<QueryContext>,
        fragment_actions: &mut QueryFragmentActions,
    ) -> Result<()> {
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
        let local_id = ctx.get_cluster().local_id.clone();
        match ctx.get_settings().get_replace_into_shuffle_strategy()? {
            ReplaceIntoShuffleStrategy::SegmentLevelShuffling => {
                let partition_reshuffle = Self::reshuffle(executors, partitions.clone())?;
                for (executor, parts) in partition_reshuffle.into_iter() {
                    let mut plan = self.plan.clone();
                    let need_insert = executor == local_id;

                    let mut replace_replace_into = ReplaceReplaceInto {
                        partitions: parts,
                        slot: None,
                        need_insert,
                    };
                    plan = replace_replace_into.replace(&plan)?;

                    fragment_actions.add_action(QueryFragmentAction::create(executor, plan));
                }
            }
            ReplaceIntoShuffleStrategy::BlockLevelShuffling => {
                let num_slots = executors.len();
                // assign all the segment locations to each one of the executors,
                // but for each segment, one executor only need to take part of the blocks
                for (executor_idx, executor) in executors.into_iter().enumerate() {
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

                    fragment_actions.add_action(QueryFragmentAction::create(executor, plan));
                }
            }
        }
        Ok(())
    }

    fn redistribute_compact(
        &self,
        ctx: Arc<QueryContext>,
        fragment_actions: &mut QueryFragmentActions,
    ) -> Result<()> {
        let exchange_sink = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let compact_block = match exchange_sink.input.as_ref() {
            PhysicalPlan::CompactSource(plan) => plan,
            _ => unreachable!("logic error"),
        };

        let partitions: &Partitions = &compact_block.parts;
        let executors = Fragmenter::get_executors(ctx);

        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.into_iter() {
            let mut plan = self.plan.clone();

            let mut replace_compact_source = ReplaceCompactBlock { partitions: parts };
            plan = replace_compact_source.replace(&plan)?;

            fragment_actions.add_action(QueryFragmentAction::create(executor, plan));
        }

        Ok(())
    }

    fn redistribute_recluster(
        &self,
        ctx: Arc<QueryContext>,
        fragment_actions: &mut QueryFragmentActions,
    ) -> Result<()> {
        let exchange_sink = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let recluster = match exchange_sink.input.as_ref() {
            PhysicalPlan::ReclusterSource(plan) => plan,
            _ => unreachable!("logic error"),
        };

        let tasks = recluster.tasks.clone();
        let executors = Fragmenter::get_executors(ctx);
        if tasks.len() > executors.len() {
            return Err(ErrorCode::Internal(format!(
                "Cannot recluster {} tasks to {} executors",
                tasks.len(),
                executors.len()
            )));
        }

        let task_reshuffle = Self::reshuffle(executors, tasks)?;
        for (executor, tasks) in task_reshuffle.into_iter() {
            let mut plan = self.plan.clone();
            let mut replace_recluster = ReplaceReclusterSource { tasks };
            plan = replace_recluster.replace(&plan)?;
            fragment_actions.add_action(QueryFragmentAction::create(executor, plan));
        }

        Ok(())
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

    fn collect_data_sources(&self) -> Result<HashMap<u32, DataSource>> {
        if self.fragment_type != FragmentType::Source {
            return Err(ErrorCode::Internal(
                "Cannot get read source from a non-source fragment".to_string(),
            ));
        }

        let mut data_sources = HashMap::new();

        let mut collect_data_source = |plan: &PhysicalPlan| {
            if let PhysicalPlan::TableScan(scan) = plan {
                data_sources.insert(scan.plan_id, DataSource::Table(*scan.source.clone()));
            } else if let PhysicalPlan::ConstantTableScan(scan) = plan {
                data_sources.insert(
                    scan.plan_id,
                    DataSource::ConstTable(ConstTableColumn {
                        columns: scan.values.clone(),
                        num_rows: scan.num_rows,
                    }),
                );
            }
        };

        PhysicalPlan::traverse(
            &self.plan,
            &mut |_| true,
            &mut collect_data_source,
            &mut |_| {},
        );

        Ok(data_sources)
    }
}

struct ConstTableColumn {
    columns: Vec<Column>,
    num_rows: usize,
}

enum DataSource {
    Table(DataSourcePlan),
    // It's possible there is zero column, so we also save row number.
    ConstTable(ConstTableColumn),
}

impl TryFrom<DataSource> for DataSourcePlan {
    type Error = ErrorCode;

    fn try_from(value: DataSource) -> Result<Self> {
        match value {
            DataSource::Table(plan) => Ok(plan),
            DataSource::ConstTable(_) => Err(ErrorCode::Internal(
                "Cannot convert ConstTable to DataSourcePlan".to_string(),
            )),
        }
    }
}

impl TryFrom<DataSource> for ConstTableColumn {
    type Error = ErrorCode;

    fn try_from(value: DataSource) -> Result<Self> {
        match value {
            DataSource::Table(_) => Err(ErrorCode::Internal(
                "Cannot convert Table to Vec<Column>".to_string(),
            )),
            DataSource::ConstTable(columns) => Ok(columns),
        }
    }
}

struct ReplaceReadSource {
    sources: HashMap<u32, DataSource>,
}

impl PhysicalPlanReplacer for ReplaceReadSource {
    fn replace_table_scan(&mut self, plan: &TableScan) -> Result<PhysicalPlan> {
        let source = self.sources.remove(&plan.plan_id).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Cannot find data source for table scan plan {}",
                plan.plan_id
            ))
        })?;

        let source = DataSourcePlan::try_from(source)?;

        Ok(PhysicalPlan::TableScan(TableScan {
            plan_id: plan.plan_id,
            source: Box::new(source),
            name_mapping: plan.name_mapping.clone(),
            table_index: plan.table_index,
            stat_info: plan.stat_info.clone(),
            internal_column: plan.internal_column.clone(),
        }))
    }

    fn replace_constant_table_scan(&mut self, plan: &ConstantTableScan) -> Result<PhysicalPlan> {
        let source = self.sources.remove(&plan.plan_id).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Cannot find data source for constant table scan plan {}",
                plan.plan_id
            ))
        })?;

        let const_table_columns = ConstTableColumn::try_from(source)?;

        Ok(PhysicalPlan::ConstantTableScan(ConstantTableScan {
            plan_id: plan.plan_id,
            values: const_table_columns.columns,
            num_rows: const_table_columns.num_rows,
            output_schema: plan.output_schema.clone(),
        }))
    }

    fn replace_copy_into_table(&mut self, plan: &CopyIntoTable) -> Result<PhysicalPlan> {
        match &plan.source {
            CopyIntoTableSource::Query(query_physical_plan) => {
                let input = self.replace(query_physical_plan)?;
                Ok(PhysicalPlan::CopyIntoTable(Box::new(CopyIntoTable {
                    source: CopyIntoTableSource::Query(Box::new(input)),
                    ..plan.clone()
                })))
            }
            CopyIntoTableSource::Stage(v) => {
                let input = self.replace(v)?;
                Ok(PhysicalPlan::CopyIntoTable(Box::new(CopyIntoTable {
                    source: CopyIntoTableSource::Stage(Box::new(input)),
                    ..plan.clone()
                })))
            }
        }
    }
}

struct ReplaceReclusterSource {
    pub tasks: Vec<ReclusterTask>,
}

impl PhysicalPlanReplacer for ReplaceReclusterSource {
    fn replace_recluster_source(&mut self, plan: &ReclusterSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::ReclusterSource(Box::new(ReclusterSource {
            tasks: self.tasks.clone(),
            ..plan.clone()
        })))
    }
}

struct ReplaceCompactBlock {
    pub partitions: Partitions,
}

impl PhysicalPlanReplacer for ReplaceCompactBlock {
    fn replace_compact_source(&mut self, plan: &CompactSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::CompactSource(Box::new(CompactSource {
            parts: self.partitions.clone(),
            ..plan.clone()
        })))
    }
}

struct ReplaceDeleteSource {
    pub partitions: Partitions,
}

impl PhysicalPlanReplacer for ReplaceDeleteSource {
    fn replace_delete_source(&mut self, plan: &DeleteSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::DeleteSource(Box::new(DeleteSource {
            parts: self.partitions.clone(),
            ..plan.clone()
        })))
    }
}

struct ReplaceUpdate {
    pub partitions: Partitions,
}

impl PhysicalPlanReplacer for ReplaceUpdate {
    fn replace_update_source(&mut self, plan: &UpdateSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::UpdateSource(Box::new(UpdateSource {
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
        Ok(PhysicalPlan::ReplaceInto(Box::new(ReplaceInto {
            input: Box::new(input),
            need_insert: self.need_insert,
            segments: self.partitions.clone(),
            block_slots: self.slot.clone(),
            ..plan.clone()
        })))
    }

    fn replace_deduplicate(&mut self, plan: &ReplaceDeduplicate) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ReplaceDeduplicate(Box::new(
            ReplaceDeduplicate {
                input: Box::new(input),
                need_insert: self.need_insert,
                table_is_empty: self.partitions.is_empty(),
                ..plan.clone()
            },
        )))
    }
}
