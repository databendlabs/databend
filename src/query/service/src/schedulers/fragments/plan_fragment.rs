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
use databend_common_catalog::plan::ReclusterTask;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_settings::ReplaceIntoShuffleStrategy;
use databend_common_sql::executor::physical_plans::CompactSource;
use databend_common_sql::executor::physical_plans::ConstantTableScan;
use databend_common_sql::executor::physical_plans::CopyIntoTable;
use databend_common_sql::executor::physical_plans::CopyIntoTableSource;
use databend_common_sql::executor::physical_plans::MutationSource;
use databend_common_sql::executor::physical_plans::Recluster;
use databend_common_sql::executor::physical_plans::ReplaceDeduplicate;
use databend_common_sql::executor::physical_plans::ReplaceInto;
use databend_common_sql::executor::physical_plans::TableScan;
use databend_common_sql::executor::IPhysicalPlan;
use databend_common_sql::executor::PhysicalPlanDeriveHandle;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_table_meta::meta::BlockSlotDescription;
use databend_storages_common_table_meta::meta::Location;

use crate::schedulers::Fragmenter;
use crate::schedulers::QueryFragmentAction;
use crate::schedulers::QueryFragmentActions;
use crate::schedulers::QueryFragmentsActions;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::sessions::QueryContext;
use crate::sql::executor::PhysicalPlan;

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
    /// Intermediate fragment of a replace into plan, which contains a `ReplaceInto` operator.
    ReplaceInto,
    Compact,
    Recluster,
    MutationSource,
}

#[derive(Clone)]
pub struct PlanFragment {
    pub plan: Box<dyn IPhysicalPlan>,
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
            FragmentType::MutationSource => {
                self.redistribute_mutation_source(ctx, &mut fragment_actions)?;
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

        let executors = Fragmenter::get_executors_nodes(ctx);

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
                        .map(|col| col.clone().into())
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
                            .map(BlockEntry::to_column)
                            .collect::<Vec<Column>>();
                        let source = DataSource::ConstTable(ConstTableColumn {
                            columns,
                            num_rows: block.num_rows(),
                        });
                        executor_partitions
                            .entry(executor.id.clone())
                            .or_default()
                            .insert(*plan_id, source);
                    }
                }
            }
        }

        for (executor, sources) in executor_partitions {
            // Replace `ReadDataSourcePlan` with rewritten one and generate new fragment for it.
            let mut handle = ReadSourceDeriveHandle::new(sources);
            let plan = self.plan.derive_with(&mut handle);

            fragment_actions.add_action(QueryFragmentAction::create(executor.clone(), plan));
        }

        Ok(())
    }

    fn redistribute_mutation_source(
        &self,
        ctx: Arc<QueryContext>,
        fragment_actions: &mut QueryFragmentActions,
    ) -> Result<()> {
        let plan = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };

        let plan = PhysicalPlan::ExchangeSink(plan.clone());
        let mutation_source = plan.try_find_mutation_source().unwrap();

        let partitions: &Partitions = &mutation_source.partitions;
        let executors = Fragmenter::get_executors_nodes(ctx);

        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.into_iter() {
            let mut handle = MutationSourceDeriveHandle::new(parts);
            let plan = self.plan.derive_with(&mut handle);
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

                    let mut handle = ReplaceDeriveHandle::new(parts, None, need_insert);
                    plan = plan.derive_with(&mut handle);
                    fragment_actions.add_action(QueryFragmentAction::create(executor, plan));
                }
            }
            ReplaceIntoShuffleStrategy::BlockLevelShuffling => {
                let num_slots = executors.len();
                // assign all the segment locations to each one of the executors,
                // but for each segment, one executor only need to take part of the blocks
                for (executor_idx, executor) in executors.into_iter().enumerate() {
                    let need_insert = executor == local_id;
                    let mut handle = ReplaceDeriveHandle::new(
                        partitions.clone(),
                        Some(BlockSlotDescription {
                            num_slots,
                            slot: executor_idx as u32,
                        }),
                        need_insert,
                    );

                    let plan = self.plan.derive_with(&mut handle);

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
        let executors = Fragmenter::get_executors_nodes(ctx);

        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.into_iter() {
            let mut handle = CompactSourceDeriveHandle::new(parts);
            let plan = self.plan.derive_with(&mut handle);
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
            PhysicalPlan::Recluster(plan) => plan,
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
            let mut handle = ReclusterDeriveHandle::new(tasks);
            let plan = self.plan.derive_with(&mut handle);
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

struct ReadSourceDeriveHandle {
    sources: HashMap<u32, DataSource>,
}

impl ReadSourceDeriveHandle {
    pub fn new(sources: HashMap<u32, DataSource>) -> Box<dyn PhysicalPlanDeriveHandle> {
        Box::new(ReadSourceDeriveHandle { sources })
    }
}

impl PhysicalPlanDeriveHandle for ReadSourceDeriveHandle {
    fn derive(
        &mut self,
        v: &Box<dyn IPhysicalPlan>,
        children: Vec<Box<dyn IPhysicalPlan>>,
    ) -> std::result::Result<Box<dyn IPhysicalPlan>, Vec<Box<dyn IPhysicalPlan>>> {
        if let Some(table_scan) = v.down_cast::<TableScan>() {
            let source = self.sources.remove(&table_scan.get_id()).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Cannot find data source for table scan plan {}",
                    table_scan.get_id()
                ))
            })?;

            return Ok(Box::new(TableScan {
                source: Box::new(DataSourcePlan::try_from(source)?),
                ..table_scan.clone()
            }));
        } else if let Some(table_scan) = v.down_cast::<ConstantTableScan>() {
            let source = self.sources.remove(&table_scan.get_id()).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Cannot find data source for constant table scan plan {}",
                    table_scan.get_id()
                ))
            })?;

            let const_table_columns = ConstTableColumn::try_from(source)?;

            return Ok(Box::new(ConstantTableScan {
                values: const_table_columns.columns,
                num_rows: const_table_columns.num_rows,
                ..table_scan.clone()
            }));
        }

        Err(children)
    }
}

struct ReclusterDeriveHandle {
    tasks: Vec<ReclusterTask>,
}

impl ReclusterDeriveHandle {
    pub fn new(tasks: Vec<ReclusterTask>) -> Box<dyn PhysicalPlanDeriveHandle> {
        Box::new(ReclusterDeriveHandle { tasks })
    }
}

impl PhysicalPlanDeriveHandle for ReclusterDeriveHandle {
    fn derive(
        &mut self,
        v: &Box<dyn IPhysicalPlan>,
        children: Vec<Box<dyn IPhysicalPlan>>,
    ) -> std::result::Result<Box<dyn IPhysicalPlan>, Vec<Box<dyn IPhysicalPlan>>> {
        let Some(recluster) = v.down_cast::<Recluster>() else {
            return Err(children);
        };

        Ok(Box::new(Recluster {
            tasks: self.tasks.clone(),
            ..recluster.clone()
        }))
    }
}

struct MutationSourceDeriveHandle {
    partitions: Partitions,
}

impl MutationSourceDeriveHandle {
    pub fn new(partitions: Partitions) -> Box<dyn PhysicalPlanDeriveHandle> {
        Box::new(MutationSourceDeriveHandle { partitions })
    }
}

impl PhysicalPlanDeriveHandle for MutationSourceDeriveHandle {
    fn derive(
        &mut self,
        v: &Box<dyn IPhysicalPlan>,
        children: Vec<Box<dyn IPhysicalPlan>>,
    ) -> std::result::Result<Box<dyn IPhysicalPlan>, Vec<Box<dyn IPhysicalPlan>>> {
        let Some(mutation_source) = v.down_cast::<MutationSource>() else {
            return Err(children);
        };

        Ok(Box::new(MutationSource {
            partitions: self.partitions.clone(),
            ..mutation_source.clone()
        }))
    }
}

struct CompactSourceDeriveHandle {
    pub partitions: Partitions,
}

impl CompactSourceDeriveHandle {
    pub fn new(partitions: Partitions) -> Box<dyn PhysicalPlanDeriveHandle> {
        Box::new(CompactSourceDeriveHandle { partitions })
    }
}

impl PhysicalPlanDeriveHandle for CompactSourceDeriveHandle {
    fn derive(
        &mut self,
        v: &Box<dyn IPhysicalPlan>,
        children: Vec<Box<dyn IPhysicalPlan>>,
    ) -> std::result::Result<Box<dyn IPhysicalPlan>, Vec<Box<dyn IPhysicalPlan>>> {
        let Some(compact_source) = v.down_cast::<CompactSource>() else {
            return Err(children);
        };

        Ok(Box::new(CompactSource {
            parts: self.partitions.clone(),
            ..compact_source.clone()
        }))
    }
}

struct ReplaceDeriveHandle {
    pub partitions: Vec<(usize, Location)>,
    // for standalone mode, slot is None
    pub slot: Option<BlockSlotDescription>,
    pub need_insert: bool,
}

impl ReplaceDeriveHandle {
    pub fn new(
        partitions: Vec<(usize, Location)>,
        slot: Option<BlockSlotDescription>,
        need_insert: bool,
    ) -> Box<dyn PhysicalPlanDeriveHandle> {
        Box::new(ReplaceDeriveHandle {
            partitions,
            slot,
            need_insert,
        })
    }
}

impl PhysicalPlanDeriveHandle for ReplaceDeriveHandle {
    fn derive(
        &mut self,
        v: &Box<dyn IPhysicalPlan>,
        children: Vec<Box<dyn IPhysicalPlan>>,
    ) -> std::result::Result<Box<dyn IPhysicalPlan>, Vec<Box<dyn IPhysicalPlan>>> {
        if let Some(replace_into) = v.down_cast::<ReplaceInto>() {
            assert_eq!(children.len(), 1);
            return Ok(Box::new(ReplaceInto {
                input: children[0],
                need_insert: self.need_insert,
                segments: self.partitions.clone(),
                block_slots: self.slot.clone(),
                ..replace_into.clone()
            }));
        } else if let Some(replace_deduplicate) = v.down_cast::<ReplaceDeduplicate>() {
            assert_eq!(children.len(), 1);
            return Ok(Box::new(ReplaceDeduplicate {
                input: children[0],
                need_insert: self.need_insert,
                table_is_empty: self.partitions.is_empty(),
                ..replace_deduplicate.clone()
            }));
        }

        None
    }
}
