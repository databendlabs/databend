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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::ReclusterTask;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_settings::ReplaceIntoShuffleStrategy;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_table_meta::meta::BlockSlotDescription;
use databend_storages_common_table_meta::meta::Location;

use crate::physical_plans::CompactSource;
use crate::physical_plans::ConstantTableScan;
use crate::physical_plans::DeriveHandle;
use crate::physical_plans::ExchangeSink;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::MutationSource;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanCast;
use crate::physical_plans::PhysicalPlanVisitor;
use crate::physical_plans::Recluster;
use crate::physical_plans::ReplaceDeduplicate;
use crate::physical_plans::ReplaceInto;
use crate::physical_plans::TableScan;
use crate::schedulers::Fragmenter;
use crate::schedulers::QueryFragmentAction;
use crate::schedulers::QueryFragmentActions;
use crate::schedulers::QueryFragmentsActions;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::sessions::QueryContext;

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
        // for input in self.source_fragments.iter() {
        //     input.get_actions(ctx.clone(), actions)?;
        // }

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

        // Sort executors by cache_id for deterministic ordering (same as reshuffle)
        let mut executors_sorted = executors.clone();
        executors_sorted.sort_by(|a, b| a.cache_id.cmp(&b.cache_id));

        let mut executor_partitions: HashMap<String, HashMap<u32, DataSource>> = HashMap::new();

        for (plan_id, data_source) in data_sources.iter() {
            match data_source {
                DataSource::Table(data_source_plan) => {
                    // Redistribute partitions of ReadDataSourcePlan.
                    let partitions = &data_source_plan.parts;
                    let use_block_mod = partitions.kind == PartitionsShuffleKind::BlockMod;
                    let partition_reshuffle = partitions.reshuffle(executors.clone())?;

                    for (executor_id, parts) in partition_reshuffle {
                        let mut source = data_source_plan.clone();
                        source.parts = parts;

                        // For BlockMod shuffle, compute block_slot based on executor position
                        if use_block_mod {
                            let num_slots = executors_sorted.len();
                            let slot = executors_sorted
                                .iter()
                                .position(|e| e.id == executor_id)
                                .unwrap_or(0) as u32;
                            source.block_slot = Some(BlockSlotDescription { num_slots, slot });
                        }

                        executor_partitions
                            .entry(executor_id)
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
            let mut handle = ReadSourceDeriveHandle::create(sources);
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
        let Some(plan) = ExchangeSink::from_physical_plan(&self.plan) else {
            unreachable!("logic error");
        };

        let plan: PhysicalPlan = PhysicalPlan::new(plan.clone());
        let mutation_source = plan
            .try_find_mutation_source()
            .ok_or_else(|| ErrorCode::Internal("No mutation source found in exchange sink plan"))?;

        let partitions: &Partitions = &mutation_source.partitions;
        let executors = Fragmenter::get_executors_nodes(ctx);

        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.into_iter() {
            let mut handle = MutationSourceDeriveHandle::create(parts);
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
        struct PartitionsCollector {
            partitions: Vec<(usize, Location)>,
        }

        impl PartitionsCollector {
            pub fn create() -> Box<dyn PhysicalPlanVisitor> {
                Box::new(PartitionsCollector { partitions: vec![] })
            }

            pub fn take(&mut self) -> Vec<(usize, Location)> {
                std::mem::take(&mut self.partitions)
            }
        }

        impl PhysicalPlanVisitor for PartitionsCollector {
            fn as_any(&mut self) -> &mut dyn Any {
                self
            }

            fn visit(&mut self, plan: &PhysicalPlan) -> Result<()> {
                if let Some(v) = ReplaceInto::from_physical_plan(plan) {
                    assert!(self.partitions.is_empty());
                    self.partitions = v.segments.clone();
                }

                Ok(())
            }
        }

        let mut visitor = PartitionsCollector::create();
        self.plan.visit(&mut visitor)?;

        let mut partitions = vec![];

        if let Some(v) = visitor.as_any().downcast_mut::<PartitionsCollector>() {
            partitions = v.take();
        }

        let executors = Fragmenter::get_executors(ctx.clone());
        let local_id = ctx.get_cluster().local_id.clone();
        match ctx.get_settings().get_replace_into_shuffle_strategy()? {
            ReplaceIntoShuffleStrategy::SegmentLevelShuffling => {
                let partition_reshuffle = Self::reshuffle(executors, partitions.clone())?;
                for (executor, parts) in partition_reshuffle.into_iter() {
                    let mut plan = self.plan.clone();
                    let need_insert = executor == local_id;

                    let mut handle = ReplaceDeriveHandle::create(parts, None, need_insert);
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
                    let mut handle = ReplaceDeriveHandle::create(
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
        struct SourceCollector {
            partitions: Option<Partitions>,
        }

        impl SourceCollector {
            pub fn create() -> Box<dyn PhysicalPlanVisitor> {
                Box::new(SourceCollector { partitions: None })
            }

            pub fn take(&mut self) -> Result<Partitions> {
                self.partitions.take().ok_or_else(|| {
                    ErrorCode::Internal("No partitions collected from plan fragment")
                })
            }
        }

        impl PhysicalPlanVisitor for SourceCollector {
            fn as_any(&mut self) -> &mut dyn Any {
                self
            }

            fn visit(&mut self, plan: &PhysicalPlan) -> Result<()> {
                if let Some(v) = CompactSource::from_physical_plan(plan) {
                    assert!(self.partitions.is_none());
                    self.partitions = Some(v.parts.clone());
                }

                Ok(())
            }
        }

        let mut visitor = SourceCollector::create();
        self.plan.visit(&mut visitor)?;

        let partitions = visitor
            .as_any()
            .downcast_mut::<SourceCollector>()
            .ok_or_else(|| ErrorCode::Internal("Failed to downcast visitor to SourceCollector"))?
            .take()?;
        let executors = Fragmenter::get_executors_nodes(ctx);

        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.into_iter() {
            let mut handle = CompactSourceDeriveHandle::create(parts);
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
        struct TasksCollector {
            tasks: Vec<ReclusterTask>,
        }

        impl TasksCollector {
            pub fn create() -> Box<dyn PhysicalPlanVisitor> {
                Box::new(TasksCollector { tasks: Vec::new() })
            }

            pub fn take(&mut self) -> Vec<ReclusterTask> {
                std::mem::take(&mut self.tasks)
            }
        }

        impl PhysicalPlanVisitor for TasksCollector {
            fn as_any(&mut self) -> &mut dyn Any {
                self
            }

            fn visit(&mut self, plan: &PhysicalPlan) -> Result<()> {
                if let Some(recluster) = Recluster::from_physical_plan(plan) {
                    if !self.tasks.is_empty() {
                        unreachable!("logic error, expect only one recluster");
                    }

                    self.tasks = recluster.tasks.clone()
                }

                Ok(())
            }
        }

        let mut visitor = TasksCollector::create();
        self.plan.visit(&mut visitor)?;

        let mut tasks = vec![];
        if let Some(visitor) = visitor.as_any().downcast_mut::<TasksCollector>() {
            tasks = visitor.take();
        }

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
            let mut handle = ReclusterDeriveHandle::create(tasks);
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

        struct DataSourceVisitor {
            data_sources: HashMap<u32, DataSource>,
        }

        impl DataSourceVisitor {
            pub fn create() -> Box<dyn PhysicalPlanVisitor> {
                Box::new(DataSourceVisitor {
                    data_sources: HashMap::new(),
                })
            }

            pub fn take(&mut self) -> HashMap<u32, DataSource> {
                std::mem::take(&mut self.data_sources)
            }
        }

        impl PhysicalPlanVisitor for DataSourceVisitor {
            fn as_any(&mut self) -> &mut dyn Any {
                self
            }

            fn visit(&mut self, plan: &PhysicalPlan) -> Result<()> {
                if let Some(scan) = TableScan::from_physical_plan(plan) {
                    self.data_sources
                        .insert(plan.get_id(), DataSource::Table(*scan.source.clone()));
                } else if let Some(scan) = ConstantTableScan::from_physical_plan(plan) {
                    self.data_sources.insert(
                        plan.get_id(),
                        DataSource::ConstTable(ConstTableColumn {
                            columns: scan.values.clone(),
                            num_rows: scan.num_rows,
                        }),
                    );
                }

                Ok(())
            }
        }

        let mut visitor = DataSourceVisitor::create();
        self.plan.visit(&mut visitor)?;

        let data_sources = visitor
            .as_any()
            .downcast_mut::<DataSourceVisitor>()
            .ok_or_else(|| {
                ErrorCode::Internal("Failed to downcast visitor to DataSourceVisitor")
            })?;
        Ok(data_sources.take())
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
    pub fn create(sources: HashMap<u32, DataSource>) -> Box<dyn DeriveHandle> {
        Box::new(ReadSourceDeriveHandle { sources })
    }
}

impl DeriveHandle for ReadSourceDeriveHandle {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn derive(
        &mut self,
        v: &PhysicalPlan,
        children: Vec<PhysicalPlan>,
    ) -> std::result::Result<PhysicalPlan, Vec<PhysicalPlan>> {
        if let Some(table_scan) = TableScan::from_physical_plan(v) {
            let Some(source) = self.sources.remove(&table_scan.get_id()) else {
                unreachable!(
                    "Cannot find data source for table scan plan {}",
                    table_scan.get_id()
                )
            };

            let Ok(source) = DataSourcePlan::try_from(source) else {
                unreachable!("Cannot create data source plan");
            };

            return Ok(PhysicalPlan::new(TableScan {
                source: Box::new(source),
                ..table_scan.clone()
            }));
        } else if let Some(table_scan) = ConstantTableScan::from_physical_plan(v) {
            let Some(source) = self.sources.remove(&table_scan.get_id()) else {
                unreachable!(
                    "Cannot find data source for constant table scan plan {}",
                    table_scan.get_id()
                )
            };

            let Ok(const_table_columns) = ConstTableColumn::try_from(source) else {
                unreachable!("Cannot convert Table to Vec<Column>")
            };

            return Ok(PhysicalPlan::new(ConstantTableScan {
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
    pub fn create(tasks: Vec<ReclusterTask>) -> Box<dyn DeriveHandle> {
        Box::new(ReclusterDeriveHandle { tasks })
    }
}

impl DeriveHandle for ReclusterDeriveHandle {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn derive(
        &mut self,
        v: &PhysicalPlan,
        children: Vec<PhysicalPlan>,
    ) -> std::result::Result<PhysicalPlan, Vec<PhysicalPlan>> {
        let Some(recluster) = Recluster::from_physical_plan(v) else {
            return Err(children);
        };

        Ok(PhysicalPlan::new(Recluster {
            tasks: self.tasks.clone(),
            ..recluster.clone()
        }))
    }
}

struct MutationSourceDeriveHandle {
    partitions: Partitions,
}

impl MutationSourceDeriveHandle {
    pub fn create(partitions: Partitions) -> Box<dyn DeriveHandle> {
        Box::new(MutationSourceDeriveHandle { partitions })
    }
}

impl DeriveHandle for MutationSourceDeriveHandle {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn derive(
        &mut self,
        v: &PhysicalPlan,
        children: Vec<PhysicalPlan>,
    ) -> std::result::Result<PhysicalPlan, Vec<PhysicalPlan>> {
        let Some(mutation_source) = MutationSource::from_physical_plan(v) else {
            return Err(children);
        };

        Ok(PhysicalPlan::new(MutationSource {
            partitions: self.partitions.clone(),
            ..mutation_source.clone()
        }))
    }
}

struct CompactSourceDeriveHandle {
    pub partitions: Partitions,
}

impl CompactSourceDeriveHandle {
    pub fn create(partitions: Partitions) -> Box<dyn DeriveHandle> {
        Box::new(CompactSourceDeriveHandle { partitions })
    }
}

impl DeriveHandle for CompactSourceDeriveHandle {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn derive(
        &mut self,
        v: &PhysicalPlan,
        children: Vec<PhysicalPlan>,
    ) -> std::result::Result<PhysicalPlan, Vec<PhysicalPlan>> {
        let Some(compact_source) = CompactSource::from_physical_plan(v) else {
            return Err(children);
        };

        Ok(PhysicalPlan::new(CompactSource {
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
    pub fn create(
        partitions: Vec<(usize, Location)>,
        slot: Option<BlockSlotDescription>,
        need_insert: bool,
    ) -> Box<dyn DeriveHandle> {
        Box::new(ReplaceDeriveHandle {
            partitions,
            slot,
            need_insert,
        })
    }
}

impl DeriveHandle for ReplaceDeriveHandle {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn derive(
        &mut self,
        v: &PhysicalPlan,
        mut children: Vec<PhysicalPlan>,
    ) -> std::result::Result<PhysicalPlan, Vec<PhysicalPlan>> {
        if let Some(replace_into) = ReplaceInto::from_physical_plan(v) {
            assert_eq!(children.len(), 1);
            return Ok(PhysicalPlan::new(ReplaceInto {
                input: children.remove(0),
                need_insert: self.need_insert,
                segments: self.partitions.clone(),
                block_slots: self.slot.clone(),
                ..replace_into.clone()
            }));
        } else if let Some(replace_deduplicate) = ReplaceDeduplicate::from_physical_plan(v) {
            assert_eq!(children.len(), 1);
            return Ok(PhysicalPlan::new(ReplaceDeduplicate {
                input: children.remove(0),
                need_insert: self.need_insert,
                table_is_empty: self.partitions.is_empty(),
                ..replace_deduplicate.clone()
            }));
        }

        Err(children)
    }
}
