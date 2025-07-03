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
use std::any::TypeId;
use std::collections::HashMap;
use std::fmt::Debug;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_base::runtime::profile::get_statistics_desc;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_exception::{ErrorCode, Result};
use databend_common_expression::DataSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::PlanProfile;
use educe::Educe;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;

use super::physical_plans::AddStreamColumn;
use super::physical_plans::BroadcastSink;
use super::physical_plans::BroadcastSource;
use super::physical_plans::HilbertPartition;
use super::physical_plans::MutationManipulate;
use super::physical_plans::MutationOrganize;
use super::physical_plans::MutationSource;
use super::physical_plans::MutationSplit;
use crate::executor::format::FormatContext;
use crate::executor::physical_plans::AggregateExpand;
use crate::executor::physical_plans::AggregateFinal;
use crate::executor::physical_plans::AggregatePartial;
use crate::executor::physical_plans::AsyncFunction;
use crate::executor::physical_plans::CacheScan;
use crate::executor::physical_plans::ChunkAppendData;
use crate::executor::physical_plans::ChunkCastSchema;
use crate::executor::physical_plans::ChunkCommitInsert;
use crate::executor::physical_plans::ChunkEvalScalar;
use crate::executor::physical_plans::ChunkFillAndReorder;
use crate::executor::physical_plans::ChunkFilter;
use crate::executor::physical_plans::ChunkMerge;
use crate::executor::physical_plans::ColumnMutation;
use crate::executor::physical_plans::CommitSink;
use crate::executor::physical_plans::CompactSource;
use crate::executor::physical_plans::ConstantTableScan;
use crate::executor::physical_plans::CopyIntoLocation;
use crate::executor::physical_plans::CopyIntoTable;
use crate::executor::physical_plans::CopyIntoTableSource;
use crate::executor::physical_plans::DistributedInsertSelect;
use crate::executor::physical_plans::Duplicate;
use crate::executor::physical_plans::EvalScalar;
use crate::executor::physical_plans::Exchange;
use crate::executor::physical_plans::ExchangeSink;
use crate::executor::physical_plans::ExchangeSource;
use crate::executor::physical_plans::ExpressionScan;
use crate::executor::physical_plans::Filter;
use crate::executor::physical_plans::HashJoin;
use crate::executor::physical_plans::Limit;
use crate::executor::physical_plans::Mutation;
use crate::executor::physical_plans::ProjectSet;
use crate::executor::physical_plans::RangeJoin;
use crate::executor::physical_plans::Recluster;
use crate::executor::physical_plans::RecursiveCteScan;
use crate::executor::physical_plans::ReplaceAsyncSourcer;
use crate::executor::physical_plans::ReplaceDeduplicate;
use crate::executor::physical_plans::ReplaceInto;
use crate::executor::physical_plans::RowFetch;
use crate::executor::physical_plans::Shuffle;
use crate::executor::physical_plans::Sort;
use crate::executor::physical_plans::TableScan;
use crate::executor::physical_plans::Udf;
use crate::executor::physical_plans::UnionAll;
use crate::executor::physical_plans::Window;
use crate::executor::physical_plans::WindowPartition;
use crate::Metadata;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PhysicalPlanMeta {
    plan_id: u32,
    name: String,
}

impl PhysicalPlanMeta {
    pub fn new(name: impl Into<String>) -> PhysicalPlanMeta {
        PhysicalPlanMeta::with_plan_id(name, 0)
    }

    pub fn with_plan_id(name: impl Into<String>, plan_id: u32) -> PhysicalPlanMeta {
        PhysicalPlanMeta {
            plan_id,
            name: name.into(),
        }
    }
}

pub trait DeriveHandle: Send + Sync + 'static {
    fn derive(
        &mut self,
        v: &Box<dyn IPhysicalPlan>,
        children: Vec<Box<dyn IPhysicalPlan>>,
    ) -> std::result::Result<Box<dyn IPhysicalPlan>, Vec<Box<dyn IPhysicalPlan>>>;
}

#[typetag::serde]
pub trait IPhysicalPlan: Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    fn get_meta(&self) -> &PhysicalPlanMeta;

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta;

    // For methods with default implementations, the default implementation is usually sufficient.
    fn get_id(&self) -> u32 {
        self.get_meta().plan_id
    }

    fn get_name(&self) -> String {
        self.get_meta().name.clone()
    }

    /// Adjust the plan_id of the physical plan.
    /// This function will assign a unique plan_id to each physical plan node in a top-down manner.
    /// Which means the plan_id of a node is always greater than the plan_id of its parent node.
    // #[recursive::recursive]
    fn adjust_plan_id(&mut self, next_id: &mut u32) {
        self.get_meta_mut().plan_id = *next_id;
        *next_id += 1;

        for child in self.children_mut() {
            child.adjust_plan_id(next_id);
        }
    }

    fn output_schema(&self) -> Result<DataSchemaRef> {
        match self.children().next() {
            None => Ok(DataSchemaRef::default()),
            Some(child) => child.output_schema(),
        }
    }

    fn children(&self) -> Box<dyn Iterator<Item=&'_ Box<dyn IPhysicalPlan>> + '_> {
        Box::new(std::iter::empty())
    }

    fn children_mut(&mut self) -> Box<dyn Iterator<Item=&'_ mut Box<dyn IPhysicalPlan>> + '_> {
        Box::new(std::iter::empty())
    }

    fn to_format_node(
        &self,
        _ctx: &mut FormatContext<'_>,
        children: Vec<FormatTreeNode<String>>,
    ) -> Result<FormatTreeNode<String>> {
        Ok(FormatTreeNode::with_children(self.get_name(), children))
    }

    /// Used to find data source info in a non-aggregation and single-table query plan.
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        None
    }

    // #[recursive::recursive]
    fn try_find_mutation_source(&self) -> Option<MutationSource> {
        for child in self.children() {
            if let Some(plan) = child.try_find_mutation_source() {
                return Some(plan);
            }
        }

        None
    }

    // #[recursive::recursive]
    fn get_all_data_source(&self, sources: &mut Vec<(u32, Box<DataSourcePlan>)>) {
        for child in self.children() {
            child.get_all_data_source(sources);
        }
    }

    // #[recursive::recursive]
    fn set_pruning_stats(&mut self, stats: &mut HashMap<u32, PartStatistics>) {
        for child in self.children_mut() {
            child.set_pruning_stats(stats)
        }
    }

    // #[recursive::recursive]
    fn is_distributed_plan(&self) -> bool {
        self.children().any(|child| child.is_distributed_plan())
    }

    // #[recursive::recursive]
    fn is_warehouse_distributed_plan(&self) -> bool {
        self.children()
            .any(|child| child.is_warehouse_distributed_plan())
    }

    fn get_desc(&self) -> Result<String> {
        Ok(String::new())
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        Ok(HashMap::new())
    }

    fn derive(&self, children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan>;
}

pub trait PhysicalPlanVisitor {
    fn visit(&mut self, plan: &Box<dyn IPhysicalPlan>) -> Result<()>;
}

pub trait PhysicalPlanDynExt {
    fn format(
        &self,
        metadata: &Metadata,
        profs: HashMap<u32, PlanProfile>,
    ) -> Result<FormatTreeNode<String>> {
        let mut context = FormatContext {
            metadata,
            scan_id_to_runtime_filters: HashMap::new(),
        };

        self.to_format_tree(&profs, &mut context)
    }

    fn to_format_tree(
        &self,
        profs: &HashMap<u32, PlanProfile>,
        ctx: &mut FormatContext<'_>,
    ) -> Result<FormatTreeNode<String>>;

    fn downcast_ref<To: 'static>(&self) -> Option<&To>;

    fn downcast_mut_ref<To: 'static>(&mut self) -> Option<&mut To>;

    fn derive_with(&self, handle: &mut Box<dyn DeriveHandle>) -> Box<dyn IPhysicalPlan>;

    fn visit(&self, visitor: &mut Box<dyn PhysicalPlanVisitor>) -> Result<()>;
}

impl PhysicalPlanDynExt for Box<dyn IPhysicalPlan + 'static> {
    fn to_format_tree(
        &self,
        profs: &HashMap<u32, PlanProfile>,
        ctx: &mut FormatContext<'_>,
    ) -> Result<FormatTreeNode<String>> {
        let mut children = Vec::with_capacity(4);
        for child in self.children() {
            children.push(child.to_format_tree(profs, ctx)?);
        }

        let mut format_tree_node = self.to_format_node(ctx, children)?;

        if let Some(prof) = profs.get(&self.get_id()) {
            let mut children = Vec::with_capacity(format_tree_node.children.len() + 10);
            for (_, desc) in get_statistics_desc().iter() {
                if desc.display_name != "output rows" {
                    continue;
                }
                if prof.statistics[desc.index] != 0 {
                    children.push(FormatTreeNode::new(format!(
                        "{}: {}",
                        desc.display_name.to_lowercase(),
                        desc.human_format(prof.statistics[desc.index])
                    )));
                }
                break;
            }

            children.append(&mut format_tree_node.children);
            format_tree_node.children = children;
        }

        Ok(format_tree_node)
    }

    fn downcast_ref<To: 'static>(&self) -> Option<&To> {
        self.as_any().downcast_ref()
    }

    fn downcast_mut_ref<To: 'static>(&mut self) -> Option<&mut To> {
        // self.as_any().downcast_mut()
        unimplemented!()
    }

    fn derive_with(&self, handle: &mut Box<dyn DeriveHandle>) -> Box<dyn IPhysicalPlan> {
        let mut children = vec![];
        for child in self.children() {
            children.push(child.derive_with(handle));
        }

        self.derive(children)
    }

    fn visit(&self, visitor: &mut Box<dyn PhysicalPlanVisitor>) -> Result<()> {
        for child in self.children() {
            child.visit(visitor)?;
        }

        visitor.visit(self)
    }
}

impl Clone for Box<dyn IPhysicalPlan> {
    fn clone(&self) -> Self {
        let mut children = vec![];
        for child in self.children() {
            children.push(child.clone());
        }

        self.derive(children)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Educe, EnumAsInner)]
#[educe(
    Clone(bound = false, attrs = "#[recursive::recursive]"),
    Debug(bound = false, attrs = "#[recursive::recursive]")
)]
#[allow(clippy::large_enum_variant)]
pub enum PhysicalPlan {
    /// Query
    TableScan(TableScan),
    Filter(Filter),
    EvalScalar(EvalScalar),
    ProjectSet(ProjectSet),
    AggregateExpand(AggregateExpand),
    AggregatePartial(AggregatePartial),
    AggregateFinal(AggregateFinal),
    Window(Window),
    Sort(Sort),
    WindowPartition(WindowPartition),
    Limit(Limit),
    RowFetch(RowFetch),
    HashJoin(HashJoin),
    RangeJoin(RangeJoin),
    Exchange(Exchange),
    UnionAll(UnionAll),
    ConstantTableScan(ConstantTableScan),
    ExpressionScan(ExpressionScan),
    CacheScan(CacheScan),
    Udf(Udf),
    RecursiveCteScan(RecursiveCteScan),

    /// For insert into ... select ... in cluster
    DistributedInsertSelect(Box<DistributedInsertSelect>),

    /// Synthesized by fragmented
    ExchangeSource(ExchangeSource),
    ExchangeSink(ExchangeSink),

    /// Copy into table
    CopyIntoTable(Box<CopyIntoTable>),
    CopyIntoLocation(Box<CopyIntoLocation>),

    /// Replace
    ReplaceAsyncSourcer(ReplaceAsyncSourcer),
    ReplaceDeduplicate(Box<ReplaceDeduplicate>),
    ReplaceInto(Box<ReplaceInto>),

    /// Mutation
    Mutation(Box<Mutation>),
    MutationSplit(Box<MutationSplit>),
    MutationManipulate(Box<MutationManipulate>),
    MutationOrganize(Box<MutationOrganize>),
    AddStreamColumn(Box<AddStreamColumn>),
    ColumnMutation(ColumnMutation),
    MutationSource(MutationSource),

    /// Compact
    CompactSource(Box<CompactSource>),

    /// Commit
    CommitSink(Box<CommitSink>),

    /// Recluster
    Recluster(Box<Recluster>),
    HilbertPartition(Box<HilbertPartition>),

    /// Multi table insert
    Duplicate(Box<Duplicate>),
    Shuffle(Box<Shuffle>),
    ChunkFilter(Box<ChunkFilter>),
    ChunkEvalScalar(Box<ChunkEvalScalar>),
    ChunkCastSchema(Box<ChunkCastSchema>),
    ChunkFillAndReorder(Box<ChunkFillAndReorder>),
    ChunkAppendData(Box<ChunkAppendData>),
    ChunkMerge(Box<ChunkMerge>),
    ChunkCommitInsert(Box<ChunkCommitInsert>),

    // async function call
    AsyncFunction(AsyncFunction),

    // broadcast
    BroadcastSource(BroadcastSource),
    BroadcastSink(BroadcastSink),
}

impl PhysicalPlan {
    pub fn name(&self) -> String {
        match self {
            PhysicalPlan::TableScan(v) => match &v.source.source_info {
                DataSourceInfo::TableSource(_) => "TableScan".to_string(),
                DataSourceInfo::StageSource(_) => "StageScan".to_string(),
                DataSourceInfo::ParquetSource(_) => "ParquetScan".to_string(),
                DataSourceInfo::ResultScanSource(_) => "ResultScan".to_string(),
                DataSourceInfo::ORCSource(_) => "OrcScan".to_string(),
            },
            PhysicalPlan::AsyncFunction(_) => "AsyncFunction".to_string(),
            PhysicalPlan::Filter(_) => "Filter".to_string(),
            PhysicalPlan::EvalScalar(_) => "EvalScalar".to_string(),
            PhysicalPlan::AggregateExpand(_) => "AggregateExpand".to_string(),
            PhysicalPlan::AggregatePartial(_) => "AggregatePartial".to_string(),
            PhysicalPlan::AggregateFinal(_) => "AggregateFinal".to_string(),
            PhysicalPlan::Window(_) => "Window".to_string(),
            PhysicalPlan::WindowPartition(_) => "WindowPartition".to_string(),
            PhysicalPlan::Sort(_) => "Sort".to_string(),
            PhysicalPlan::Limit(_) => "Limit".to_string(),
            PhysicalPlan::RowFetch(_) => "RowFetch".to_string(),
            PhysicalPlan::HashJoin(_) => "HashJoin".to_string(),
            PhysicalPlan::Exchange(_) => "Exchange".to_string(),
            PhysicalPlan::UnionAll(_) => "UnionAll".to_string(),
            PhysicalPlan::DistributedInsertSelect(_) => "DistributedInsertSelect".to_string(),
            PhysicalPlan::ExchangeSource(_) => "Exchange Source".to_string(),
            PhysicalPlan::ExchangeSink(_) => "Exchange Sink".to_string(),
            PhysicalPlan::ProjectSet(_) => "Unnest".to_string(),
            PhysicalPlan::CompactSource(_) => "CompactBlock".to_string(),
            PhysicalPlan::CommitSink(_) => "CommitSink".to_string(),
            PhysicalPlan::RangeJoin(_) => "RangeJoin".to_string(),
            PhysicalPlan::CopyIntoTable(_) => "CopyIntoTable".to_string(),
            PhysicalPlan::CopyIntoLocation(_) => "CopyIntoLocation".to_string(),
            PhysicalPlan::ReplaceAsyncSourcer(_) => "ReplaceAsyncSourcer".to_string(),
            PhysicalPlan::ReplaceDeduplicate(_) => "ReplaceDeduplicate".to_string(),
            PhysicalPlan::ReplaceInto(_) => "Replace".to_string(),
            PhysicalPlan::MutationSource(_) => "MutationSource".to_string(),
            PhysicalPlan::ColumnMutation(_) => "ColumnMutation".to_string(),
            PhysicalPlan::Mutation(_) => "MergeInto".to_string(),
            PhysicalPlan::MutationSplit(_) => "MutationSplit".to_string(),
            PhysicalPlan::MutationManipulate(_) => "MutationManipulate".to_string(),
            PhysicalPlan::MutationOrganize(_) => "MutationOrganize".to_string(),
            PhysicalPlan::AddStreamColumn(_) => "AddStreamColumn".to_string(),
            PhysicalPlan::RecursiveCteScan(_) => "RecursiveCteScan".to_string(),
            PhysicalPlan::ConstantTableScan(_) => "PhysicalConstantTableScan".to_string(),
            PhysicalPlan::ExpressionScan(_) => "ExpressionScan".to_string(),
            PhysicalPlan::CacheScan(_) => "CacheScan".to_string(),
            PhysicalPlan::Recluster(_) => "Recluster".to_string(),
            PhysicalPlan::HilbertPartition(_) => "HilbertPartition".to_string(),
            PhysicalPlan::Udf(_) => "Udf".to_string(),
            PhysicalPlan::Duplicate(_) => "Duplicate".to_string(),
            PhysicalPlan::Shuffle(_) => "Shuffle".to_string(),
            PhysicalPlan::ChunkFilter(_) => "Filter".to_string(),
            PhysicalPlan::ChunkEvalScalar(_) => "EvalScalar".to_string(),
            PhysicalPlan::ChunkCastSchema(_) => "CastSchema".to_string(),
            PhysicalPlan::ChunkFillAndReorder(_) => "FillAndReorder".to_string(),
            PhysicalPlan::ChunkAppendData(_) => "WriteData".to_string(),
            PhysicalPlan::ChunkMerge(_) => "ChunkMerge".to_string(),
            PhysicalPlan::ChunkCommitInsert(_) => "Commit".to_string(),
            PhysicalPlan::BroadcastSource(_) => "RuntimeFilterSource".to_string(),
            PhysicalPlan::BroadcastSink(_) => "RuntimeFilterSink".to_string(),
        }
    }
}
