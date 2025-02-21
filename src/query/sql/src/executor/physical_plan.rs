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

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use educe::Educe;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;

use super::physical_plans::AddStreamColumn;
use super::physical_plans::HilbertSerialize;
use super::physical_plans::MutationManipulate;
use super::physical_plans::MutationOrganize;
use super::physical_plans::MutationSource;
use super::physical_plans::MutationSplit;
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

#[derive(serde::Serialize, serde::Deserialize, Educe, EnumAsInner)]
#[educe(
    Clone(bound = false, attrs = "#[recursive::recursive]"),
    Debug(bound = false, attrs = "#[recursive::recursive]")
)]
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
    HilbertSerialize(Box<HilbertSerialize>),

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
}

impl PhysicalPlan {
    /// Adjust the plan_id of the physical plan.
    /// This function will assign a unique plan_id to each physical plan node in a top-down manner.
    /// Which means the plan_id of a node is always greater than the plan_id of its parent node.
    #[recursive::recursive]
    pub fn adjust_plan_id(&mut self, next_id: &mut u32) {
        match self {
            PhysicalPlan::AsyncFunction(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::TableScan(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::Filter(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::EvalScalar(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ProjectSet(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::AggregateExpand(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::AggregatePartial(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::AggregateFinal(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::Window(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::WindowPartition(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::Sort(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::Limit(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::RowFetch(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::HashJoin(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.probe.adjust_plan_id(next_id);
                plan.build.adjust_plan_id(next_id);
            }
            PhysicalPlan::RangeJoin(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.left.adjust_plan_id(next_id);
                plan.right.adjust_plan_id(next_id);
            }
            PhysicalPlan::Exchange(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::UnionAll(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.left.adjust_plan_id(next_id);
                plan.right.adjust_plan_id(next_id);
            }
            PhysicalPlan::RecursiveCteScan(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::ConstantTableScan(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::ExpressionScan(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::CacheScan(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::Udf(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::DistributedInsertSelect(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ExchangeSource(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::ExchangeSink(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::CopyIntoTable(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                match &mut plan.source {
                    CopyIntoTableSource::Query(input) => input.adjust_plan_id(next_id),
                    CopyIntoTableSource::Stage(input) => input.adjust_plan_id(next_id),
                };
            }
            PhysicalPlan::CopyIntoLocation(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ReplaceInto(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::MutationSource(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::ColumnMutation(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::Mutation(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::MutationSplit(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::MutationManipulate(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::MutationOrganize(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::AddStreamColumn(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::CommitSink(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ReplaceAsyncSourcer(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::ReplaceDeduplicate(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::CompactSource(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::Recluster(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::HilbertSerialize(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::Duplicate(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::Shuffle(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkFilter(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkEvalScalar(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkCastSchema(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkFillAndReorder(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkAppendData(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkMerge(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkCommitInsert(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
        }
    }

    /// Get the id of the plan node
    pub fn get_id(&self) -> u32 {
        match self {
            PhysicalPlan::AsyncFunction(v) => v.plan_id,
            PhysicalPlan::TableScan(v) => v.plan_id,
            PhysicalPlan::Filter(v) => v.plan_id,
            PhysicalPlan::EvalScalar(v) => v.plan_id,
            PhysicalPlan::ProjectSet(v) => v.plan_id,
            PhysicalPlan::AggregateExpand(v) => v.plan_id,
            PhysicalPlan::AggregatePartial(v) => v.plan_id,
            PhysicalPlan::AggregateFinal(v) => v.plan_id,
            PhysicalPlan::Window(v) => v.plan_id,
            PhysicalPlan::WindowPartition(v) => v.plan_id,
            PhysicalPlan::Sort(v) => v.plan_id,
            PhysicalPlan::Limit(v) => v.plan_id,
            PhysicalPlan::RowFetch(v) => v.plan_id,
            PhysicalPlan::HashJoin(v) => v.plan_id,
            PhysicalPlan::RangeJoin(v) => v.plan_id,
            PhysicalPlan::Exchange(v) => v.plan_id,
            PhysicalPlan::UnionAll(v) => v.plan_id,
            PhysicalPlan::DistributedInsertSelect(v) => v.plan_id,
            PhysicalPlan::ExchangeSource(v) => v.plan_id,
            PhysicalPlan::ExchangeSink(v) => v.plan_id,
            PhysicalPlan::ConstantTableScan(v) => v.plan_id,
            PhysicalPlan::ExpressionScan(v) => v.plan_id,
            PhysicalPlan::CacheScan(v) => v.plan_id,
            PhysicalPlan::Udf(v) => v.plan_id,
            PhysicalPlan::MutationSource(v) => v.plan_id,
            PhysicalPlan::ColumnMutation(v) => v.plan_id,
            PhysicalPlan::Mutation(v) => v.plan_id,
            PhysicalPlan::MutationSplit(v) => v.plan_id,
            PhysicalPlan::MutationManipulate(v) => v.plan_id,
            PhysicalPlan::MutationOrganize(v) => v.plan_id,
            PhysicalPlan::AddStreamColumn(v) => v.plan_id,
            PhysicalPlan::CommitSink(v) => v.plan_id,
            PhysicalPlan::CopyIntoTable(v) => v.plan_id,
            PhysicalPlan::CopyIntoLocation(v) => v.plan_id,
            PhysicalPlan::ReplaceAsyncSourcer(v) => v.plan_id,
            PhysicalPlan::ReplaceDeduplicate(v) => v.plan_id,
            PhysicalPlan::ReplaceInto(v) => v.plan_id,
            PhysicalPlan::CompactSource(v) => v.plan_id,
            PhysicalPlan::Recluster(v) => v.plan_id,
            PhysicalPlan::HilbertSerialize(v) => v.plan_id,
            PhysicalPlan::Duplicate(v) => v.plan_id,
            PhysicalPlan::Shuffle(v) => v.plan_id,
            PhysicalPlan::ChunkFilter(v) => v.plan_id,
            PhysicalPlan::ChunkEvalScalar(v) => v.plan_id,
            PhysicalPlan::ChunkCastSchema(v) => v.plan_id,
            PhysicalPlan::ChunkFillAndReorder(v) => v.plan_id,
            PhysicalPlan::ChunkAppendData(v) => v.plan_id,
            PhysicalPlan::ChunkMerge(v) => v.plan_id,
            PhysicalPlan::ChunkCommitInsert(v) => v.plan_id,
            PhysicalPlan::RecursiveCteScan(v) => v.plan_id,
        }
    }

    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        match self {
            PhysicalPlan::AsyncFunction(plan) => plan.output_schema(),
            PhysicalPlan::TableScan(plan) => plan.output_schema(),
            PhysicalPlan::Filter(plan) => plan.output_schema(),
            PhysicalPlan::EvalScalar(plan) => plan.output_schema(),
            PhysicalPlan::AggregateExpand(plan) => plan.output_schema(),
            PhysicalPlan::AggregatePartial(plan) => plan.output_schema(),
            PhysicalPlan::AggregateFinal(plan) => plan.output_schema(),
            PhysicalPlan::Window(plan) => plan.output_schema(),
            PhysicalPlan::WindowPartition(plan) => plan.output_schema(),
            PhysicalPlan::Sort(plan) => plan.output_schema(),
            PhysicalPlan::Limit(plan) => plan.output_schema(),
            PhysicalPlan::RowFetch(plan) => plan.output_schema(),
            PhysicalPlan::HashJoin(plan) => plan.output_schema(),
            PhysicalPlan::Exchange(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSource(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSink(plan) => plan.output_schema(),
            PhysicalPlan::UnionAll(plan) => plan.output_schema(),
            PhysicalPlan::ProjectSet(plan) => plan.output_schema(),
            PhysicalPlan::RangeJoin(plan) => plan.output_schema(),
            PhysicalPlan::CopyIntoTable(plan) => plan.output_schema(),
            PhysicalPlan::CopyIntoLocation(plan) => plan.output_schema(),
            PhysicalPlan::ConstantTableScan(plan) => plan.output_schema(),
            PhysicalPlan::ExpressionScan(plan) => plan.output_schema(),
            PhysicalPlan::CacheScan(plan) => plan.output_schema(),
            PhysicalPlan::RecursiveCteScan(plan) => plan.output_schema(),
            PhysicalPlan::Udf(plan) => plan.output_schema(),
            PhysicalPlan::MutationSource(plan) => plan.output_schema(),
            PhysicalPlan::MutationSplit(plan) => plan.output_schema(),
            PhysicalPlan::MutationManipulate(plan) => plan.output_schema(),
            PhysicalPlan::MutationOrganize(plan) => plan.output_schema(),
            PhysicalPlan::AddStreamColumn(plan) => plan.output_schema(),
            PhysicalPlan::Mutation(_)
            | PhysicalPlan::ColumnMutation(_)
            | PhysicalPlan::ReplaceAsyncSourcer(_)
            | PhysicalPlan::ReplaceDeduplicate(_)
            | PhysicalPlan::ReplaceInto(_)
            | PhysicalPlan::CompactSource(_)
            | PhysicalPlan::CommitSink(_)
            | PhysicalPlan::DistributedInsertSelect(_)
            | PhysicalPlan::Recluster(_)
            | PhysicalPlan::HilbertSerialize(_) => Ok(DataSchemaRef::default()),
            PhysicalPlan::Duplicate(plan) => plan.input.output_schema(),
            PhysicalPlan::Shuffle(plan) => plan.input.output_schema(),
            PhysicalPlan::ChunkFilter(plan) => plan.input.output_schema(),
            PhysicalPlan::ChunkEvalScalar(_) => todo!(),
            PhysicalPlan::ChunkCastSchema(_) => todo!(),
            PhysicalPlan::ChunkFillAndReorder(_) => todo!(),
            PhysicalPlan::ChunkAppendData(_) => todo!(),
            PhysicalPlan::ChunkMerge(_) => todo!(),
            PhysicalPlan::ChunkCommitInsert(_) => todo!(),
        }
    }

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
            PhysicalPlan::HilbertSerialize(_) => "HilbertSerialize".to_string(),
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
        }
    }

    pub fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        match self {
            PhysicalPlan::TableScan(_)
            | PhysicalPlan::ConstantTableScan(_)
            | PhysicalPlan::CacheScan(_)
            | PhysicalPlan::ExchangeSource(_)
            | PhysicalPlan::CompactSource(_)
            | PhysicalPlan::ReplaceAsyncSourcer(_)
            | PhysicalPlan::Recluster(_)
            | PhysicalPlan::RecursiveCteScan(_) => Box::new(std::iter::empty()),
            PhysicalPlan::HilbertSerialize(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Filter(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::EvalScalar(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregateExpand(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregatePartial(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregateFinal(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Window(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::WindowPartition(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Sort(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Limit(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::RowFetch(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::HashJoin(plan) => Box::new(
                std::iter::once(plan.probe.as_ref()).chain(std::iter::once(plan.build.as_ref())),
            ),
            PhysicalPlan::ExpressionScan(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Exchange(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ExchangeSink(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::UnionAll(plan) => Box::new(
                std::iter::once(plan.left.as_ref()).chain(std::iter::once(plan.right.as_ref())),
            ),
            PhysicalPlan::DistributedInsertSelect(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::CommitSink(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ProjectSet(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::RangeJoin(plan) => Box::new(
                std::iter::once(plan.left.as_ref()).chain(std::iter::once(plan.right.as_ref())),
            ),
            PhysicalPlan::ReplaceDeduplicate(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::ReplaceInto(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::MutationSource(_) => Box::new(std::iter::empty()),
            PhysicalPlan::ColumnMutation(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Mutation(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::MutationSplit(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::MutationManipulate(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::MutationOrganize(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AddStreamColumn(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Udf(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AsyncFunction(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::CopyIntoLocation(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Duplicate(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Shuffle(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ChunkFilter(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ChunkEvalScalar(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ChunkCastSchema(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ChunkFillAndReorder(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::ChunkAppendData(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ChunkMerge(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ChunkCommitInsert(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::CopyIntoTable(v) => match &v.source {
                CopyIntoTableSource::Query(v) => Box::new(std::iter::once(v.as_ref())),
                CopyIntoTableSource::Stage(v) => Box::new(std::iter::once(v.as_ref())),
            },
        }
    }

    pub fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        match self {
            PhysicalPlan::TableScan(_)
            | PhysicalPlan::ConstantTableScan(_)
            | PhysicalPlan::CacheScan(_)
            | PhysicalPlan::ExchangeSource(_)
            | PhysicalPlan::CompactSource(_)
            | PhysicalPlan::ReplaceAsyncSourcer(_)
            | PhysicalPlan::Recluster(_)
            | PhysicalPlan::RecursiveCteScan(_) => Box::new(std::iter::empty()),
            PhysicalPlan::HilbertSerialize(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::Filter(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::EvalScalar(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::AggregateExpand(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::AggregatePartial(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::AggregateFinal(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::Window(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::WindowPartition(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::Sort(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::Limit(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::RowFetch(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::HashJoin(plan) => Box::new(
                std::iter::once(plan.probe.as_mut()).chain(std::iter::once(plan.build.as_mut())),
            ),
            PhysicalPlan::ExpressionScan(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::Exchange(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::ExchangeSink(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::UnionAll(plan) => Box::new(
                std::iter::once(plan.left.as_mut()).chain(std::iter::once(plan.right.as_mut())),
            ),
            PhysicalPlan::DistributedInsertSelect(plan) => {
                Box::new(std::iter::once(plan.input.as_mut()))
            }
            PhysicalPlan::CommitSink(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::ProjectSet(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::RangeJoin(plan) => Box::new(
                std::iter::once(plan.left.as_mut()).chain(std::iter::once(plan.right.as_mut())),
            ),
            PhysicalPlan::ReplaceDeduplicate(plan) => {
                Box::new(std::iter::once(plan.input.as_mut()))
            }
            PhysicalPlan::ReplaceInto(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::MutationSource(_) => Box::new(std::iter::empty()),
            PhysicalPlan::ColumnMutation(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::Mutation(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::MutationSplit(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::MutationManipulate(plan) => {
                Box::new(std::iter::once(plan.input.as_mut()))
            }
            PhysicalPlan::MutationOrganize(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::AddStreamColumn(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::Udf(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::AsyncFunction(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::CopyIntoLocation(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::Duplicate(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::Shuffle(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::ChunkFilter(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::ChunkEvalScalar(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::ChunkCastSchema(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::ChunkFillAndReorder(plan) => {
                Box::new(std::iter::once(plan.input.as_mut()))
            }
            PhysicalPlan::ChunkAppendData(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::ChunkMerge(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::ChunkCommitInsert(plan) => Box::new(std::iter::once(plan.input.as_mut())),
            PhysicalPlan::CopyIntoTable(v) => match &mut v.source {
                CopyIntoTableSource::Query(v) => Box::new(std::iter::once(v.as_mut())),
                CopyIntoTableSource::Stage(v) => Box::new(std::iter::once(v.as_mut())),
            },
        }
    }

    /// Used to find data source info in a non-aggregation and single-table query plan.
    pub fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        match self {
            PhysicalPlan::TableScan(scan) => Some(&scan.source),
            PhysicalPlan::Filter(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::EvalScalar(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Window(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::WindowPartition(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Sort(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Limit(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Exchange(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::ExchangeSink(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::DistributedInsertSelect(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::ProjectSet(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::RowFetch(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Udf(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::AsyncFunction(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::CopyIntoLocation(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::UnionAll(_)
            | PhysicalPlan::ExchangeSource(_)
            | PhysicalPlan::HashJoin(_)
            | PhysicalPlan::RangeJoin(_)
            | PhysicalPlan::AggregateExpand(_)
            | PhysicalPlan::AggregateFinal(_)
            | PhysicalPlan::AggregatePartial(_)
            | PhysicalPlan::CompactSource(_)
            | PhysicalPlan::CommitSink(_)
            | PhysicalPlan::CopyIntoTable(_)
            | PhysicalPlan::ReplaceAsyncSourcer(_)
            | PhysicalPlan::ReplaceDeduplicate(_)
            | PhysicalPlan::ReplaceInto(_)
            | PhysicalPlan::MutationSource(_)
            | PhysicalPlan::ColumnMutation(_)
            | PhysicalPlan::Mutation(_)
            | PhysicalPlan::MutationSplit(_)
            | PhysicalPlan::MutationManipulate(_)
            | PhysicalPlan::MutationOrganize(_)
            | PhysicalPlan::AddStreamColumn(_)
            | PhysicalPlan::ConstantTableScan(_)
            | PhysicalPlan::ExpressionScan(_)
            | PhysicalPlan::CacheScan(_)
            | PhysicalPlan::RecursiveCteScan(_)
            | PhysicalPlan::Recluster(_)
            | PhysicalPlan::HilbertSerialize(_)
            | PhysicalPlan::Duplicate(_)
            | PhysicalPlan::Shuffle(_)
            | PhysicalPlan::ChunkFilter(_)
            | PhysicalPlan::ChunkEvalScalar(_)
            | PhysicalPlan::ChunkCastSchema(_)
            | PhysicalPlan::ChunkFillAndReorder(_)
            | PhysicalPlan::ChunkAppendData(_)
            | PhysicalPlan::ChunkMerge(_)
            | PhysicalPlan::ChunkCommitInsert(_) => None,
        }
    }

    #[recursive::recursive]
    pub fn is_distributed_plan(&self) -> bool {
        self.children().any(|child| child.is_distributed_plan())
            || matches!(
                self,
                Self::ExchangeSource(_) | Self::ExchangeSink(_) | Self::Exchange(_)
            )
    }

    #[recursive::recursive]
    pub fn is_warehouse_distributed_plan(&self) -> bool {
        self.children()
            .any(|child| child.is_warehouse_distributed_plan())
            || matches!(self, Self::TableScan(v) if v.source.parts.kind == PartitionsShuffleKind::BroadcastWarehouse)
    }

    pub fn get_desc(&self) -> Result<String> {
        Ok(match self {
            PhysicalPlan::TableScan(v) => format!(
                "{}.{}",
                v.source.source_info.catalog_name(),
                v.source.source_info.desc()
            ),
            PhysicalPlan::Filter(v) => match v.predicates.is_empty() {
                true => String::new(),
                false => v.predicates[0].as_expr(&BUILTIN_FUNCTIONS).sql_display(),
            },
            PhysicalPlan::AggregatePartial(v) => {
                v.agg_funcs.iter().map(|x| x.display.clone()).join(", ")
            }
            PhysicalPlan::AggregateFinal(v) => {
                v.agg_funcs.iter().map(|x| x.display.clone()).join(", ")
            }
            PhysicalPlan::Sort(v) => v
                .order_by
                .iter()
                .map(|x| {
                    format!(
                        "{}{}{}",
                        x.display_name,
                        if x.asc { "" } else { " DESC" },
                        if x.nulls_first { " NULLS FIRST" } else { "" },
                    )
                })
                .join(", "),
            PhysicalPlan::Limit(v) => match v.limit {
                Some(limit) => format!("LIMIT {} OFFSET {}", limit, v.offset),
                None => format!("OFFSET {}", v.offset),
            },
            PhysicalPlan::EvalScalar(v) => v
                .exprs
                .iter()
                .map(|(x, _)| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                .join(", "),
            PhysicalPlan::HashJoin(v) => {
                let mut conditions = v
                    .build_keys
                    .iter()
                    .zip(v.probe_keys.iter())
                    .map(|(l, r)| {
                        format!(
                            "({} = {})",
                            l.as_expr(&BUILTIN_FUNCTIONS).sql_display(),
                            r.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                        )
                    })
                    .collect::<Vec<_>>();

                conditions.extend(
                    v.non_equi_conditions
                        .iter()
                        .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display()),
                );

                conditions.join(" AND ")
            }
            PhysicalPlan::ProjectSet(v) => v
                .srf_exprs
                .iter()
                .map(|(x, _)| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                .join(", "),
            PhysicalPlan::AggregateExpand(v) => v
                .grouping_sets
                .sets
                .iter()
                .map(|set| {
                    set.iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .map(|s| format!("({})", s))
                .collect::<Vec<_>>()
                .join(", "),
            PhysicalPlan::Window(v) => {
                let partition_by = v
                    .partition_by
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");

                let order_by = v
                    .order_by
                    .iter()
                    .map(|x| {
                        format!(
                            "{}{}{}",
                            x.display_name,
                            if x.asc { "" } else { " DESC" },
                            if x.nulls_first { " NULLS FIRST" } else { "" },
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");

                format!("partition by {}, order by {}", partition_by, order_by)
            }
            PhysicalPlan::RowFetch(v) => {
                let table_schema = v.source.source_info.schema();
                let projected_schema = v.cols_to_fetch.project_schema(&table_schema);
                projected_schema.fields.iter().map(|f| f.name()).join(", ")
            }
            PhysicalPlan::RangeJoin(v) => {
                let mut condition = v
                    .conditions
                    .iter()
                    .map(|condition| {
                        let left = condition
                            .left_expr
                            .as_expr(&BUILTIN_FUNCTIONS)
                            .sql_display();
                        let right = condition
                            .right_expr
                            .as_expr(&BUILTIN_FUNCTIONS)
                            .sql_display();
                        format!("{left} {:?} {right}", condition.operator)
                    })
                    .collect::<Vec<_>>();

                condition.extend(
                    v.other_conditions
                        .iter()
                        .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display()),
                );

                condition.join(" AND ")
            }
            PhysicalPlan::Udf(v) => v
                .udf_funcs
                .iter()
                .map(|x| format!("{}({})", x.func_name, x.arg_exprs.join(", ")))
                .join(", "),
            PhysicalPlan::UnionAll(v) => v
                .left_outputs
                .iter()
                .zip(v.right_outputs.iter())
                .map(|(l, r)| format!("#{} <- #{}", l.0, r.0))
                .join(", "),
            PhysicalPlan::AsyncFunction(v) => v
                .async_func_descs
                .iter()
                .map(|x| x.display_name.clone())
                .join(", "),
            _ => String::new(),
        })
    }

    pub fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        let mut labels = HashMap::with_capacity(16);

        match self {
            PhysicalPlan::TableScan(v) => {
                labels.insert(String::from("Full table name"), vec![format!(
                    "{}.{}",
                    v.source.source_info.catalog_name(),
                    v.source.source_info.desc()
                )]);

                labels.insert(
                    format!(
                        "Columns ({} / {})",
                        v.output_schema()?.num_fields(),
                        std::cmp::max(
                            v.output_schema()?.num_fields(),
                            v.source.source_info.schema().num_fields(),
                        )
                    ),
                    v.name_mapping.keys().cloned().collect(),
                );
                labels.insert(String::from("Total partitions"), vec![v
                    .source
                    .statistics
                    .partitions_total
                    .to_string()]);
            }
            PhysicalPlan::Filter(v) => {
                labels.insert(
                    String::from("Filter condition"),
                    v.predicates
                        .iter()
                        .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                        .collect(),
                );
            }
            PhysicalPlan::Limit(v) => {
                labels.insert(String::from("Offset"), vec![v.offset.to_string()]);

                if let Some(limit) = v.limit {
                    labels.insert(String::from("Number of rows"), vec![limit.to_string()]);
                }
            }
            PhysicalPlan::EvalScalar(v) => {
                labels.insert(
                    String::from("List of Expressions"),
                    v.exprs
                        .iter()
                        .map(|(x, _)| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                        .collect(),
                );
            }
            PhysicalPlan::AggregatePartial(v) => {
                if !v.group_by_display.is_empty() {
                    labels.insert(String::from("Grouping keys"), v.group_by_display.clone());
                }

                if !v.agg_funcs.is_empty() {
                    labels.insert(
                        String::from("Aggregate Functions"),
                        v.agg_funcs.iter().map(|x| x.display.clone()).collect(),
                    );
                }
            }
            PhysicalPlan::AggregateFinal(v) => {
                if !v.group_by_display.is_empty() {
                    labels.insert(String::from("Grouping keys"), v.group_by_display.clone());
                }

                if !v.agg_funcs.is_empty() {
                    labels.insert(
                        String::from("Aggregate Functions"),
                        v.agg_funcs.iter().map(|x| x.display.clone()).collect(),
                    );
                }
            }
            PhysicalPlan::HashJoin(v) => {
                labels.insert(String::from("Join Type"), vec![v.join_type.to_string()]);

                if !v.build_keys.is_empty() {
                    labels.insert(
                        String::from("Join Build Side Keys"),
                        v.build_keys
                            .iter()
                            .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                            .collect(),
                    );
                }

                if !v.probe_keys.is_empty() {
                    labels.insert(
                        String::from("Join Probe Side Keys"),
                        v.probe_keys
                            .iter()
                            .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                            .collect(),
                    );
                }

                if !v.non_equi_conditions.is_empty() {
                    labels.insert(
                        String::from("Join Conditions"),
                        v.non_equi_conditions
                            .iter()
                            .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                            .collect(),
                    );
                }
            }
            _ => {}
        };

        Ok(labels)
    }

    #[recursive::recursive]
    pub fn try_find_mutation_source(&self) -> Option<MutationSource> {
        match self {
            PhysicalPlan::MutationSource(mutation_source) => Some(mutation_source.clone()),
            _ => {
                for child in self.children() {
                    if let Some(plan) = child.try_find_mutation_source() {
                        return Some(plan);
                    }
                }
                None
            }
        }
    }

    pub fn get_all_data_source(&self, sources: &mut Vec<(u32, Box<DataSourcePlan>)>) {
        match self {
            PhysicalPlan::TableScan(table_scan) => {
                sources.push((table_scan.plan_id, table_scan.source.clone()));
            }
            _ => {
                for child in self.children() {
                    child.get_all_data_source(sources);
                }
            }
        }
    }

    pub fn set_pruning_stats(&mut self, plan_id: u32, stat: PartStatistics) {
        match self {
            PhysicalPlan::TableScan(table_scan) => {
                if table_scan.plan_id == plan_id {
                    table_scan.source.statistics = stat;
                }
            }
            _ => {
                for child in self.children_mut() {
                    child.set_pruning_stats(plan_id, stat.clone())
                }
            }
        }
    }
}
