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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_ast::ast::FormatTreeNode;
use databend_common_base::runtime::profile::ProfileLabel;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline::core::PlanProfile;
use databend_common_pipeline::core::PlanScope;
use databend_common_sql::Metadata;
use dyn_clone::DynClone;
use serde::de::Error as DeError;
use serde::Deserializer;
use serde::Serializer;
use serde_json::Value as JsonValue;

use crate::physical_plans::format::FormatContext;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::SimplePhysicalFormat;
use crate::physical_plans::ExchangeSink;
use crate::physical_plans::MutationSource;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PhysicalPlanMeta {
    pub plan_id: u32,
    pub name: String,
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
    fn as_any(&mut self) -> &mut dyn Any;

    fn derive(
        &mut self,
        v: &PhysicalPlan,
        children: Vec<PhysicalPlan>,
    ) -> std::result::Result<PhysicalPlan, Vec<PhysicalPlan>>;
}

pub(crate) trait IPhysicalPlan: DynClone + Debug + Send + Sync + 'static {
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
    #[recursive::recursive]
    fn adjust_plan_id(&mut self, next_id: &mut u32) {
        self.get_meta_mut().plan_id = *next_id;
        *next_id += 1;

        for child in self.children_mut() {
            child.adjust_plan_id(next_id);
        }
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        match self.children().next() {
            None => Ok(DataSchemaRef::default()),
            Some(child) => child.output_schema(),
        }
    }

    fn children(&self) -> Box<dyn Iterator<Item = &'_ PhysicalPlan> + '_> {
        Box::new(std::iter::empty())
    }

    fn children_mut(&mut self) -> Box<dyn Iterator<Item = &'_ mut PhysicalPlan> + '_> {
        Box::new(std::iter::empty())
    }

    #[recursive::recursive]
    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        let mut children = vec![];
        for child in self.children() {
            children.push(child.formatter()?);
        }

        Ok(SimplePhysicalFormat::create(self.get_meta(), children))
    }

    /// Used to find data source info in a non-aggregation and single-table query plan.
    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        None
    }

    #[recursive::recursive]
    fn try_find_mutation_source(&self) -> Option<MutationSource> {
        for child in self.children() {
            if let Some(plan) = child.try_find_mutation_source() {
                return Some(plan);
            }
        }

        None
    }

    #[recursive::recursive]
    fn get_all_data_source(&self, sources: &mut Vec<(u32, Box<DataSourcePlan>)>) {
        for child in self.children() {
            child.get_all_data_source(sources);
        }
    }

    #[recursive::recursive]
    fn set_pruning_stats(&mut self, stats: &mut HashMap<u32, PartStatistics>) {
        for child in self.children_mut() {
            child.set_pruning_stats(stats)
        }
    }

    #[recursive::recursive]
    fn is_distributed_plan(&self) -> bool {
        self.children().any(|child| child.is_distributed_plan())
    }

    #[recursive::recursive]
    fn is_warehouse_distributed_plan(&self) -> bool {
        self.children()
            .any(|child| child.is_warehouse_distributed_plan())
    }

    fn display_in_profile(&self) -> bool {
        true
    }

    fn get_desc(&self) -> Result<String> {
        Ok(String::new())
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        Ok(HashMap::new())
    }

    fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan;

    #[recursive::recursive]
    fn build_pipeline(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let is_exchange_sink = self.as_any().downcast_ref::<ExchangeSink>().is_some();
        builder.is_exchange_stack.push(is_exchange_sink);

        if !self.display_in_profile() {
            self.build_pipeline2(builder)?;
            builder.is_exchange_stack.pop();
            return Ok(());
        }

        let desc = self.get_desc()?;
        let plan_labels = self.get_labels()?;
        let mut profile_labels = Vec::with_capacity(plan_labels.len());
        for (name, value) in plan_labels {
            profile_labels.push(ProfileLabel::create(name, value));
        }

        let scope = PlanScope::create(
            self.get_id(),
            self.get_name(),
            Arc::new(desc),
            Arc::new(profile_labels),
        );

        let _guard = scope.enter_scope_guard();
        self.build_pipeline2(builder)?;
        builder.is_exchange_stack.pop();
        Ok(())
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let _ = builder;
        Err(ErrorCode::Unimplemented(format!(
            "UnImplement build_pipeline method for {:?}",
            self.get_name()
        )))
    }
}

pub trait PhysicalPlanVisitor: Send + Sync + 'static {
    fn as_any(&mut self) -> &mut dyn Any;

    fn visit(&mut self, plan: &PhysicalPlan) -> Result<()>;
}

pub trait VisitorCast {
    fn from_visitor(x: &mut Box<dyn PhysicalPlanVisitor>) -> &mut Self;
}

impl<T: PhysicalPlanVisitor> VisitorCast for T {
    fn from_visitor(x: &mut Box<dyn PhysicalPlanVisitor>) -> &mut T {
        match x.as_any().downcast_mut::<T>() {
            Some(x) => x,
            None => unreachable!(),
        }
    }
}

pub trait PhysicalPlanCast {
    fn check_physical_plan(plan: &PhysicalPlan) -> bool;

    fn from_physical_plan(plan: &PhysicalPlan) -> Option<&Self>;

    fn from_mut_physical_plan(plan: &mut PhysicalPlan) -> Option<&mut Self>;
}

impl<T: IPhysicalPlan> PhysicalPlanCast for T {
    fn check_physical_plan(plan: &PhysicalPlan) -> bool {
        plan.as_any().downcast_ref::<T>().is_some()
    }

    fn from_physical_plan(plan: &PhysicalPlan) -> Option<&T> {
        plan.as_any().downcast_ref()
    }

    fn from_mut_physical_plan(plan: &mut PhysicalPlan) -> Option<&mut T> {
        unsafe {
            match T::from_physical_plan(plan) {
                None => None,
                #[allow(invalid_reference_casting)]
                Some(v) => Some(&mut *(v as *const T as *mut T)),
            }
        }
    }
}

macro_rules! define_physical_plan_impl {
    ( $( $variant:ident => $path:path ),+ $(,)? ) => {
        #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
        pub(crate) enum PhysicalPlanImpl {
            $( $variant($path), )+
            #[cfg(test)]
            StackDepthPlan(crate::physical_plans::physical_plan::tests::StackDepthPlan),
        }

        $( impl From<$path> for PhysicalPlanImpl {
            fn from(v: $path) -> Self {
                PhysicalPlanImpl::$variant(v)
            }
        })+

        impl PhysicalPlanImpl {
            fn try_from_typetag_value<E: serde::de::Error>(
                type_name: &str,
                value: JsonValue,
            ) -> std::result::Result<Option<Self>, E> {
                $({
                    if type_name == std::any::type_name::<$path>()
                        || type_name == stringify!($variant)
                    {
                        let value = serde_json::from_value::<$path>(value).map_err(E::custom)?;
                        return Ok(Some(PhysicalPlanImpl::$variant(value)));
                    }
                })+

                #[cfg(test)]
                if type_name
                    == std::any::type_name::<crate::physical_plans::physical_plan::tests::StackDepthPlan>()
                    || type_name == stringify!(StackDepthPlan)
                {
                    let value = serde_json::from_value::<crate::physical_plans::physical_plan::tests::StackDepthPlan>(value)
                        .map_err(E::custom)?;
                    return Ok(Some(PhysicalPlanImpl::StackDepthPlan(value)));
                }

                Ok(None)
            }
        }
    };
}

define_physical_plan_impl!(
    AddStreamColumn => crate::physical_plans::physical_add_stream_column::AddStreamColumn,
    AggregateExpand => crate::physical_plans::physical_aggregate_expand::AggregateExpand,
    AggregateFinal => crate::physical_plans::physical_aggregate_final::AggregateFinal,
    AggregatePartial => crate::physical_plans::physical_aggregate_partial::AggregatePartial,
    AsyncFunction => crate::physical_plans::physical_async_func::AsyncFunction,
    BroadcastSink => crate::physical_plans::physical_broadcast::BroadcastSink,
    BroadcastSource => crate::physical_plans::physical_broadcast::BroadcastSource,
    CacheScan => crate::physical_plans::physical_cache_scan::CacheScan,
    ChunkAppendData => crate::physical_plans::physical_multi_table_insert::ChunkAppendData,
    ChunkCastSchema => crate::physical_plans::physical_multi_table_insert::ChunkCastSchema,
    ChunkCommitInsert => crate::physical_plans::physical_multi_table_insert::ChunkCommitInsert,
    ChunkEvalScalar => crate::physical_plans::physical_multi_table_insert::ChunkEvalScalar,
    ChunkFillAndReorder => crate::physical_plans::physical_multi_table_insert::ChunkFillAndReorder,
    ChunkFilter => crate::physical_plans::physical_multi_table_insert::ChunkFilter,
    ChunkMerge => crate::physical_plans::physical_multi_table_insert::ChunkMerge,
    ColumnMutation => crate::physical_plans::physical_column_mutation::ColumnMutation,
    CommitSink => crate::physical_plans::physical_commit_sink::CommitSink,
    CompactSource => crate::physical_plans::physical_compact_source::CompactSource,
    ConstantTableScan => crate::physical_plans::physical_constant_table_scan::ConstantTableScan,
    CopyIntoLocation => crate::physical_plans::physical_copy_into_location::CopyIntoLocation,
    CopyIntoTable => crate::physical_plans::physical_copy_into_table::CopyIntoTable,
    DistributedInsertSelect => crate::physical_plans::physical_distributed_insert_select::DistributedInsertSelect,
    Duplicate => crate::physical_plans::physical_multi_table_insert::Duplicate,
    EvalScalar => crate::physical_plans::physical_eval_scalar::EvalScalar,
    Exchange => crate::physical_plans::physical_exchange::Exchange,
    ExchangeSink => crate::physical_plans::physical_exchange_sink::ExchangeSink,
    ExchangeSource => crate::physical_plans::physical_exchange_source::ExchangeSource,
    ExpressionScan => crate::physical_plans::physical_expression_scan::ExpressionScan,
    Filter => crate::physical_plans::physical_filter::Filter,
    HashJoin => crate::physical_plans::physical_hash_join::HashJoin,
    HilbertPartition => crate::physical_plans::physical_recluster::HilbertPartition,
    Limit => crate::physical_plans::physical_limit::Limit,
    MaterializeCTERef => crate::physical_plans::physical_cte_consumer::MaterializeCTERef,
    MaterializedCTE => crate::physical_plans::physical_materialized_cte::MaterializedCTE,
    Mutation => crate::physical_plans::physical_mutation::Mutation,
    MutationManipulate => crate::physical_plans::physical_mutation_manipulate::MutationManipulate,
    MutationOrganize => crate::physical_plans::physical_mutation_into_organize::MutationOrganize,
    MutationSource => crate::physical_plans::physical_mutation_source::MutationSource,
    MutationSplit => crate::physical_plans::physical_mutation_into_split::MutationSplit,
    ProjectSet => crate::physical_plans::physical_project_set::ProjectSet,
    RangeJoin => crate::physical_plans::physical_range_join::RangeJoin,
    Recluster => crate::physical_plans::physical_recluster::Recluster,
    RecursiveCteScan => crate::physical_plans::physical_r_cte_scan::RecursiveCteScan,
    ReplaceAsyncSourcer => crate::physical_plans::physical_replace_async_source::ReplaceAsyncSourcer,
    ReplaceDeduplicate => crate::physical_plans::physical_replace_deduplicate::ReplaceDeduplicate,
    ReplaceInto => crate::physical_plans::physical_replace_into::ReplaceInto,
    RowFetch => crate::physical_plans::physical_row_fetch::RowFetch,
    SecureFilter => crate::physical_plans::physical_secure_filter::SecureFilter,
    Sequence => crate::physical_plans::physical_sequence::Sequence,
    SerializedPhysicalPlanRef => crate::servers::flight::v1::packets::packet_fragment::SerializedPhysicalPlanRef,
    Shuffle => crate::physical_plans::physical_multi_table_insert::Shuffle,
    Sort => crate::physical_plans::physical_sort::Sort,
    TableScan => crate::physical_plans::physical_table_scan::TableScan,
    Udf => crate::physical_plans::physical_udf::Udf,
    UnionAll => crate::physical_plans::physical_union_all::UnionAll,
    Window => crate::physical_plans::physical_window::Window,
    WindowPartition => crate::physical_plans::physical_window_partition::WindowPartition,
);

macro_rules! dispatch_plan_ref {
    ($s:expr, $plan:ident => $body:expr) => {
        match $s.inner.as_ref() {
            PhysicalPlanImpl::AddStreamColumn($plan) => $body,
            PhysicalPlanImpl::AggregateExpand($plan) => $body,
            PhysicalPlanImpl::AggregateFinal($plan) => $body,
            PhysicalPlanImpl::AggregatePartial($plan) => $body,
            PhysicalPlanImpl::AsyncFunction($plan) => $body,
            PhysicalPlanImpl::BroadcastSink($plan) => $body,
            PhysicalPlanImpl::BroadcastSource($plan) => $body,
            PhysicalPlanImpl::CacheScan($plan) => $body,
            PhysicalPlanImpl::ChunkAppendData($plan) => $body,
            PhysicalPlanImpl::ChunkCastSchema($plan) => $body,
            PhysicalPlanImpl::ChunkCommitInsert($plan) => $body,
            PhysicalPlanImpl::ChunkEvalScalar($plan) => $body,
            PhysicalPlanImpl::ChunkFillAndReorder($plan) => $body,
            PhysicalPlanImpl::ChunkFilter($plan) => $body,
            PhysicalPlanImpl::ChunkMerge($plan) => $body,
            PhysicalPlanImpl::ColumnMutation($plan) => $body,
            PhysicalPlanImpl::CommitSink($plan) => $body,
            PhysicalPlanImpl::CompactSource($plan) => $body,
            PhysicalPlanImpl::ConstantTableScan($plan) => $body,
            PhysicalPlanImpl::CopyIntoLocation($plan) => $body,
            PhysicalPlanImpl::CopyIntoTable($plan) => $body,
            PhysicalPlanImpl::DistributedInsertSelect($plan) => $body,
            PhysicalPlanImpl::Duplicate($plan) => $body,
            PhysicalPlanImpl::EvalScalar($plan) => $body,
            PhysicalPlanImpl::Exchange($plan) => $body,
            PhysicalPlanImpl::ExchangeSink($plan) => $body,
            PhysicalPlanImpl::ExchangeSource($plan) => $body,
            PhysicalPlanImpl::ExpressionScan($plan) => $body,
            PhysicalPlanImpl::Filter($plan) => $body,
            PhysicalPlanImpl::HashJoin($plan) => $body,
            PhysicalPlanImpl::HilbertPartition($plan) => $body,
            PhysicalPlanImpl::Limit($plan) => $body,
            PhysicalPlanImpl::MaterializeCTERef($plan) => $body,
            PhysicalPlanImpl::MaterializedCTE($plan) => $body,
            PhysicalPlanImpl::Mutation($plan) => $body,
            PhysicalPlanImpl::MutationManipulate($plan) => $body,
            PhysicalPlanImpl::MutationOrganize($plan) => $body,
            PhysicalPlanImpl::MutationSource($plan) => $body,
            PhysicalPlanImpl::MutationSplit($plan) => $body,
            PhysicalPlanImpl::ProjectSet($plan) => $body,
            PhysicalPlanImpl::RangeJoin($plan) => $body,
            PhysicalPlanImpl::Recluster($plan) => $body,
            PhysicalPlanImpl::RecursiveCteScan($plan) => $body,
            PhysicalPlanImpl::ReplaceAsyncSourcer($plan) => $body,
            PhysicalPlanImpl::ReplaceDeduplicate($plan) => $body,
            PhysicalPlanImpl::ReplaceInto($plan) => $body,
            PhysicalPlanImpl::RowFetch($plan) => $body,
            PhysicalPlanImpl::SecureFilter($plan) => $body,
            PhysicalPlanImpl::Sequence($plan) => $body,
            PhysicalPlanImpl::SerializedPhysicalPlanRef($plan) => $body,
            PhysicalPlanImpl::Shuffle($plan) => $body,
            PhysicalPlanImpl::Sort($plan) => $body,
            PhysicalPlanImpl::TableScan($plan) => $body,
            PhysicalPlanImpl::Udf($plan) => $body,
            PhysicalPlanImpl::UnionAll($plan) => $body,
            PhysicalPlanImpl::Window($plan) => $body,
            PhysicalPlanImpl::WindowPartition($plan) => $body,
            #[cfg(test)]
            PhysicalPlanImpl::StackDepthPlan($plan) => $body,
        }
    };
}

macro_rules! dispatch_plan_mut {
    ($s:expr, $plan:ident => $body:expr) => {
        match $s.inner.as_mut() {
            PhysicalPlanImpl::AddStreamColumn($plan) => $body,
            PhysicalPlanImpl::AggregateExpand($plan) => $body,
            PhysicalPlanImpl::AggregateFinal($plan) => $body,
            PhysicalPlanImpl::AggregatePartial($plan) => $body,
            PhysicalPlanImpl::AsyncFunction($plan) => $body,
            PhysicalPlanImpl::BroadcastSink($plan) => $body,
            PhysicalPlanImpl::BroadcastSource($plan) => $body,
            PhysicalPlanImpl::CacheScan($plan) => $body,
            PhysicalPlanImpl::ChunkAppendData($plan) => $body,
            PhysicalPlanImpl::ChunkCastSchema($plan) => $body,
            PhysicalPlanImpl::ChunkCommitInsert($plan) => $body,
            PhysicalPlanImpl::ChunkEvalScalar($plan) => $body,
            PhysicalPlanImpl::ChunkFillAndReorder($plan) => $body,
            PhysicalPlanImpl::ChunkFilter($plan) => $body,
            PhysicalPlanImpl::ChunkMerge($plan) => $body,
            PhysicalPlanImpl::ColumnMutation($plan) => $body,
            PhysicalPlanImpl::CommitSink($plan) => $body,
            PhysicalPlanImpl::CompactSource($plan) => $body,
            PhysicalPlanImpl::ConstantTableScan($plan) => $body,
            PhysicalPlanImpl::CopyIntoLocation($plan) => $body,
            PhysicalPlanImpl::CopyIntoTable($plan) => $body,
            PhysicalPlanImpl::DistributedInsertSelect($plan) => $body,
            PhysicalPlanImpl::Duplicate($plan) => $body,
            PhysicalPlanImpl::EvalScalar($plan) => $body,
            PhysicalPlanImpl::Exchange($plan) => $body,
            PhysicalPlanImpl::ExchangeSink($plan) => $body,
            PhysicalPlanImpl::ExchangeSource($plan) => $body,
            PhysicalPlanImpl::ExpressionScan($plan) => $body,
            PhysicalPlanImpl::Filter($plan) => $body,
            PhysicalPlanImpl::HashJoin($plan) => $body,
            PhysicalPlanImpl::HilbertPartition($plan) => $body,
            PhysicalPlanImpl::Limit($plan) => $body,
            PhysicalPlanImpl::MaterializeCTERef($plan) => $body,
            PhysicalPlanImpl::MaterializedCTE($plan) => $body,
            PhysicalPlanImpl::Mutation($plan) => $body,
            PhysicalPlanImpl::MutationManipulate($plan) => $body,
            PhysicalPlanImpl::MutationOrganize($plan) => $body,
            PhysicalPlanImpl::MutationSource($plan) => $body,
            PhysicalPlanImpl::MutationSplit($plan) => $body,
            PhysicalPlanImpl::ProjectSet($plan) => $body,
            PhysicalPlanImpl::RangeJoin($plan) => $body,
            PhysicalPlanImpl::Recluster($plan) => $body,
            PhysicalPlanImpl::RecursiveCteScan($plan) => $body,
            PhysicalPlanImpl::ReplaceAsyncSourcer($plan) => $body,
            PhysicalPlanImpl::ReplaceDeduplicate($plan) => $body,
            PhysicalPlanImpl::ReplaceInto($plan) => $body,
            PhysicalPlanImpl::RowFetch($plan) => $body,
            PhysicalPlanImpl::SecureFilter($plan) => $body,
            PhysicalPlanImpl::Sequence($plan) => $body,
            PhysicalPlanImpl::SerializedPhysicalPlanRef($plan) => $body,
            PhysicalPlanImpl::Shuffle($plan) => $body,
            PhysicalPlanImpl::Sort($plan) => $body,
            PhysicalPlanImpl::TableScan($plan) => $body,
            PhysicalPlanImpl::Udf($plan) => $body,
            PhysicalPlanImpl::UnionAll($plan) => $body,
            PhysicalPlanImpl::Window($plan) => $body,
            PhysicalPlanImpl::WindowPartition($plan) => $body,
            #[cfg(test)]
            PhysicalPlanImpl::StackDepthPlan($plan) => $body,
        }
    };
}

#[cfg(test)]
impl From<crate::physical_plans::physical_plan::tests::StackDepthPlan> for PhysicalPlanImpl {
    fn from(v: crate::physical_plans::physical_plan::tests::StackDepthPlan) -> Self {
        PhysicalPlanImpl::StackDepthPlan(v)
    }
}

pub struct PhysicalPlan {
    inner: Box<PhysicalPlanImpl>,
}

dyn_clone::clone_trait_object!(IPhysicalPlan);

impl Clone for PhysicalPlan {
    #[recursive::recursive]
    fn clone(&self) -> Self {
        PhysicalPlan {
            inner: self.inner.clone(),
        }
    }
}

impl Debug for PhysicalPlan {
    #[recursive::recursive]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl serde::Serialize for PhysicalPlan {
    #[recursive::recursive]
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        PhysicalPlanEnvelopeRef::V2(self.inner.as_ref()).serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for PhysicalPlan {
    #[recursive::recursive]
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let envelope = PhysicalPlanEnvelope::deserialize(deserializer)?;

        let inner = match envelope {
            PhysicalPlanEnvelope::V2(plan) => plan,
            PhysicalPlanEnvelope::V1(LegacyPhysicalPlanImpl(plan)) => plan,
        };

        Ok(PhysicalPlan {
            inner: Box::new(inner),
        })
    }
}

#[derive(serde::Serialize)]
#[serde(untagged)]
enum PhysicalPlanEnvelopeRef<'a> {
    V2(&'a PhysicalPlanImpl),
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum PhysicalPlanEnvelope {
    V2(PhysicalPlanImpl),
    V1(LegacyPhysicalPlanImpl),
}

struct LegacyPhysicalPlanImpl(PhysicalPlanImpl);

impl<'de> serde::Deserialize<'de> for LegacyPhysicalPlanImpl {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = JsonValue::deserialize(deserializer)?;
        let Some((type_name, rest)) = extract_typetag_value(value) else {
            return Err(DeError::custom("missing typetag 'type' field"));
        };

        let Some(plan) = PhysicalPlanImpl::try_from_typetag_value::<D::Error>(&type_name, rest)?
        else {
            return Err(DeError::custom(format!(
                "unknown typetag type: {type_name}"
            )));
        };

        Ok(LegacyPhysicalPlanImpl(plan))
    }
}

fn extract_typetag_value(value: JsonValue) -> Option<(String, JsonValue)> {
    let JsonValue::Object(mut map) = value else {
        return None;
    };

    let type_value = map.remove("type")?;
    let type_name = type_value.as_str()?.to_string();

    Some((type_name, JsonValue::Object(map)))
}

impl PhysicalPlan {
    #[allow(private_bounds)]
    pub fn new<T>(inner: T) -> PhysicalPlan
    where PhysicalPlanImpl: From<T> {
        PhysicalPlan {
            inner: Box::new(inner.into()),
        }
    }

    pub fn as_any(&self) -> &dyn Any {
        dispatch_plan_ref!(self, v => v.as_any())
    }

    pub fn get_meta(&self) -> &PhysicalPlanMeta {
        dispatch_plan_ref!(self, v => v.get_meta())
    }

    pub fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        dispatch_plan_mut!(self, v => v.get_meta_mut())
    }

    pub fn get_id(&self) -> u32 {
        dispatch_plan_ref!(self, v => v.get_id())
    }

    pub fn get_name(&self) -> String {
        dispatch_plan_ref!(self, v => v.get_name())
    }

    pub fn adjust_plan_id(&mut self, next_id: &mut u32) {
        dispatch_plan_mut!(self, v => v.adjust_plan_id(next_id))
    }

    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        dispatch_plan_ref!(self, v => v.output_schema())
    }

    pub fn children(&self) -> Box<dyn Iterator<Item = &'_ PhysicalPlan> + '_> {
        dispatch_plan_ref!(self, v => v.children())
    }

    pub fn children_mut(&mut self) -> Box<dyn Iterator<Item = &'_ mut PhysicalPlan> + '_> {
        dispatch_plan_mut!(self, v => v.children_mut())
    }

    pub fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        dispatch_plan_ref!(self, v => v.formatter())
    }

    pub fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        dispatch_plan_ref!(self, v => v.try_find_single_data_source())
    }

    pub fn try_find_mutation_source(&self) -> Option<MutationSource> {
        dispatch_plan_ref!(self, v => v.try_find_mutation_source())
    }

    pub fn get_all_data_source(&self, sources: &mut Vec<(u32, Box<DataSourcePlan>)>) {
        dispatch_plan_ref!(self, v => v.get_all_data_source(sources))
    }

    pub fn set_pruning_stats(&mut self, stats: &mut HashMap<u32, PartStatistics>) {
        dispatch_plan_mut!(self, v => v.set_pruning_stats(stats))
    }

    pub fn is_distributed_plan(&self) -> bool {
        dispatch_plan_ref!(self, v => v.is_distributed_plan())
    }

    pub fn is_warehouse_distributed_plan(&self) -> bool {
        dispatch_plan_ref!(self, v => v.is_warehouse_distributed_plan())
    }

    pub fn display_in_profile(&self) -> bool {
        dispatch_plan_ref!(self, v => v.display_in_profile())
    }

    pub fn get_desc(&self) -> Result<String> {
        dispatch_plan_ref!(self, v => v.get_desc())
    }

    pub fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        dispatch_plan_ref!(self, v => v.get_labels())
    }

    pub fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        dispatch_plan_ref!(self, v => v.derive(children))
    }

    pub fn build_pipeline(&self, builder: &mut PipelineBuilder) -> Result<()> {
        dispatch_plan_ref!(self, v => v.build_pipeline(builder))
    }

    pub fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        dispatch_plan_ref!(self, v => v.build_pipeline2(builder))
    }

    #[recursive::recursive]
    pub fn derive_with(&self, handle: &mut Box<dyn DeriveHandle>) -> PhysicalPlan {
        let mut children = vec![];
        for child in self.children() {
            children.push(child.derive_with(handle));
        }

        match handle.derive(self, children) {
            Ok(v) => v,
            Err(children) => self.derive(children),
        }
    }

    #[recursive::recursive]
    pub fn visit(&self, visitor: &mut Box<dyn PhysicalPlanVisitor>) -> Result<()> {
        for child in self.children() {
            child.visit(visitor)?;
        }

        visitor.visit(self)
    }

    pub fn format(
        &self,
        metadata: &Metadata,
        profs: HashMap<u32, PlanProfile>,
    ) -> Result<FormatTreeNode<String>> {
        let mut context = FormatContext {
            profs,
            metadata,
            scan_id_to_runtime_filters: HashMap::new(),
            runtime_filter_reports: HashMap::new(),
        };

        self.formatter()?.format(&mut context)
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::backtrace::Backtrace;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use serde::ser::SerializeStruct;
    use serde_json::json;
    use serde_json::Value;
    use serde_json::{self};

    use super::*;
    use crate::physical_plans::physical_limit::Limit;
    use crate::physical_plans::physical_sequence::Sequence;
    use crate::servers::flight::v1::packets::packet_fragment::SerializedPhysicalPlanRef;

    static STACK_DEPTH: AtomicUsize = AtomicUsize::new(0);
    static STACK_DELTA: AtomicUsize = AtomicUsize::new(0);
    static BASELINE_DEPTH: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone, Debug, serde::Deserialize)]
    pub(crate) struct StackDepthPlan {
        meta: PhysicalPlanMeta,
    }

    impl StackDepthPlan {
        fn new(name: impl Into<String>) -> Self {
            StackDepthPlan {
                meta: PhysicalPlanMeta::new(name),
            }
        }
    }

    impl serde::Serialize for StackDepthPlan {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where S: serde::Serializer {
            let depth = current_stack_depth();
            STACK_DEPTH.store(depth, Ordering::Relaxed);
            let baseline = BASELINE_DEPTH.load(Ordering::Relaxed);
            STACK_DELTA.store(depth.saturating_sub(baseline), Ordering::Relaxed);

            // Serialize in the same shape as a normal plan node.
            let mut state = serializer.serialize_struct("StackDepthPlan", 1)?;
            state.serialize_field("meta", &self.meta)?;
            state.end()
        }
    }

    impl IPhysicalPlan for StackDepthPlan {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get_meta(&self) -> &PhysicalPlanMeta {
            &self.meta
        }

        fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
            &mut self.meta
        }

        fn derive(&self, _children: Vec<PhysicalPlan>) -> PhysicalPlan {
            PhysicalPlan::new(self.clone())
        }

        fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
            let _ = builder;
            Ok(())
        }
    }

    // Used to compare the serialization stack depth of different versions
    #[test]
    fn typetag_serialize_stack_depth_is_measured() {
        STACK_DEPTH.store(0, Ordering::Relaxed);
        STACK_DELTA.store(0, Ordering::Relaxed);
        let baseline = current_stack_depth();
        BASELINE_DEPTH.store(baseline, Ordering::Relaxed);

        let plan = PhysicalPlan::new(StackDepthPlan::new("stack_depth_plan"));
        serde_json::to_vec(&plan).expect("serialize typetag plan");

        let depth = STACK_DEPTH.load(Ordering::Relaxed);
        let delta = STACK_DELTA.load(Ordering::Relaxed);

        assert!(depth > 0, "backtrace depth was not captured");
        assert!(
            delta > 0,
            "delta between typetag serialize and baseline should be > 0"
        );
        eprintln!(
            "typetag serialize stack depth: {}, delta from baseline: {}",
            depth, delta
        );
    }

    fn current_stack_depth() -> usize {
        Backtrace::force_capture()
            .to_string()
            .lines()
            .filter(|line| {
                line.trim_start()
                    .chars()
                    .next()
                    .map_or(false, |c| c.is_ascii_digit())
            })
            .count()
    }

    #[test]
    fn legacy_typetag_format_can_be_deserialized() {
        let samples: Vec<(PhysicalPlan, &'static str, fn(&PhysicalPlan) -> bool)> = vec![
            (sample_serialized_ref(), "SerializedPhysicalPlanRef", |p| {
                SerializedPhysicalPlanRef::from_physical_plan(p).is_some()
            }),
            (sample_limit(), "Limit", |p| {
                Limit::from_physical_plan(p).is_some()
            }),
            (sample_sequence(), "Sequence", |p| {
                Sequence::from_physical_plan(p).is_some()
            }),
        ];

        for (plan, name, matcher) in samples {
            let new_json = serde_json::to_value(&plan).expect("serialize new format");
            let legacy_json = convert_to_legacy(&new_json);

            let restored: PhysicalPlan =
                serde_json::from_value(legacy_json).expect("legacy typetag deserialize");
            assert!(
                matcher(&restored),
                "legacy typetag should deserialize into {name}"
            );
        }
    }

    fn sample_serialized_ref() -> PhysicalPlan {
        serde_json::from_value(serialized_ref_json()).expect("build SerializedPhysicalPlanRef")
    }

    fn sample_limit() -> PhysicalPlan {
        serde_json::from_value(limit_json()).expect("build Limit plan")
    }

    fn sample_sequence() -> PhysicalPlan {
        serde_json::from_value(sequence_json()).expect("build Sequence plan")
    }

    fn serialized_ref_json() -> Value {
        json!({ "SerializedPhysicalPlanRef": 1 })
    }

    fn limit_json() -> Value {
        json!({
            "Limit": {
                "meta": { "plan_id": 7, "name": "Limit" },
                "input": serialized_ref_json(),
                "limit": 5,
                "offset": 2,
                "stat_info": null
            }
        })
    }

    fn sequence_json() -> Value {
        json!({
            "Sequence": {
                "plan_id": 99,
                "stat_info": null,
                "left": limit_json(),
                "right": serialized_ref_json(),
                "meta": { "plan_id": 8, "name": "Sequence" }
            }
        })
    }

    fn convert_to_legacy(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                if map.len() == 1 {
                    if let Some((variant, inner)) = map.iter().next() {
                        if let Value::Object(inner_obj) = inner {
                            let mut legacy = serde_json::Map::new();
                            legacy.insert("type".to_string(), Value::String(variant.clone()));
                            for (k, v) in inner_obj {
                                legacy.insert(k.clone(), convert_to_legacy(v));
                            }
                            return Value::Object(legacy);
                        }
                    }
                }

                let mut new_map = serde_json::Map::new();
                for (k, v) in map {
                    new_map.insert(k.clone(), convert_to_legacy(v));
                }
                Value::Object(new_map)
            }
            Value::Array(arr) => Value::Array(arr.iter().map(convert_to_legacy).collect()),
            _ => value.clone(),
        }
    }
}
