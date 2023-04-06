// Copyright 2022 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::fmt::Display;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::InternalColumn;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_expression::Scalar;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::schema::TableInfo;

use crate::executor::explain::PlanStatsInfo;
use crate::optimizer::ColumnSet;
use crate::plans::JoinType;
use crate::plans::RuntimeFilterId;
use crate::plans::WindowFuncFrame;
use crate::ColumnBinding;
use crate::IndexType;

pub type ColumnID = String;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TableScan {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub name_mapping: BTreeMap<String, IndexType>,
    pub source: Box<DataSourcePlan>,

    /// Only used for display
    pub table_index: IndexType,
    pub stat_info: Option<PlanStatsInfo>,

    pub internal_column: Option<BTreeMap<FieldIndex, InternalColumn>>,
}

impl TableScan {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = Vec::with_capacity(self.name_mapping.len());
        let schema = self.source.schema();
        for (name, id) in self.name_mapping.iter() {
            let orig_field = schema.field_with_name(name)?;
            let data_type = DataType::from(orig_field.data_type());
            fields.push(DataField::new(&id.to_string(), data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Filter {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,

    // Assumption: expression's data type must be `DataType::Boolean`.
    pub predicates: Vec<RemoteExpr>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Filter {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Project {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub projections: Vec<usize>,

    /// Only used for display
    pub columns: ColumnSet,
    pub stat_info: Option<PlanStatsInfo>,
}

impl Project {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::new();
        for i in self.projections.iter() {
            fields.push(input_schema.field(*i).clone());
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EvalScalar {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub exprs: Vec<(RemoteExpr, IndexType)>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl EvalScalar {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = input_schema.fields().clone();
        for (expr, index) in self.exprs.iter() {
            if let RemoteExpr::ColumnRef { id, .. } = expr {
                if index == id {
                    continue;
                }
            }
            let name = index.to_string();
            let data_type = expr.as_expr(&BUILTIN_FUNCTIONS).data_type().clone();
            fields.push(DataField::new(&name, data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ProjectSet {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,

    pub srf_exprs: Vec<(RemoteExpr, IndexType)>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl ProjectSet {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = input_schema.fields().clone();
        fields.extend(self.srf_exprs.iter().map(|(srf, index)| {
            DataField::new(
                &index.to_string(),
                srf.as_expr(&BUILTIN_FUNCTIONS).data_type().clone(),
            )
        }));
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AggregateExpand {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub group_bys: Vec<IndexType>,
    pub grouping_id_index: IndexType,
    pub grouping_sets: Vec<Vec<IndexType>>,
    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl AggregateExpand {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut output_fields = input_schema.fields().clone();

        for group_by in self
            .group_bys
            .iter()
            .filter(|&index| *index != self.grouping_id_index)
        {
            // All group by columns will wrap nullable.
            let i = input_schema.index_of(&group_by.to_string())?;
            let f = &mut output_fields[i];
            *f = DataField::new(f.name(), f.data_type().wrap_nullable())
        }

        output_fields.push(DataField::new(
            &self.grouping_id_index.to_string(),
            DataType::Number(NumberDataType::UInt32),
        ));
        Ok(DataSchemaRefExt::create(output_fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AggregatePartial {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<IndexType>,
    pub agg_funcs: Vec<AggregateFunctionDesc>,
    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl AggregatePartial {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::with_capacity(self.agg_funcs.len() + self.group_by.len());
        for agg in self.agg_funcs.iter() {
            fields.push(DataField::new(
                &agg.output_column.to_string(),
                DataType::String,
            ));
        }
        if !self.group_by.is_empty() {
            let method = DataBlock::choose_hash_method_with_types(
                &self
                    .group_by
                    .iter()
                    .map(|index| {
                        Ok(input_schema
                            .field_with_name(&index.to_string())?
                            .data_type()
                            .clone())
                    })
                    .collect::<Result<Vec<_>>>()?,
            )?;
            fields.push(DataField::new("_group_by_key", method.data_type()));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AggregateFinal {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<IndexType>,
    pub agg_funcs: Vec<AggregateFunctionDesc>,
    pub before_group_by_schema: DataSchemaRef,

    pub limit: Option<usize>,
    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl AggregateFinal {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = Vec::with_capacity(self.agg_funcs.len() + self.group_by.len());
        for agg in self.agg_funcs.iter() {
            let data_type = agg.sig.return_type.clone();
            fields.push(DataField::new(&agg.output_column.to_string(), data_type));
        }
        for id in self.group_by.iter() {
            let data_type = self
                .before_group_by_schema
                .field_with_name(&id.to_string())?
                .data_type()
                .clone();
            fields.push(DataField::new(&id.to_string(), data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum WindowFunction {
    Aggregate(AggregateFunctionDesc),
    RowNumber,
    Rank,
    DenseRank,
}

impl WindowFunction {
    fn data_type(&self) -> DataType {
        match self {
            WindowFunction::Aggregate(agg) => agg.sig.return_type.clone(),
            WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
                DataType::Number(NumberDataType::UInt64)
            }
        }
    }
}

impl Display for WindowFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowFunction::Aggregate(agg) => write!(f, "{}", agg.sig.name),
            WindowFunction::RowNumber => write!(f, "row_number"),
            WindowFunction::Rank => write!(f, "rank"),
            WindowFunction::DenseRank => write!(f, "dense_rank"),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Window {
    pub plan_id: u32,
    pub index: IndexType,
    pub input: Box<PhysicalPlan>,
    pub func: WindowFunction,
    pub partition_by: Vec<IndexType>,
    pub order_by: Vec<SortDesc>,
    pub window_frame: WindowFuncFrame,
}

impl Window {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::with_capacity(input_schema.fields().len() + 1);
        fields.extend_from_slice(input_schema.fields());
        fields.push(DataField::new(
            &self.index.to_string(),
            self.func.data_type(),
        ));
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Sort {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub order_by: Vec<SortDesc>,
    // limit = Limit.limit + Limit.offset
    pub limit: Option<usize>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Sort {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Limit {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub limit: Option<usize>,
    pub offset: usize,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Limit {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HashJoin {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub build: Box<PhysicalPlan>,
    pub probe: Box<PhysicalPlan>,
    pub build_keys: Vec<RemoteExpr>,
    pub probe_keys: Vec<RemoteExpr>,
    pub non_equi_conditions: Vec<RemoteExpr>,
    pub join_type: JoinType,
    pub marker_index: Option<IndexType>,
    pub from_correlated_subquery: bool,

    // It means that join has a corresponding runtime filter
    pub contain_runtime_filter: bool,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl HashJoin {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = self.probe.output_schema()?.fields().clone();
        match self.join_type {
            JoinType::Left | JoinType::Single => {
                for field in self.build.output_schema()?.fields() {
                    fields.push(DataField::new(
                        field.name().as_str(),
                        field.data_type().wrap_nullable(),
                    ));
                }
            }
            JoinType::Right => {
                fields.clear();
                for field in self.probe.output_schema()?.fields() {
                    fields.push(DataField::new(
                        field.name().as_str(),
                        field.data_type().wrap_nullable(),
                    ));
                }
                for field in self.build.output_schema()?.fields() {
                    fields.push(DataField::new(
                        field.name().as_str(),
                        field.data_type().clone(),
                    ));
                }
            }
            JoinType::Full => {
                fields.clear();
                for field in self.probe.output_schema()?.fields() {
                    fields.push(DataField::new(
                        field.name().as_str(),
                        field.data_type().wrap_nullable(),
                    ));
                }
                for field in self.build.output_schema()?.fields() {
                    fields.push(DataField::new(
                        field.name().as_str(),
                        field.data_type().wrap_nullable(),
                    ));
                }
            }
            JoinType::LeftSemi | JoinType::LeftAnti => {
                // Do nothing
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                fields.clear();
                fields = self.build.output_schema()?.fields().clone();
            }
            JoinType::LeftMark | JoinType::RightMark => {
                fields.clear();
                let outer_table = if self.join_type == JoinType::RightMark {
                    &self.probe
                } else {
                    &self.build
                };
                fields = outer_table.output_schema()?.fields().clone();
                let name = if let Some(idx) = self.marker_index {
                    idx.to_string()
                } else {
                    "marker".to_string()
                };
                fields.push(DataField::new(
                    name.as_str(),
                    DataType::Nullable(Box::new(DataType::Boolean)),
                ));
            }

            _ => {
                for field in self.build.output_schema()?.fields() {
                    fields.push(DataField::new(
                        field.name().as_str(),
                        field.data_type().clone(),
                    ));
                }
            }
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Exchange {
    pub input: Box<PhysicalPlan>,
    pub kind: FragmentKind,
    pub keys: Vec<RemoteExpr>,
}

impl Exchange {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExchangeSource {
    /// Output schema of exchanged data
    pub schema: DataSchemaRef,

    /// Fragment ID of source fragment
    pub source_fragment_id: usize,
    pub query_id: String,
}

impl ExchangeSource {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum FragmentKind {
    // Init-partition
    Init,
    // Partitioned by hash
    Normal,
    // Broadcast
    Expansive,
    Merge,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExchangeSink {
    pub input: Box<PhysicalPlan>,
    /// Input schema of exchanged data
    pub schema: DataSchemaRef,
    pub kind: FragmentKind,
    pub keys: Vec<RemoteExpr>,

    /// Fragment ID of sink fragment
    pub destination_fragment_id: usize,
    /// Addresses of destination nodes
    pub destinations: Vec<String>,
    pub query_id: String,
}

impl ExchangeSink {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct UnionAll {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub pairs: Vec<(String, String)>,
    pub schema: DataSchemaRef,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl UnionAll {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DistributedInsertSelect {
    pub input: Box<PhysicalPlan>,
    pub catalog: String,
    pub table_info: TableInfo,
    pub insert_schema: DataSchemaRef,
    pub select_schema: DataSchemaRef,
    pub select_column_bindings: Vec<ColumnBinding>,
    pub cast_needed: bool,
}

impl DistributedInsertSelect {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRefExt::create(vec![
            DataField::new("seg_loc", DataType::String),
            DataField::new("seg_info", DataType::String),
        ]))
    }
}

// Build runtime predicate data from join build side
// Then pass it to runtime filter on join probe side
// It's the children of join node
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RuntimeFilterSource {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub left_side: Box<PhysicalPlan>,
    pub right_side: Box<PhysicalPlan>,
    pub left_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
    pub right_runtime_filters: BTreeMap<RuntimeFilterId, RemoteExpr>,
}

impl RuntimeFilterSource {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.left_side.output_schema()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum PhysicalPlan {
    TableScan(TableScan),
    Filter(Filter),
    Project(Project),
    EvalScalar(EvalScalar),
    ProjectSet(ProjectSet),
    AggregateExpand(AggregateExpand),
    AggregatePartial(AggregatePartial),
    AggregateFinal(AggregateFinal),
    Window(Window),
    Sort(Sort),
    Limit(Limit),
    HashJoin(HashJoin),
    Exchange(Exchange),
    UnionAll(UnionAll),
    RuntimeFilterSource(RuntimeFilterSource),

    /// For insert into ... select ... in cluster
    DistributedInsertSelect(Box<DistributedInsertSelect>),

    /// Synthesized by fragmenter
    ExchangeSource(ExchangeSource),
    ExchangeSink(ExchangeSink),
}

impl PhysicalPlan {
    pub fn is_distributed_plan(&self) -> bool {
        self.children().any(|child| child.is_distributed_plan())
            || matches!(
                self,
                Self::ExchangeSource(_) | Self::ExchangeSink(_) | Self::Exchange(_)
            )
    }

    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        match self {
            PhysicalPlan::TableScan(plan) => plan.output_schema(),
            PhysicalPlan::Filter(plan) => plan.output_schema(),
            PhysicalPlan::Project(plan) => plan.output_schema(),
            PhysicalPlan::EvalScalar(plan) => plan.output_schema(),
            PhysicalPlan::AggregateExpand(plan) => plan.output_schema(),
            PhysicalPlan::AggregatePartial(plan) => plan.output_schema(),
            PhysicalPlan::AggregateFinal(plan) => plan.output_schema(),
            PhysicalPlan::Window(plan) => plan.output_schema(),
            PhysicalPlan::Sort(plan) => plan.output_schema(),
            PhysicalPlan::Limit(plan) => plan.output_schema(),
            PhysicalPlan::HashJoin(plan) => plan.output_schema(),
            PhysicalPlan::Exchange(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSource(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSink(plan) => plan.output_schema(),
            PhysicalPlan::UnionAll(plan) => plan.output_schema(),
            PhysicalPlan::DistributedInsertSelect(plan) => plan.output_schema(),
            PhysicalPlan::ProjectSet(plan) => plan.output_schema(),
            PhysicalPlan::RuntimeFilterSource(plan) => plan.output_schema(),
        }
    }

    pub fn name(&self) -> String {
        match self {
            PhysicalPlan::TableScan(_) => "TableScan".to_string(),
            PhysicalPlan::Filter(_) => "Filter".to_string(),
            PhysicalPlan::Project(_) => "Project".to_string(),
            PhysicalPlan::EvalScalar(_) => "EvalScalar".to_string(),
            PhysicalPlan::AggregateExpand(_) => "AggregateExpand".to_string(),
            PhysicalPlan::AggregatePartial(_) => "AggregatePartial".to_string(),
            PhysicalPlan::AggregateFinal(_) => "AggregateFinal".to_string(),
            PhysicalPlan::Window(_) => "Window".to_string(),
            PhysicalPlan::Sort(_) => "Sort".to_string(),
            PhysicalPlan::Limit(_) => "Limit".to_string(),
            PhysicalPlan::HashJoin(_) => "HashJoin".to_string(),
            PhysicalPlan::Exchange(_) => "Exchange".to_string(),
            PhysicalPlan::UnionAll(_) => "UnionAll".to_string(),
            PhysicalPlan::DistributedInsertSelect(_) => "DistributedInsertSelect".to_string(),
            PhysicalPlan::ExchangeSource(_) => "Exchange Source".to_string(),
            PhysicalPlan::ExchangeSink(_) => "Exchange Sink".to_string(),
            PhysicalPlan::ProjectSet(_) => "Unnest".to_string(),
            PhysicalPlan::RuntimeFilterSource(_) => "RuntimeFilterSource".to_string(),
        }
    }

    pub fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        match self {
            PhysicalPlan::TableScan(_) => Box::new(std::iter::empty()),
            PhysicalPlan::Filter(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Project(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::EvalScalar(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregateExpand(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregatePartial(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregateFinal(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Window(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Sort(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Limit(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::HashJoin(plan) => Box::new(
                std::iter::once(plan.probe.as_ref()).chain(std::iter::once(plan.build.as_ref())),
            ),
            PhysicalPlan::Exchange(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ExchangeSource(_) => Box::new(std::iter::empty()),
            PhysicalPlan::ExchangeSink(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::UnionAll(plan) => Box::new(
                std::iter::once(plan.left.as_ref()).chain(std::iter::once(plan.right.as_ref())),
            ),
            PhysicalPlan::DistributedInsertSelect(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::ProjectSet(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::RuntimeFilterSource(plan) => Box::new(
                std::iter::once(plan.left_side.as_ref())
                    .chain(std::iter::once(plan.right_side.as_ref())),
            ),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionDesc {
    pub sig: AggregateFunctionSignature,
    pub output_column: IndexType,
    pub args: Vec<usize>,
    pub arg_indices: Vec<IndexType>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionSignature {
    pub name: String,
    pub args: Vec<DataType>,
    pub params: Vec<Scalar>,
    pub return_type: DataType,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SortDesc {
    pub asc: bool,
    pub nulls_first: bool,
    pub order_by: IndexType,
}
