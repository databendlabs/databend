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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::InternalColumn;
use common_catalog::plan::Partitions;
use common_catalog::plan::Projection;
use common_catalog::plan::StageTableInfo;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::BlockThresholds;
use common_expression::Column;
use common_expression::ColumnId;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_expression::Scalar;
use common_expression::TableSchemaRef;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::BUILTIN_FUNCTIONS;
use common_meta_app::schema::CatalogInfo;
use common_meta_app::schema::TableInfo;
use common_storage::StageFileInfo;
use enum_as_inner::EnumAsInner;
use storages_common_table_meta::meta::BlockSlotDescription;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::TableSnapshot;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::RangeJoinCondition;
use crate::optimizer::ColumnSet;
use crate::plans::CopyIntoTableMode;
use crate::plans::JoinType;
use crate::plans::RuntimeFilterId;
use crate::plans::ValidationMode;
use crate::plans::WindowFuncFrame;
use crate::ColumnBinding;
use crate::IndexType;
use crate::MetadataRef;

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
    pub fn output_fields(
        schema: TableSchemaRef,
        name_mapping: &BTreeMap<String, IndexType>,
    ) -> Result<Vec<DataField>> {
        let mut fields = Vec::with_capacity(name_mapping.len());
        let mut name_and_ids = name_mapping
            .iter()
            .map(|(name, id)| {
                let index = schema.index_of(name)?;
                Ok((name, id, index))
            })
            .collect::<Result<Vec<_>>>()?;
        // Make the order of output fields the same as their indexes in te table schema.
        name_and_ids.sort_by_key(|(_, _, index)| *index);

        for (name, id, _) in name_and_ids {
            let orig_field = schema.field_with_name(name)?;
            let data_type = DataType::from(orig_field.data_type());
            fields.push(DataField::new(&id.to_string(), data_type));
        }
        Ok(fields)
    }

    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let fields = TableScan::output_fields(self.source.schema(), &self.name_mapping)?;
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CteScan {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,
    pub cte_idx: (IndexType, IndexType),
    pub output_schema: DataSchemaRef,
    pub offsets: Vec<IndexType>,
}

impl CteScan {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MaterializedCte {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub cte_idx: IndexType,
    pub left_output_columns: Vec<ColumnBinding>,
}

impl MaterializedCte {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let fields = self.right.output_schema()?.fields().clone();
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ConstantTableScan {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,
    pub values: Vec<Column>,
    pub num_rows: usize,
    pub output_schema: DataSchemaRef,
}

impl ConstantTableScan {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Filter {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,
    pub projections: ColumnSet,

    pub input: Box<PhysicalPlan>,

    // Assumption: expression's data type must be `DataType::Boolean`.
    pub predicates: Vec<RemoteExpr>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Filter {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::with_capacity(self.projections.len());
        for (i, field) in input_schema.fields().iter().enumerate() {
            if self.projections.contains(&i) {
                fields.push(field.clone());
            }
        }
        Ok(DataSchemaRefExt::create(fields))
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
    pub projections: ColumnSet,

    pub input: Box<PhysicalPlan>,
    pub exprs: Vec<(RemoteExpr, IndexType)>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl EvalScalar {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        if self.exprs.is_empty() {
            return self.input.output_schema();
        }
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::with_capacity(self.projections.len());
        for (i, field) in input_schema.fields().iter().enumerate() {
            if self.projections.contains(&i) {
                fields.push(field.clone());
            }
        }
        let input_column_nums = input_schema.num_fields();
        for (i, (expr, index)) in self.exprs.iter().enumerate() {
            let i = i + input_column_nums;
            if !self.projections.contains(&i) {
                continue;
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
    pub projections: ColumnSet,

    pub input: Box<PhysicalPlan>,
    pub srf_exprs: Vec<(RemoteExpr, IndexType)>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl ProjectSet {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::with_capacity(input_schema.num_fields() + self.srf_exprs.len());
        for (i, field) in input_schema.fields().iter().enumerate() {
            if self.projections.contains(&i) {
                fields.push(field.clone());
            }
        }
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
                false,
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
            let data_type = agg.sig.return_type()?;
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
    PercentRank,
    LagLead(LagLeadFunctionDesc),
    NthValue(NthValueFunctionDesc),
    Ntile(NtileFunctionDesc),
    CumeDist,
}

impl WindowFunction {
    fn data_type(&self) -> Result<DataType> {
        match self {
            WindowFunction::Aggregate(agg) => agg.sig.return_type(),
            WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
                Ok(DataType::Number(NumberDataType::UInt64))
            }
            WindowFunction::PercentRank | WindowFunction::CumeDist => {
                Ok(DataType::Number(NumberDataType::Float64))
            }
            WindowFunction::LagLead(f) => Ok(f.return_type.clone()),
            WindowFunction::NthValue(f) => Ok(f.return_type.clone()),
            WindowFunction::Ntile(f) => Ok(f.return_type.clone()),
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
            WindowFunction::PercentRank => write!(f, "percent_rank"),
            WindowFunction::LagLead(lag_lead) if lag_lead.is_lag => write!(f, "lag"),
            WindowFunction::LagLead(_) => write!(f, "lead"),
            WindowFunction::NthValue(_) => write!(f, "nth_value"),
            WindowFunction::Ntile(_) => write!(f, "ntile"),
            WindowFunction::CumeDist => write!(f, "cume_dist"),
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
            self.func.data_type()?,
        ));
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LambdaFunctionDesc {
    pub func_name: String,
    pub output_column: IndexType,
    pub arg_indices: Vec<IndexType>,
    pub arg_exprs: Vec<String>,
    pub params: Vec<String>,
    pub lambda_expr: RemoteExpr,
    pub data_type: Box<DataType>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Lambda {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub lambda_funcs: Vec<LambdaFunctionDesc>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Lambda {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = input_schema.fields().clone();
        for lambda_func in self.lambda_funcs.iter() {
            let name = lambda_func.output_column.to_string();
            let data_type = lambda_func.data_type.clone();
            fields.push(DataField::new(&name, *data_type));
        }
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

    // If the sort plan is after the exchange plan
    pub after_exchange: bool,
    pub pre_projection: Option<Vec<IndexType>>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Sort {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        if let Some(proj) = &self.pre_projection {
            let fields = proj
                .iter()
                .filter_map(|index| input_schema.field_with_name(&index.to_string()).ok())
                .cloned()
                .collect::<Vec<_>>();
            if fields.len() < input_schema.fields().len() {
                // Only if the projection is not a full projection, we need to add a projection transform.
                return Ok(DataSchemaRefExt::create(fields));
            }
        }
        Ok(input_schema)
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
pub struct RowFetch {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,

    // cloned from `input`.
    pub source: Box<DataSourcePlan>,
    // projection on the source table schema.
    pub cols_to_fetch: Projection,

    pub row_id_col_offset: usize,

    pub fetched_fields: Vec<DataField>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl RowFetch {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = self.input.output_schema()?.fields().clone();
        fields.extend_from_slice(&self.fetched_fields);
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HashJoin {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,
    // After building the probe key and build key, we apply probe_projections to probe_datablock
    // and build_projections to build_datablock, which can help us reduce memory usage and calls
    // of expensive functions (take_compacted_indices and gather), after processing other_conditions,
    // we will use projections for final column elimination.
    pub projections: ColumnSet,
    pub probe_projections: ColumnSet,
    pub build_projections: ColumnSet,

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
        let build_schema = match self.join_type {
            JoinType::Left | JoinType::LeftSingle | JoinType::Full => {
                let build_schema = self.build.output_schema()?;
                // Wrap nullable type for columns in build side.
                let build_schema = DataSchemaRefExt::create(
                    build_schema
                        .fields()
                        .iter()
                        .map(|field| {
                            DataField::new(field.name(), field.data_type().wrap_nullable())
                        })
                        .collect::<Vec<_>>(),
                );
                build_schema
            }
            _ => self.build.output_schema()?,
        };

        let probe_schema = match self.join_type {
            JoinType::Right | JoinType::RightSingle | JoinType::Full => {
                let probe_schema = self.probe.output_schema()?;
                // Wrap nullable type for columns in probe side.
                let probe_schema = DataSchemaRefExt::create(
                    probe_schema
                        .fields()
                        .iter()
                        .map(|field| {
                            DataField::new(field.name(), field.data_type().wrap_nullable())
                        })
                        .collect::<Vec<_>>(),
                );
                probe_schema
            }
            _ => self.probe.output_schema()?,
        };

        let mut probe_fields = Vec::with_capacity(self.probe_projections.len());
        let mut build_fields = Vec::with_capacity(self.build_projections.len());
        for (i, field) in probe_schema.fields().iter().enumerate() {
            if self.probe_projections.contains(&i) {
                probe_fields.push(field.clone());
            }
        }
        for (i, field) in build_schema.fields().iter().enumerate() {
            if self.build_projections.contains(&i) {
                build_fields.push(field.clone());
            }
        }

        let merged_fields = match self.join_type {
            JoinType::Cross
            | JoinType::Inner
            | JoinType::Left
            | JoinType::LeftSingle
            | JoinType::Right
            | JoinType::RightSingle
            | JoinType::Full => {
                probe_fields.extend(build_fields);
                probe_fields
            }
            JoinType::LeftSemi | JoinType::LeftAnti => probe_fields,
            JoinType::RightSemi | JoinType::RightAnti => build_fields,
            JoinType::LeftMark => {
                let name = if let Some(idx) = self.marker_index {
                    idx.to_string()
                } else {
                    "marker".to_string()
                };
                build_fields.push(DataField::new(
                    name.as_str(),
                    DataType::Nullable(Box::new(DataType::Boolean)),
                ));
                build_fields
            }
            JoinType::RightMark => {
                let name = if let Some(idx) = self.marker_index {
                    idx.to_string()
                } else {
                    "marker".to_string()
                };
                probe_fields.push(DataField::new(
                    name.as_str(),
                    DataType::Nullable(Box::new(DataType::Boolean)),
                ));
                probe_fields
            }
        };
        let mut fields = Vec::with_capacity(self.projections.len());
        for (i, field) in merged_fields.iter().enumerate() {
            if self.projections.contains(&i) {
                fields.push(field.clone());
            }
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum RangeJoinType {
    IEJoin,
    Merge,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RangeJoin {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    /// The first two conditions: (>, >=, <, <=)
    /// Condition's left/right side only contains one table's column
    pub conditions: Vec<RangeJoinCondition>,
    /// The other conditions
    pub other_conditions: Vec<RemoteExpr>,
    /// Now only support inner join, will support left/right join later
    pub join_type: JoinType,
    pub range_join_type: RangeJoinType,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl RangeJoin {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = self.left.output_schema()?.fields().clone();
        fields.extend(self.right.output_schema()?.fields().clone());
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Exchange {
    /// A unique id of operator in a `PhysicalPlan` tree.
    pub plan_id: u32,

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
    /// A unique id of operator in a `PhysicalPlan` tree.
    pub plan_id: u32,

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
    /// A unique id of operator in a `PhysicalPlan` tree.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    /// Input schema of exchanged data
    pub schema: DataSchemaRef,
    pub kind: FragmentKind,
    pub keys: Vec<RemoteExpr>,

    /// Fragment ID of sink fragment
    pub destination_fragment_id: usize,
    /// Addresses of destination nodes
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
pub struct MergeIntoSource {
    // join result: target_columns, source_columns, target_table._row_id
    pub table_info: TableInfo,
    pub catalog_info: CatalogInfo,
    pub input: Box<PhysicalPlan>,
    pub row_id_idx: u32,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MergeInto {
    pub input: Box<PhysicalPlan>,
    pub table_info: TableInfo,
    pub catalog_info: CatalogInfo,
    // (DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>,Vec<usize>) => (source_schema, condition, value_exprs,projections)
    pub unmatched: Vec<(
        DataSchemaRef,
        Option<RemoteExpr>,
        Vec<RemoteExpr>,
        Vec<usize>,
    )>,
    // the first option stands for the condition
    // the second option stands for update/delete
    pub matched: Vec<(
        Option<RemoteExpr<String>>,
        Option<Vec<(FieldIndex, RemoteExpr<String>)>>,
    )>,
    pub row_id_idx: u32,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CopyIntoTable {
    pub catalog_info: CatalogInfo,
    pub required_values_schema: DataSchemaRef,
    pub values_consts: Vec<Scalar>,
    pub required_source_schema: DataSchemaRef,
    pub write_mode: CopyIntoTableMode,
    pub validation_mode: ValidationMode,
    pub force: bool,
    pub stage_table_info: StageTableInfo,
    pub files: Vec<StageFileInfo>,
    pub table_info: TableInfo,

    pub source: CopyIntoTableSource,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, EnumAsInner)]
pub enum CopyIntoTableSource {
    Query(Box<QuerySource>),
    Stage(Box<DataSourcePlan>),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct QuerySource {
    pub plan: PhysicalPlan,
    pub query_source_schema: DataSchemaRef,
    pub ignore_result: bool,
    pub result_columns: Vec<ColumnBinding>,
}

impl CopyIntoTable {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        match &self.source {
            CopyIntoTableSource::Query(query_ctx) => Ok(query_ctx.query_source_schema.clone()),
            CopyIntoTableSource::Stage(_) => Ok(self.required_values_schema.clone()),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DistributedInsertSelect {
    /// A unique id of operator in a `PhysicalPlan` tree.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub catalog_info: CatalogInfo,
    pub table_info: TableInfo,
    pub insert_schema: DataSchemaRef,
    pub select_schema: DataSchemaRef,
    pub select_column_bindings: Vec<ColumnBinding>,
    pub cast_needed: bool,
}

impl DistributedInsertSelect {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRef::default())
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
pub struct DeletePartial {
    pub parts: Partitions,
    pub filter: RemoteExpr<String>,
    pub table_info: TableInfo,
    pub catalog_info: CatalogInfo,
    pub col_indices: Vec<usize>,
    pub query_row_id_col: bool,
}

impl DeletePartial {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRef::default())
    }
}

impl MutationAggregate {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRef::default())
    }
}

// TODO(sky): make TableMutationAggregator distributed
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MutationAggregate {
    pub input: Box<PhysicalPlan>,
    pub snapshot: TableSnapshot,
    pub table_info: TableInfo,
    pub catalog_info: CatalogInfo,
    pub mutation_kind: MutationKind,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Copy)]
/// This is used by MutationAccumulator, so no compact here.
pub enum MutationKind {
    Delete,
    Update,
    Replace,
    Recluster,
    Insert,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AsyncSourcerPlan {
    pub value_data: String,
    pub schema: DataSchemaRef,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Deduplicate {
    pub input: Box<PhysicalPlan>,
    pub on_conflicts: Vec<OnConflictField>,
    pub bloom_filter_column_indexes: Vec<FieldIndex>,
    pub table_is_empty: bool,
    pub table_info: TableInfo,
    pub catalog_info: CatalogInfo,
    pub table_schema: TableSchemaRef,
    pub select_ctx: Option<SelectCtx>,
    pub table_level_range_index: HashMap<ColumnId, ColumnStatistics>,
    pub need_insert: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SelectCtx {
    pub select_column_bindings: Vec<ColumnBinding>,
    pub select_schema: DataSchemaRef,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct OnConflictField {
    pub table_field: common_expression::TableField,
    pub field_index: common_expression::FieldIndex,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReplaceInto {
    pub input: Box<PhysicalPlan>,
    pub block_thresholds: BlockThresholds,
    pub table_info: TableInfo,
    pub on_conflicts: Vec<OnConflictField>,
    pub bloom_filter_column_indexes: Vec<FieldIndex>,
    pub catalog_info: CatalogInfo,
    pub segments: Vec<(usize, Location)>,
    pub block_slots: Option<BlockSlotDescription>,
    pub need_insert: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RefreshIndex {
    pub input: Box<PhysicalPlan>,
    pub index_id: u64,
    pub table_info: TableInfo,
    pub select_schema: DataSchemaRef,
    pub select_column_bindings: Vec<ColumnBinding>,
}

impl RefreshIndex {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRefExt::create(vec![DataField::new(
            "index_loc",
            DataType::String,
        )]))
    }
}

impl Display for RefreshIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RefreshIndex")
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, EnumAsInner)]
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
    Lambda(Lambda),
    Sort(Sort),
    Limit(Limit),
    RowFetch(RowFetch),
    HashJoin(HashJoin),
    RangeJoin(RangeJoin),
    Exchange(Exchange),
    UnionAll(UnionAll),
    RuntimeFilterSource(RuntimeFilterSource),
    CteScan(CteScan),
    MaterializedCte(MaterializedCte),
    ConstantTableScan(ConstantTableScan),

    /// For insert into ... select ... in cluster
    DistributedInsertSelect(Box<DistributedInsertSelect>),
    /// Synthesized by fragmenter
    ExchangeSource(ExchangeSource),
    ExchangeSink(ExchangeSink),

    /// Delete
    DeletePartial(Box<DeletePartial>),
    MutationAggregate(Box<MutationAggregate>),
    /// Copy into table
    CopyIntoTable(Box<CopyIntoTable>),
    /// Replace
    AsyncSourcer(AsyncSourcerPlan),
    Deduplicate(Deduplicate),
    ReplaceInto(ReplaceInto),
    // MergeInto
    MergeIntoSource(MergeIntoSource),
    MergeInto(MergeInto),
}

impl PhysicalPlan {
    pub fn is_distributed_plan(&self) -> bool {
        self.children().any(|child| child.is_distributed_plan())
            || matches!(
                self,
                Self::ExchangeSource(_) | Self::ExchangeSink(_) | Self::Exchange(_)
            )
    }

    /// Get the id of the plan node
    pub fn get_id(&self) -> u32 {
        match self {
            PhysicalPlan::TableScan(v) => v.plan_id,
            PhysicalPlan::Filter(v) => v.plan_id,
            PhysicalPlan::Project(v) => v.plan_id,
            PhysicalPlan::EvalScalar(v) => v.plan_id,
            PhysicalPlan::ProjectSet(v) => v.plan_id,
            PhysicalPlan::AggregateExpand(v) => v.plan_id,
            PhysicalPlan::AggregatePartial(v) => v.plan_id,
            PhysicalPlan::AggregateFinal(v) => v.plan_id,
            PhysicalPlan::Window(v) => v.plan_id,
            PhysicalPlan::Lambda(v) => v.plan_id,
            PhysicalPlan::Sort(v) => v.plan_id,
            PhysicalPlan::Limit(v) => v.plan_id,
            PhysicalPlan::RowFetch(v) => v.plan_id,
            PhysicalPlan::HashJoin(v) => v.plan_id,
            PhysicalPlan::RangeJoin(v) => v.plan_id,
            PhysicalPlan::Exchange(v) => v.plan_id,
            PhysicalPlan::UnionAll(v) => v.plan_id,
            PhysicalPlan::RuntimeFilterSource(v) => v.plan_id,
            PhysicalPlan::DistributedInsertSelect(v) => v.plan_id,
            PhysicalPlan::ExchangeSource(v) => v.plan_id,
            PhysicalPlan::ExchangeSink(v) => v.plan_id,
            PhysicalPlan::CteScan(v) => v.plan_id,
            PhysicalPlan::MaterializedCte(v) => v.plan_id,
            PhysicalPlan::ConstantTableScan(v) => v.plan_id,
            PhysicalPlan::DeletePartial(_)
            | PhysicalPlan::MergeInto(_)
            | PhysicalPlan::MergeIntoSource(_)
            | PhysicalPlan::MutationAggregate(_)
            | PhysicalPlan::CopyIntoTable(_)
            | PhysicalPlan::AsyncSourcer(_)
            | PhysicalPlan::Deduplicate(_)
            | PhysicalPlan::ReplaceInto(_) => {
                unreachable!()
            }
        }
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
            PhysicalPlan::Lambda(plan) => plan.output_schema(),
            PhysicalPlan::Sort(plan) => plan.output_schema(),
            PhysicalPlan::Limit(plan) => plan.output_schema(),
            PhysicalPlan::RowFetch(plan) => plan.output_schema(),
            PhysicalPlan::HashJoin(plan) => plan.output_schema(),
            PhysicalPlan::Exchange(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSource(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSink(plan) => plan.output_schema(),
            PhysicalPlan::UnionAll(plan) => plan.output_schema(),
            PhysicalPlan::DistributedInsertSelect(plan) => plan.output_schema(),
            PhysicalPlan::ProjectSet(plan) => plan.output_schema(),
            PhysicalPlan::RuntimeFilterSource(plan) => plan.output_schema(),
            PhysicalPlan::DeletePartial(plan) => plan.output_schema(),
            PhysicalPlan::MutationAggregate(plan) => plan.output_schema(),
            PhysicalPlan::RangeJoin(plan) => plan.output_schema(),
            PhysicalPlan::CopyIntoTable(plan) => plan.output_schema(),
            PhysicalPlan::CteScan(plan) => plan.output_schema(),
            PhysicalPlan::MaterializedCte(plan) => plan.output_schema(),
            PhysicalPlan::ConstantTableScan(plan) => plan.output_schema(),
            PhysicalPlan::MergeIntoSource(plan) => plan.input.output_schema(),
            PhysicalPlan::AsyncSourcer(_)
            | PhysicalPlan::MergeInto(_)
            | PhysicalPlan::Deduplicate(_)
            | PhysicalPlan::ReplaceInto(_) => Ok(DataSchemaRef::default()),
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
            PhysicalPlan::Lambda(_) => "Lambda".to_string(),
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
            PhysicalPlan::RuntimeFilterSource(_) => "RuntimeFilterSource".to_string(),
            PhysicalPlan::DeletePartial(_) => "DeletePartial".to_string(),
            PhysicalPlan::MutationAggregate(_) => "MutationAggregate".to_string(),
            PhysicalPlan::RangeJoin(_) => "RangeJoin".to_string(),
            PhysicalPlan::CopyIntoTable(_) => "CopyIntoTable".to_string(),
            PhysicalPlan::AsyncSourcer(_) => "AsyncSourcer".to_string(),
            PhysicalPlan::Deduplicate(_) => "Deduplicate".to_string(),
            PhysicalPlan::ReplaceInto(_) => "Replace".to_string(),
            PhysicalPlan::MergeInto(_) => "MergeInto".to_string(),
            PhysicalPlan::MergeIntoSource(_) => "MergeIntoSource".to_string(),
            PhysicalPlan::CteScan(_) => "PhysicalCteScan".to_string(),
            PhysicalPlan::MaterializedCte(_) => "PhysicalMaterializedCte".to_string(),
            PhysicalPlan::ConstantTableScan(_) => "PhysicalConstantTableScan".to_string(),
        }
    }

    pub fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        match self {
            PhysicalPlan::TableScan(_)
            | PhysicalPlan::CteScan(_)
            | PhysicalPlan::ConstantTableScan(_) => Box::new(std::iter::empty()),
            PhysicalPlan::Filter(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Project(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::EvalScalar(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregateExpand(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregatePartial(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregateFinal(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Window(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Lambda(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Sort(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Limit(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::RowFetch(plan) => Box::new(std::iter::once(plan.input.as_ref())),
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
            PhysicalPlan::DeletePartial(_plan) => Box::new(std::iter::empty()),
            PhysicalPlan::MutationAggregate(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ProjectSet(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::RuntimeFilterSource(plan) => Box::new(
                std::iter::once(plan.left_side.as_ref())
                    .chain(std::iter::once(plan.right_side.as_ref())),
            ),
            PhysicalPlan::RangeJoin(plan) => Box::new(
                std::iter::once(plan.left.as_ref()).chain(std::iter::once(plan.right.as_ref())),
            ),
            PhysicalPlan::CopyIntoTable(_) => Box::new(std::iter::empty()),
            PhysicalPlan::AsyncSourcer(_) => Box::new(std::iter::empty()),
            PhysicalPlan::Deduplicate(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ReplaceInto(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::MergeInto(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::MergeIntoSource(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::MaterializedCte(plan) => Box::new(
                std::iter::once(plan.left.as_ref()).chain(std::iter::once(plan.right.as_ref())),
            ),
        }
    }

    /// Used to find data source info in a non-aggregation and single-table query plan.
    pub fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        match self {
            PhysicalPlan::TableScan(scan) => Some(&scan.source),
            PhysicalPlan::Filter(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Project(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::EvalScalar(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Window(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Lambda(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Sort(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Limit(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Exchange(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::ExchangeSink(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::DistributedInsertSelect(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::ProjectSet(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::RowFetch(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::RuntimeFilterSource(_)
            | PhysicalPlan::UnionAll(_)
            | PhysicalPlan::ExchangeSource(_)
            | PhysicalPlan::HashJoin(_)
            | PhysicalPlan::RangeJoin(_)
            | PhysicalPlan::MaterializedCte(_)
            | PhysicalPlan::AggregateExpand(_)
            | PhysicalPlan::AggregateFinal(_)
            | PhysicalPlan::AggregatePartial(_)
            | PhysicalPlan::DeletePartial(_)
            | PhysicalPlan::MutationAggregate(_)
            | PhysicalPlan::CopyIntoTable(_)
            | PhysicalPlan::AsyncSourcer(_)
            | PhysicalPlan::Deduplicate(_)
            | PhysicalPlan::ReplaceInto(_)
            | PhysicalPlan::MergeInto(_)
            | PhysicalPlan::MergeIntoSource(_)
            | PhysicalPlan::ConstantTableScan(_)
            | PhysicalPlan::CteScan(_) => None,
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
pub enum LagLeadDefault {
    Null,
    Index(IndexType),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LagLeadFunctionDesc {
    pub is_lag: bool,
    pub offset: u64,
    pub arg: usize,
    pub return_type: DataType,
    pub default: LagLeadDefault,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NthValueFunctionDesc {
    pub n: Option<u64>,
    pub arg: usize,
    pub return_type: DataType,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NtileFunctionDesc {
    pub n: u64,
    pub return_type: DataType,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionSignature {
    pub name: String,
    pub params: Vec<Scalar>,
    pub args: Vec<DataType>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SortDesc {
    pub asc: bool,
    pub nulls_first: bool,
    pub order_by: IndexType,
}

impl AggregateFunctionSignature {
    pub fn return_type(&self) -> Result<DataType> {
        AggregateFunctionFactory::instance()
            .get(&self.name, self.params.clone(), self.args.clone())?
            .return_type()
    }
}
