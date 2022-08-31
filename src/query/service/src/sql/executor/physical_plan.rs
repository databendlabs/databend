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

use common_datablocks::DataBlock;
use common_datavalues::wrap_nullable;
use common_datavalues::BooleanType;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::NullableType;
use common_datavalues::ToDataType;
use common_datavalues::Vu8;
use common_exception::Result;
use common_planners::ReadDataSourcePlan;
use common_planners::StageKind;

use super::physical_scalar::PhysicalScalar;
use super::AggregateFunctionDesc;
use super::SortDesc;
use crate::sql::optimizer::ColumnSet;
use crate::sql::plans::JoinType;
use crate::sql::IndexType;

pub type ColumnID = String;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TableScan {
    pub name_mapping: BTreeMap<String, ColumnID>,
    pub source: Box<ReadDataSourcePlan>,

    /// Only used for display
    pub table_index: IndexType,
}

impl TableScan {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = Vec::with_capacity(self.name_mapping.len());
        let schema = self.source.schema();
        for (name, id) in self.name_mapping.iter() {
            let orig_field = schema.field_with_name(name)?;
            fields.push(DataField::new(id.as_str(), orig_field.data_type().clone()));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Filter {
    pub input: Box<PhysicalPlan>,
    pub predicates: Vec<PhysicalScalar>,
}

impl Filter {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Project {
    pub input: Box<PhysicalPlan>,
    pub projections: Vec<usize>,

    /// Only used for display
    pub columns: ColumnSet,
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
    pub input: Box<PhysicalPlan>,
    pub scalars: Vec<(PhysicalScalar, ColumnID)>,
}

impl EvalScalar {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = input_schema.fields().clone();
        for (scalar, name) in self.scalars.iter() {
            let data_type = scalar.data_type();
            fields.push(DataField::new(name, data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AggregatePartial {
    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<ColumnID>,
    pub agg_funcs: Vec<AggregateFunctionDesc>,
}

impl AggregatePartial {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::with_capacity(self.agg_funcs.len() + self.group_by.len());
        for agg in self.agg_funcs.iter() {
            fields.push(DataField::new(agg.column_id.as_str(), Vu8::to_data_type()));
        }
        if !self.group_by.is_empty() {
            let sample_block = DataBlock::empty_with_schema(input_schema);
            let method = DataBlock::choose_hash_method(&sample_block, &self.group_by)?;
            fields.push(DataField::new("_group_by_key", method.data_type()));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AggregateFinal {
    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<ColumnID>,
    pub agg_funcs: Vec<AggregateFunctionDesc>,
    pub before_group_by_schema: DataSchemaRef,
}

impl AggregateFinal {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = Vec::with_capacity(self.agg_funcs.len() + self.group_by.len());
        for agg in self.agg_funcs.iter() {
            let data_type = agg.sig.return_type.clone();
            fields.push(DataField::new(agg.column_id.as_str(), data_type));
        }
        for id in self.group_by.iter() {
            let data_type = self
                .before_group_by_schema
                .field_with_name(id.as_str())?
                .data_type()
                .clone();
            fields.push(DataField::new(id.as_str(), data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Sort {
    pub input: Box<PhysicalPlan>,
    pub order_by: Vec<SortDesc>,
}

impl Sort {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Limit {
    pub input: Box<PhysicalPlan>,
    pub limit: Option<usize>,
    pub offset: usize,
}

impl Limit {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HashJoin {
    pub build: Box<PhysicalPlan>,
    pub probe: Box<PhysicalPlan>,
    pub build_keys: Vec<PhysicalScalar>,
    pub probe_keys: Vec<PhysicalScalar>,
    pub other_conditions: Vec<PhysicalScalar>,
    pub join_type: JoinType,
    pub marker_index: Option<IndexType>,
    pub from_correlated_subquery: bool,
}

impl HashJoin {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = self.probe.output_schema()?.fields().clone();
        match self.join_type {
            JoinType::Left => {
                for field in self.build.output_schema()?.fields() {
                    fields.push(DataField::new(
                        field.name().as_str(),
                        wrap_nullable(field.data_type()),
                    ));
                }
            }

            JoinType::Semi | JoinType::Anti => {
                // Do nothing
            }

            JoinType::Mark => {
                fields.clear();
                fields = self.build.output_schema()?.fields().clone();
                let name = if let Some(idx) = self.marker_index {
                    idx.to_string()
                } else {
                    "marker".to_string()
                };
                fields.push(DataField::new(
                    name.as_str(),
                    NullableType::new_impl(BooleanType::new_impl()),
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
    pub kind: StageKind,
    pub keys: Vec<PhysicalScalar>,
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExchangeSink {
    pub input: Box<PhysicalPlan>,
    /// Input schema of exchanged data
    pub schema: DataSchemaRef,
    pub kind: StageKind,
    pub keys: Vec<PhysicalScalar>,

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
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub schema: DataSchemaRef,
}

impl UnionAll {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum PhysicalPlan {
    TableScan(TableScan),
    Filter(Filter),
    Project(Project),
    EvalScalar(EvalScalar),
    AggregatePartial(AggregatePartial),
    AggregateFinal(AggregateFinal),
    Sort(Sort),
    Limit(Limit),
    HashJoin(HashJoin),
    Exchange(Exchange),
    UnionAll(UnionAll),

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
            PhysicalPlan::AggregatePartial(plan) => plan.output_schema(),
            PhysicalPlan::AggregateFinal(plan) => plan.output_schema(),
            PhysicalPlan::Sort(plan) => plan.output_schema(),
            PhysicalPlan::Limit(plan) => plan.output_schema(),
            PhysicalPlan::HashJoin(plan) => plan.output_schema(),
            PhysicalPlan::Exchange(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSource(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSink(plan) => plan.output_schema(),
            PhysicalPlan::UnionAll(plan) => plan.output_schema(),
        }
    }

    pub fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        match self {
            PhysicalPlan::TableScan(_) => Box::new(std::iter::empty()),
            PhysicalPlan::Filter(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Project(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::EvalScalar(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregatePartial(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregateFinal(plan) => Box::new(std::iter::once(plan.input.as_ref())),
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
        }
    }
}
