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
use std::collections::BTreeSet;

use common_datablocks::DataBlock;
use common_datavalues::wrap_nullable;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_datavalues::ToDataType;
use common_datavalues::Vu8;
use common_exception::Result;
use common_planners::ReadDataSourcePlan;

use crate::sql::plans::JoinType;

pub type ColumnID = String;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum PhysicalPlan {
    TableScan {
        name_mapping: BTreeMap<String, ColumnID>,
        source: Box<ReadDataSourcePlan>,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicates: Vec<PhysicalScalar>,
    },
    Project {
        input: Box<PhysicalPlan>,
        projections: Vec<usize>,
    },
    EvalScalar {
        input: Box<PhysicalPlan>,
        scalars: Vec<(PhysicalScalar, ColumnID)>,
    },
    AggregatePartial {
        input: Box<PhysicalPlan>,
        group_by: Vec<ColumnID>,
        agg_funcs: Vec<AggregateFunctionDesc>,
    },
    AggregateFinal {
        input: Box<PhysicalPlan>,
        group_by: Vec<ColumnID>,
        agg_funcs: Vec<AggregateFunctionDesc>,
        before_group_by_schema: DataSchemaRef,
    },
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<SortDesc>,
    },
    Limit {
        input: Box<PhysicalPlan>,
        limit: Option<usize>,
        offset: usize,
    },
    HashJoin {
        build: Box<PhysicalPlan>,
        probe: Box<PhysicalPlan>,
        build_keys: Vec<PhysicalScalar>,
        probe_keys: Vec<PhysicalScalar>,
        other_conditions: Vec<PhysicalScalar>,
        join_type: JoinType,
    },
    CrossApply {
        input: Box<PhysicalPlan>,
        subquery: Box<PhysicalPlan>,
        correlated_columns: BTreeSet<ColumnID>,
    },
    Max1Row {
        input: Box<PhysicalPlan>,
    },
}

impl PhysicalPlan {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        match self {
            PhysicalPlan::TableScan {
                name_mapping,
                source,
            } => {
                let mut fields = Vec::with_capacity(name_mapping.len());
                let schema = source.schema();
                for (name, id) in name_mapping.iter() {
                    let orig_field = schema.field_with_name(name)?;
                    fields.push(DataField::new(id.as_str(), orig_field.data_type().clone()));
                }
                Ok(DataSchemaRefExt::create(fields))
            }
            PhysicalPlan::Filter { input, .. } => input.output_schema(),
            PhysicalPlan::Project { projections, input } => {
                let input_schema = input.output_schema()?;
                let mut fields = Vec::new();
                for i in projections {
                    fields.push(input_schema.field(*i).clone());
                }
                Ok(DataSchemaRefExt::create(fields))
            }
            PhysicalPlan::EvalScalar { input, scalars } => {
                let input_schema = input.output_schema()?;
                let mut fields = input_schema.fields().clone();
                for (scalar, name) in scalars {
                    let data_type = scalar.data_type();
                    fields.push(DataField::new(name, data_type));
                }
                Ok(DataSchemaRefExt::create(fields))
            }
            PhysicalPlan::AggregatePartial {
                input,
                group_by,
                agg_funcs,
            } => {
                let input_schema = input.output_schema()?;
                let mut fields = Vec::new();
                for agg in agg_funcs {
                    fields.push(DataField::new(agg.column_id.as_str(), Vu8::to_data_type()));
                }
                if !group_by.is_empty() {
                    let sample_block = DataBlock::empty_with_schema(input_schema);
                    let method = DataBlock::choose_hash_method(&sample_block, group_by)?;
                    fields.push(DataField::new("_group_by_key", method.data_type()));
                }
                Ok(DataSchemaRefExt::create(fields))
            }
            PhysicalPlan::AggregateFinal {
                input: _,
                group_by,
                agg_funcs,
                before_group_by_schema,
            } => {
                let input_schema = before_group_by_schema;
                let mut fields = Vec::new();
                for agg in agg_funcs {
                    let data_type = agg.sig.return_type.clone();
                    fields.push(DataField::new(agg.column_id.as_str(), data_type));
                }
                for id in group_by {
                    let data_type = input_schema
                        .field_with_name(id.as_str())?
                        .data_type()
                        .clone();
                    fields.push(DataField::new(id.as_str(), data_type));
                }
                Ok(DataSchemaRefExt::create(fields))
            }
            PhysicalPlan::Sort { input, .. } => input.output_schema(),
            PhysicalPlan::Limit { input, .. } => input.output_schema(),
            PhysicalPlan::HashJoin {
                build,
                probe,
                join_type,
                ..
            } => {
                let mut fields = probe.output_schema()?.fields().clone();
                match join_type {
                    JoinType::Left => {
                        for field in build.output_schema()?.fields() {
                            fields.push(DataField::new(
                                field.name().as_str(),
                                wrap_nullable(field.data_type()),
                            ));
                        }
                    }

                    JoinType::Semi | JoinType::Anti => {
                        // Do nothing
                    }

                    _ => {
                        for field in build.output_schema()?.fields() {
                            fields.push(DataField::new(
                                field.name().as_str(),
                                field.data_type().clone(),
                            ));
                        }
                    }
                }
                Ok(DataSchemaRefExt::create(fields))
            }
            PhysicalPlan::CrossApply {
                input, subquery, ..
            } => Ok(DataSchemaRefExt::create(
                input
                    .output_schema()?
                    .fields()
                    .iter()
                    .chain(subquery.output_schema()?.fields().iter())
                    .cloned()
                    .collect(),
            )),
            PhysicalPlan::Max1Row { input, .. } => input.output_schema(),
        }
    }
}

/// Serializable and desugared representation of `Scalar`.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PhysicalScalar {
    Variable {
        column_id: ColumnID,
        data_type: DataTypeImpl,
    },
    Constant {
        value: DataValue,
        data_type: DataTypeImpl,
    },
    Function {
        name: String,
        args: Vec<(PhysicalScalar, DataTypeImpl)>,
        return_type: DataTypeImpl,
    },
    Cast {
        input: Box<PhysicalScalar>,
        target: DataTypeImpl,
    },
}

impl PhysicalScalar {
    pub fn data_type(&self) -> DataTypeImpl {
        match self {
            PhysicalScalar::Variable { data_type, .. } => data_type.clone(),
            PhysicalScalar::Constant { data_type, .. } => data_type.clone(),
            PhysicalScalar::Function { return_type, .. } => return_type.clone(),
            PhysicalScalar::Cast { target, .. } => target.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionDesc {
    pub sig: AggregateFunctionSignature,
    pub column_id: ColumnID,
    pub args: Vec<ColumnID>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionSignature {
    pub name: String,
    pub args: Vec<DataTypeImpl>,
    pub params: Vec<DataValue>,
    pub return_type: DataTypeImpl,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SortDesc {
    pub asc: bool,
    pub nulls_first: bool,
    pub order_by: ColumnID,
}
