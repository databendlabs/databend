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

use common_exception::ErrorCode;
use common_exception::Result;
use itertools::Itertools;

use crate::sql::exec::AggregateFunctionDesc;
use crate::sql::exec::AggregateFunctionSignature;
use crate::sql::exec::ColumnID;
use crate::sql::exec::PhysicalPlan;
use crate::sql::exec::PhysicalScalar;
use crate::sql::exec::SortDesc;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::RelOperator;
use crate::sql::plans::Scalar;
use crate::sql::MetadataRef;
use crate::sql::ScalarExpr;

pub struct PhysicalPlanBuilder {
    metadata: MetadataRef,
}

impl PhysicalPlanBuilder {
    pub fn new(metadata: MetadataRef) -> Self {
        Self { metadata }
    }

    pub fn build(&self, s_expr: &SExpr) -> Result<PhysicalPlan> {
        match s_expr.plan() {
            RelOperator::PhysicalScan(scan) => {
                let mut name_mapping = BTreeMap::new();
                let metadata = self.metadata.read();
                for index in scan.columns.iter() {
                    let name = metadata.column(*index).name.clone();
                    name_mapping.insert(name, index.to_string());
                }
                Ok(PhysicalPlan::TableScan {
                    name_mapping,
                    source: Box::new(metadata.table(scan.table_index).source.clone()),
                })
            }
            RelOperator::PhysicalHashJoin(join) => {
                let build_side = self.build(s_expr.child(1)?)?;
                let probe_side = self.build(s_expr.child(0)?)?;
                Ok(PhysicalPlan::HashJoin {
                    build: Box::new(build_side),
                    probe: Box::new(probe_side),
                    join_type: join.join_type.clone(),
                    build_keys: join
                        .build_keys
                        .iter()
                        .map(|v| {
                            let mut builder = PhysicalScalarBuilder;
                            builder.build(v)
                        })
                        .collect::<Result<_>>()?,
                    probe_keys: join
                        .probe_keys
                        .iter()
                        .map(|v| {
                            let mut builder = PhysicalScalarBuilder;
                            builder.build(v)
                        })
                        .collect::<Result<_>>()?,
                    other_conditions: join
                        .other_conditions
                        .iter()
                        .map(|v| {
                            let mut builder = PhysicalScalarBuilder;
                            builder.build(v)
                        })
                        .collect::<Result<_>>()?,
                })
            }
            RelOperator::Project(project) => {
                let input = self.build(s_expr.child(0)?)?;
                let input_schema = input.output_schema()?;
                Ok(PhysicalPlan::Project {
                    input: Box::new(input),
                    projections: project
                        .columns
                        .iter()
                        .sorted()
                        .map(|index| input_schema.index_of(index.to_string().as_str()))
                        .collect::<Result<_>>()?,
                })
            }
            RelOperator::EvalScalar(eval_scalar) => Ok(PhysicalPlan::EvalScalar {
                input: Box::new(self.build(s_expr.child(0)?)?),
                scalars: eval_scalar
                    .items
                    .iter()
                    .map(|item| {
                        let mut builder = PhysicalScalarBuilder;
                        Ok((builder.build(&item.scalar)?, item.index.to_string()))
                    })
                    .collect::<Result<_>>()?,
            }),

            RelOperator::Filter(filter) => Ok(PhysicalPlan::Filter {
                input: Box::new(self.build(s_expr.child(0)?)?),
                predicates: filter
                    .predicates
                    .iter()
                    .map(|pred| {
                        let mut builder = PhysicalScalarBuilder;
                        builder.build(pred)
                    })
                    .collect::<Result<_>>()?,
            }),
            RelOperator::Aggregate(agg) => {
                let input = self.build(s_expr.child(0)?)?;
                let group_items: Vec<ColumnID> = agg
                    .group_items
                    .iter()
                    .map(|v| v.index.to_string())
                    .collect();
                let agg_funcs: Vec<AggregateFunctionDesc> = agg.aggregate_functions.iter().map(|v| {
                    if let Scalar::AggregateFunction(agg) = &v.scalar {
                        Ok(AggregateFunctionDesc {
                            sig: AggregateFunctionSignature {
                                name: agg.func_name.clone(),
                                args: agg.args.iter().map(|s| {
                                    s.data_type()
                                }).collect(),
                                params: agg.params.clone(),
                                return_type: agg.return_type.clone(),
                            },
                            column_id: v.index.to_string(),
                            args: agg.args.iter().map(|arg| {
                                if let Scalar::BoundColumnRef(col) = arg {
                                    Ok(col.column.index.to_string())
                                } else {
                                    Err(ErrorCode::LogicalError(
                                        "Aggregate function argument must be a BoundColumnRef".to_string()
                                    ))
                                }
                            }).collect::<Result<_>>()?,
                        })
                    } else {
                        Err(ErrorCode::LogicalError("Expected aggregate function".to_string()))
                    }
                }).collect::<Result<_>>()?;
                let before_group_by_schema = input.output_schema()?;
                let partial_agg = PhysicalPlan::AggregatePartial {
                    input: Box::new(input),
                    group_by: group_items.clone(),
                    agg_funcs: agg_funcs.clone(),
                };
                let final_agg = PhysicalPlan::AggregateFinal {
                    input: Box::new(partial_agg),
                    group_by: group_items,
                    agg_funcs,
                    before_group_by_schema,
                };

                Ok(final_agg)
            }
            RelOperator::Sort(sort) => Ok(PhysicalPlan::Sort {
                input: Box::new(self.build(s_expr.child(0)?)?),
                order_by: sort
                    .items
                    .iter()
                    .map(|v| SortDesc {
                        asc: v.asc.unwrap_or(true),
                        nulls_first: v.nulls_first.unwrap_or_default(),
                        order_by: v.index.to_string(),
                    })
                    .collect(),
            }),
            RelOperator::Limit(limit) => Ok(PhysicalPlan::Limit {
                input: Box::new(self.build(s_expr.child(0)?)?),
                limit: limit.limit,
                offset: limit.offset,
            }),
            RelOperator::CrossApply(cross_apply) => Ok(PhysicalPlan::CrossApply {
                input: Box::new(self.build(s_expr.child(0)?)?),
                subquery: Box::new(self.build(s_expr.child(1)?)?),
                correlated_columns: cross_apply
                    .correlated_columns
                    .iter()
                    .map(|v| v.to_string())
                    .collect(),
            }),
            RelOperator::Max1Row(_) => Ok(PhysicalPlan::Max1Row {
                input: Box::new(self.build(s_expr.child(0)?)?),
            }),

            _ => Err(ErrorCode::LogicalError(format!(
                "Unsupported physical plan: {:?}",
                s_expr.plan()
            ))),
        }
    }
}

pub struct PhysicalScalarBuilder;

impl PhysicalScalarBuilder {
    pub fn build(&mut self, scalar: &Scalar) -> Result<PhysicalScalar> {
        match scalar {
            Scalar::BoundColumnRef(column_ref) => Ok(PhysicalScalar::Variable {
                column_id: column_ref.column.index.to_string(),
                data_type: column_ref.data_type(),
            }),
            Scalar::ConstantExpr(constant) => Ok(PhysicalScalar::Constant {
                value: constant.value.clone(),
                data_type: constant.data_type.clone(),
            }),
            Scalar::AndExpr(and) => Ok(PhysicalScalar::Function {
                name: "and".to_string(),
                args: vec![
                    (self.build(&and.left)?, and.left.data_type()),
                    (self.build(&and.right)?, and.right.data_type()),
                ],
                return_type: and.data_type(),
            }),
            Scalar::OrExpr(or) => Ok(PhysicalScalar::Function {
                name: "or".to_string(),
                args: vec![
                    (self.build(&or.left)?, or.left.data_type()),
                    (self.build(&or.right)?, or.right.data_type()),
                ],
                return_type: or.data_type(),
            }),
            Scalar::ComparisonExpr(comp) => Ok(PhysicalScalar::Function {
                name: comp.op.to_func_name(),
                args: vec![
                    (self.build(&comp.left)?, comp.left.data_type()),
                    (self.build(&comp.right)?, comp.right.data_type()),
                ],
                return_type: comp.data_type(),
            }),
            Scalar::FunctionCall(func) => Ok(PhysicalScalar::Function {
                name: func.func_name.clone(),
                args: func
                    .arguments
                    .iter()
                    .zip(func.arg_types.iter())
                    .map(|(arg, typ)| Ok((self.build(arg)?, typ.clone())))
                    .collect::<Result<_>>()?,
                return_type: func.return_type.clone(),
            }),
            Scalar::CastExpr(cast) => Ok(PhysicalScalar::Cast {
                input: Box::new(self.build(&cast.argument)?),
                target: cast.target_type.clone(),
            }),

            _ => Err(ErrorCode::LogicalError(format!(
                "Unsupported physical scalar: {:?}",
                scalar
            ))),
        }
    }
}
