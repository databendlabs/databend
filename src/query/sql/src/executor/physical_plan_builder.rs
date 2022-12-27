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
use std::collections::HashSet;
use std::sync::Arc;

use common_catalog::catalog::CatalogManager;
use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::plan::PrewhereInfo;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check;
use common_expression::types::DataType;
use common_expression::Chunk;
use common_expression::DataSchemaRefExt;
use common_expression::RemoteExpr;
use common_expression::TableSchema;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;
use itertools::Itertools;

use super::AggregateFinal;
use super::AggregateFunctionDesc;
use super::AggregateFunctionSignature;
use super::AggregatePartial;
use super::Exchange as PhysicalExchange;
use super::Filter;
use super::HashJoin;
use super::Limit;
use super::Sort;
use super::TableScan;
use crate::executor::table_read_plan::ToReadDataSourcePlan;
use crate::executor::util::check_physical;
use crate::executor::EvalScalar;
use crate::executor::FragmentKind;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalScalar;
use crate::executor::SortDesc;
use crate::executor::UnionAll;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;
use crate::plans::AggregateMode;
use crate::plans::AndExpr;
use crate::plans::Exchange;
use crate::plans::PhysicalScan;
use crate::plans::RelOperator;
use crate::plans::Scalar;
use crate::ColumnEntry;
use crate::IndexType;
use crate::Metadata;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::DUMMY_COLUMN_INDEX;
use crate::DUMMY_TABLE_INDEX;
use crate::GROUP_BY_KEY_COLUMN_INDEX;

pub struct PhysicalPlanBuilder {
    metadata: MetadataRef,
    ctx: Arc<dyn TableContext>,
}

impl PhysicalPlanBuilder {
    pub fn new(metadata: MetadataRef, ctx: Arc<dyn TableContext>) -> Self {
        Self { metadata, ctx }
    }

    fn build_projection(
        metadata: &Metadata,
        schema: &TableSchema,
        columns: &ColumnSet,
        has_inner_column: bool,
    ) -> Projection {
        if !has_inner_column {
            let col_indices = columns
                .iter()
                .map(|index| {
                    let name = match metadata.column(*index) {
                        ColumnEntry::BaseTableColumn { column_name, .. } => column_name,
                        ColumnEntry::DerivedColumn { alias, .. } => alias,
                    };
                    schema.index_of(name).unwrap()
                })
                .sorted()
                .collect::<Vec<_>>();
            Projection::Columns(col_indices)
        } else {
            let col_indices = columns
                .iter()
                .map(|index| {
                    let column = metadata.column(*index);
                    match column {
                        ColumnEntry::BaseTableColumn {
                            column_name,
                            path_indices,
                            ..
                        } => match path_indices {
                            Some(path_indices) => (column.index(), path_indices.to_vec()),
                            None => {
                                let idx = schema.index_of(column_name).unwrap();
                                (column.index(), vec![idx])
                            }
                        },
                        ColumnEntry::DerivedColumn { alias, .. } => {
                            let idx = schema.index_of(alias).unwrap();
                            (column.index(), vec![idx])
                        }
                    }
                })
                .sorted()
                .collect::<BTreeMap<_, Vec<IndexType>>>();
            Projection::InnerColumns(col_indices)
        }
    }

    #[async_recursion::async_recursion]
    pub async fn build(&self, s_expr: &SExpr) -> Result<PhysicalPlan> {
        debug_assert!(check_physical(s_expr));

        match s_expr.plan() {
            RelOperator::PhysicalScan(scan) => {
                let mut has_inner_column = false;
                let mut name_mapping = BTreeMap::new();
                let metadata = self.metadata.read().clone();
                for index in scan.columns.iter() {
                    let column = metadata.column(*index);
                    if let ColumnEntry::BaseTableColumn { path_indices, .. } = column {
                        if path_indices.is_some() {
                            has_inner_column = true;
                        }
                    }

                    let name = match column {
                        ColumnEntry::BaseTableColumn { column_name, .. } => column_name,
                        ColumnEntry::DerivedColumn { alias, .. } => alias,
                    };
                    if let Some(prewhere) = &scan.prewhere {
                        // if there is a prewhere optimization,
                        // we can prune `PhysicalScan`'s output schema.
                        if prewhere.output_columns.contains(index) {
                            name_mapping.insert(name.to_string(), *index);
                        }
                    } else {
                        name_mapping.insert(name.to_string(), *index);
                    }
                }

                let table_entry = metadata.table(scan.table_index);
                let table = table_entry.table();
                let table_schema = table.schema();

                let push_downs = self.push_downs(scan, &table_schema, has_inner_column)?;

                let source = table
                    .read_plan_with_catalog(
                        self.ctx.clone(),
                        table_entry.catalog().to_string(),
                        Some(push_downs),
                    )
                    .await?;
                Ok(PhysicalPlan::TableScan(TableScan {
                    name_mapping,
                    source: Box::new(source),
                    table_index: scan.table_index,
                }))
            }
            RelOperator::DummyTableScan(_) => {
                let catalogs = CatalogManager::instance();
                let table = catalogs
                    .get_catalog(CATALOG_DEFAULT)?
                    .get_table(self.ctx.get_tenant().as_str(), "system", "one")
                    .await?;
                let source = table
                    .read_plan_with_catalog(self.ctx.clone(), CATALOG_DEFAULT.to_string(), None)
                    .await?;
                Ok(PhysicalPlan::TableScan(TableScan {
                    name_mapping: BTreeMap::from([("dummy".to_string(), DUMMY_COLUMN_INDEX)]),
                    source: Box::new(source),
                    table_index: DUMMY_TABLE_INDEX,
                }))
            }
            RelOperator::PhysicalHashJoin(join) => {
                let build_side = self.build(s_expr.child(1)?).await?;
                let probe_side = self.build(s_expr.child(0)?).await?;
                Ok(PhysicalPlan::HashJoin(HashJoin {
                    build: Box::new(build_side),
                    probe: Box::new(probe_side),
                    join_type: join.join_type.clone(),
                    build_keys: join
                        .build_keys
                        .iter()
                        .map(PhysicalScalarBuilder::build)
                        .collect::<Result<_>>()?,
                    probe_keys: join
                        .probe_keys
                        .iter()
                        .map(PhysicalScalarBuilder::build)
                        .collect::<Result<_>>()?,
                    non_equi_conditions: join
                        .non_equi_conditions
                        .iter()
                        .map(PhysicalScalarBuilder::build)
                        .collect::<Result<_>>()?,
                    marker_index: join.marker_index,
                    from_correlated_subquery: join.from_correlated_subquery,
                }))
            }

            RelOperator::EvalScalar(eval_scalar) => {
                let input = Box::new(self.build(s_expr.child(0)?).await?);
                Ok(PhysicalPlan::EvalScalar(EvalScalar {
                    input,
                    scalars: eval_scalar
                        .items
                        .iter()
                        .map(|item| Ok((PhysicalScalarBuilder::build(&item.scalar)?, item.index)))
                        .collect::<Result<_>>()?,
                }))
            }

            RelOperator::Filter(filter) => {
                let input = Box::new(self.build(s_expr.child(0)?).await?);
                Ok(PhysicalPlan::Filter(Filter {
                    input,
                    predicates: filter
                        .predicates
                        .iter()
                        .map(PhysicalScalarBuilder::build)
                        .collect::<Result<_>>()?,
                }))
            }
            RelOperator::Aggregate(agg) => {
                let input = self.build(s_expr.child(0)?).await?;
                let group_items = agg.group_items.iter().map(|v| v.index).collect::<Vec<_>>();
                let result = match &agg.mode {
                    AggregateMode::Partial => {
                        let agg_funcs: Vec<AggregateFunctionDesc> = agg.aggregate_functions.iter().map(|v| {
                            if let Scalar::AggregateFunction(agg) = &v.scalar {
                                Ok(AggregateFunctionDesc {
                                    sig: AggregateFunctionSignature {
                                        name: agg.func_name.clone(),
                                        args: agg.args.iter().map(|s| {
                                            s.data_type()
                                        }).collect(),
                                        params: agg.params.clone(),
                                        return_type: *agg.return_type.clone(),
                                    },
                                    output_column: v.index,
                                    args: agg.args.iter().map(|arg| {
                                        if let Scalar::BoundColumnRef(col) = arg {
                                            Ok(col.column.index)
                                        } else {
                                            Err(ErrorCode::Internal(
                                                "Aggregate function argument must be a BoundColumnRef".to_string()
                                            ))
                                        }
                                    }).collect::<Result<_>>()?,
                                })
                            } else {
                                Err(ErrorCode::Internal("Expected aggregate function".to_string()))
                            }
                        }).collect::<Result<_>>()?;

                        match input {
                            PhysicalPlan::Exchange(PhysicalExchange { input, kind, .. }) => {
                                let aggregate_partial = AggregatePartial {
                                    input,
                                    agg_funcs,
                                    group_by: group_items,
                                };

                                let group_by_key_index = GROUP_BY_KEY_COLUMN_INDEX;
                                let group_by_key_data_type = Chunk::choose_hash_method_with_types(
                                    &agg.group_items
                                        .iter()
                                        .map(|v| v.scalar.data_type())
                                        .collect::<Vec<_>>(),
                                )?
                                .data_type();

                                PhysicalPlan::Exchange(PhysicalExchange {
                                    kind,
                                    input: Box::new(PhysicalPlan::AggregatePartial(
                                        aggregate_partial,
                                    )),
                                    keys: vec![PhysicalScalar::IndexedVariable {
                                        index: group_by_key_index,
                                        data_type: group_by_key_data_type,
                                        display_name: "_group_by_key".to_string(),
                                    }],
                                })
                            }
                            _ => PhysicalPlan::AggregatePartial(AggregatePartial {
                                agg_funcs,
                                group_by: group_items,
                                input: Box::new(input),
                            }),
                        }
                    }

                    // Hack to get before group by schema, we should refactor this
                    AggregateMode::Final => {
                        let agg_funcs: Vec<AggregateFunctionDesc> = agg.aggregate_functions.iter().map(|v| {
                            if let Scalar::AggregateFunction(agg) = &v.scalar {
                                Ok(AggregateFunctionDesc {
                                    sig: AggregateFunctionSignature {
                                        name: agg.func_name.clone(),
                                        args: agg.args.iter().map(|s| {
                                            s.data_type()
                                        }).collect(),
                                        params: agg.params.clone(),
                                        return_type: *agg.return_type.clone(),
                                    },
                                    output_column: v.index,
                                    args: agg.args.iter().map(|arg| {
                                        if let Scalar::BoundColumnRef(col) = arg {
                                            Ok(col.column.index)
                                        } else {
                                            Err(ErrorCode::Internal(
                                                "Aggregate function argument must be a BoundColumnRef".to_string()
                                            ))
                                        }
                                    }).collect::<Result<_>>()?,
                                })
                            } else {
                                Err(ErrorCode::Internal("Expected aggregate function".to_string()))
                            }
                        }).collect::<Result<_>>()?;

                        match input {
                            PhysicalPlan::AggregatePartial(ref agg) => {
                                let before_group_by_schema = agg.input.output_schema()?;
                                PhysicalPlan::AggregateFinal(AggregateFinal {
                                    input: Box::new(input),
                                    group_by: group_items,
                                    agg_funcs,
                                    before_group_by_schema,
                                })
                            }

                            PhysicalPlan::Exchange(PhysicalExchange {
                                input: box PhysicalPlan::AggregatePartial(ref agg),
                                ..
                            }) => {
                                let before_group_by_schema = agg.input.output_schema()?;
                                PhysicalPlan::AggregateFinal(AggregateFinal {
                                    input: Box::new(input),
                                    group_by: group_items,
                                    agg_funcs,
                                    before_group_by_schema,
                                })
                            }

                            _ => {
                                return Err(ErrorCode::Internal(format!(
                                    "invalid input physical plan: {}",
                                    input.name(),
                                )));
                            }
                        }
                    }
                    AggregateMode::Initial => {
                        return Err(ErrorCode::Internal("Invalid aggregate mode: Initial"));
                    }
                };

                Ok(result)
            }
            RelOperator::Sort(sort) => Ok(PhysicalPlan::Sort(Sort {
                input: Box::new(self.build(s_expr.child(0)?).await?),
                order_by: sort
                    .items
                    .iter()
                    .map(|v| SortDesc {
                        asc: v.asc,
                        nulls_first: v.nulls_first,
                        order_by: v.index,
                    })
                    .collect(),
                limit: sort.limit,
            })),
            RelOperator::Limit(limit) => Ok(PhysicalPlan::Limit(Limit {
                input: Box::new(self.build(s_expr.child(0)?).await?),
                limit: limit.limit,
                offset: limit.offset,
            })),
            RelOperator::Exchange(exchange) => {
                let input = Box::new(self.build(s_expr.child(0)?).await?);
                // let input_schema = input.output_schema()?;
                let mut keys = vec![];
                let kind = match exchange {
                    Exchange::Random => FragmentKind::Init,
                    Exchange::Hash(scalars) => {
                        for scalar in scalars {
                            keys.push(PhysicalScalarBuilder::build(scalar)?);
                        }
                        FragmentKind::Normal
                    }
                    Exchange::Broadcast => FragmentKind::Expansive,
                    Exchange::Merge => FragmentKind::Merge,
                };
                Ok(PhysicalPlan::Exchange(PhysicalExchange {
                    input,
                    kind,
                    keys,
                }))
            }
            RelOperator::UnionAll(op) => {
                let left = self.build(s_expr.child(0)?).await?;
                let left_schema = left.output_schema()?;
                let pairs = op
                    .pairs
                    .iter()
                    .map(|(l, r)| (l.to_string(), r.to_string()))
                    .collect::<Vec<_>>();
                let fields = pairs
                    .iter()
                    .map(|(left, _)| Ok(left_schema.field_with_name(left)?.clone()))
                    .collect::<Result<Vec<_>>>()?;
                Ok(PhysicalPlan::UnionAll(UnionAll {
                    left: Box::new(left),
                    right: Box::new(self.build(s_expr.child(1)?).await?),
                    pairs,
                    schema: DataSchemaRefExt::create(fields),
                }))
            }
            _ => Err(ErrorCode::Internal(format!(
                "Unsupported physical plan: {:?}",
                s_expr.plan()
            ))),
        }
    }

    fn push_downs(
        &self,
        scan: &PhysicalScan,
        table_schema: &TableSchema,
        has_inner_column: bool,
    ) -> Result<PushDownInfo> {
        let metadata = self.metadata.read().clone();

        let projection =
            Self::build_projection(&metadata, table_schema, &scan.columns, has_inner_column);
        let _project_schema = projection.project_schema(table_schema);

        let push_down_filters = scan
            .push_down_predicates
            .clone()
            .map(|predicates| -> Result<Vec<RemoteExpr<String>>> {
                predicates
                    .into_iter()
                    .map(|scalar| {
                        let raw_expr = scalar.as_raw_expr();
                        let filter = check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
                        let filter = filter.project_column_ref(|index: &usize| {
                            table_schema.fields()[*index].name().clone()
                        });
                        Ok(RemoteExpr::from_expr(&filter))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;

        let prewhere_info = scan
            .prewhere
            .as_ref()
            .map(|prewhere| -> Result<PrewhereInfo> {
                let predicate = if prewhere.predicates.is_empty() {
                    None
                } else {
                    let mut scalar = prewhere.predicates[0].clone();
                    for predicate in prewhere.predicates.iter().skip(1) {
                        scalar = Scalar::AndExpr(AndExpr {
                            left: Box::new(scalar),
                            right: Box::new(predicate.clone()),
                            return_type: Box::new(DataType::Boolean),
                        });
                    }

                    Some(scalar)
                };

                assert!(
                    predicate.is_some(),
                    "There should be at least one predicate in prewhere"
                );

                let filter = predicate.unwrap().as_raw_expr();
                let filter = check(&filter, &BUILTIN_FUNCTIONS)
                    .map_err(|(_, _)| ErrorCode::Internal("Invalid expression"))?;
                let filter = RemoteExpr::from_expr(&filter);

                let remain_columns = scan
                    .columns
                    .difference(&prewhere.prewhere_columns)
                    .copied()
                    .collect::<HashSet<usize>>();

                let output_columns = Self::build_projection(
                    &metadata,
                    table_schema,
                    &prewhere.output_columns,
                    has_inner_column,
                );
                let prewhere_columns = Self::build_projection(
                    &metadata,
                    table_schema,
                    &prewhere.prewhere_columns,
                    has_inner_column,
                );
                let remain_columns = Self::build_projection(
                    &metadata,
                    table_schema,
                    &remain_columns,
                    has_inner_column,
                );

                Ok::<PrewhereInfo, ErrorCode>(PrewhereInfo {
                    output_columns,
                    prewhere_columns,
                    remain_columns,
                    filter,
                })
            })
            .transpose()?;

        let order_by = scan
            .order_by
            .clone()
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| {
                        let metadata = self.metadata.read();
                        let column = metadata.column(item.index);
                        let (name, data_type) = match column {
                            ColumnEntry::BaseTableColumn {
                                column_name,
                                data_type,
                                ..
                            } => (column_name.clone(), DataType::from(data_type)),
                            ColumnEntry::DerivedColumn {
                                alias, data_type, ..
                            } => (alias.clone(), data_type.clone()),
                        };

                        // sort item is already a column
                        let scalar = RemoteExpr::ColumnRef {
                            span: None,
                            id: name,
                            data_type,
                        };

                        Ok((scalar, item.asc, item.nulls_first))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;

        Ok(PushDownInfo {
            projection: Some(projection),
            filters: push_down_filters.unwrap_or_default(),
            prewhere: prewhere_info,
            limit: scan.limit,
            order_by: order_by.unwrap_or_default(),
        })
    }
}

#[derive(Default)]
pub struct PhysicalScalarBuilder {
    // input_schema: &'a DataSchemaRef,
}

impl PhysicalScalarBuilder {
    pub fn build(scalar: &Scalar) -> Result<PhysicalScalar> {
        match scalar {
            Scalar::BoundColumnRef(column_ref) => {
                let index = column_ref.column.index;
                Ok(PhysicalScalar::IndexedVariable {
                    data_type: column_ref.data_type(),
                    index,
                    display_name: format!(
                        "{}{} (#{})",
                        column_ref
                            .column
                            .table_name
                            .as_ref()
                            .map_or("".to_string(), |t| t.to_string() + "."),
                        column_ref.column.column_name.clone(),
                        column_ref.column.index
                    ),
                })
            }
            Scalar::ConstantExpr(constant) => Ok(PhysicalScalar::Constant {
                value: constant.value.clone(),
                data_type: *constant.data_type.clone(),
            }),
            Scalar::AndExpr(and) => Ok(PhysicalScalar::Function {
                name: "and".to_string(),
                args: vec![Self::build(&and.left)?, Self::build(&and.right)?],
                return_type: and.data_type(),
            }),
            Scalar::OrExpr(or) => Ok(PhysicalScalar::Function {
                name: "or".to_string(),
                args: vec![Self::build(&or.left)?, Self::build(&or.right)?],
                return_type: or.data_type(),
            }),
            Scalar::ComparisonExpr(comp) => Ok(PhysicalScalar::Function {
                name: comp.op.to_func_name(),
                args: vec![Self::build(&comp.left)?, Self::build(&comp.right)?],
                return_type: comp.data_type(),
            }),
            Scalar::FunctionCall(func) => Ok(PhysicalScalar::Function {
                name: func.func_name.clone(),
                args: func
                    .arguments
                    .iter()
                    .map(Self::build)
                    .collect::<Result<_>>()?,
                return_type: *func.return_type.clone(),
            }),
            Scalar::CastExpr(cast) => Ok(PhysicalScalar::Cast {
                input: Box::new(Self::build(&cast.argument)?),
                target: *cast.target_type.clone(),
            }),

            _ => Err(ErrorCode::Internal(format!(
                "Unsupported physical scalar: {:?}",
                scalar
            ))),
        }
    }
}
