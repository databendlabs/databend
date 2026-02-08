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

use std::borrow::Cow;
use std::collections::HashMap;

use databend_common_ast::Span;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::cast_scalar;
use databend_common_expression::is_internal_column;
use databend_common_expression::is_stream_column;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::decimal::Decimal128Type;
use databend_common_expression::types::decimal::Decimal256Type;
use databend_common_expression::types::decimal::DecimalDomain;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::SimpleDomain;
use databend_common_expression::types::string::StringDomain;
use databend_common_expression::visit_expr;
use databend_common_expression::with_number_mapped_type;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;

use super::eliminate_cast::*;
use crate::Index;

#[derive(Clone)]
pub struct RangeIndex {
    expr: Expr<String>,
    func_ctx: FunctionContext,
    schema: TableSchemaRef,

    // Default stats for each column if no stats are available (e.g. for new-add columns)
    default_stats: StatisticsOfColumns,
}

impl RangeIndex {
    pub fn try_create(
        func_ctx: FunctionContext,
        expr: &Expr<String>,
        schema: TableSchemaRef,
        default_stats: StatisticsOfColumns,
    ) -> Result<Self> {
        Ok(Self {
            expr: expr.clone(),
            func_ctx,
            schema,
            default_stats,
        })
    }

    pub fn try_apply_const(&self) -> Result<bool> {
        // Only return false, which means to skip this block, when the expression is folded to a constant false.
        Ok(!matches!(
            self.expr,
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            })
        ))
    }

    pub fn apply<F>(&self, stats: &StatisticsOfColumns, column_is_default: F) -> Result<bool>
    where F: Fn(&ColumnId) -> bool {
        let input_domains = self
            .expr
            .column_refs()
            .into_iter()
            .map(|(name, ty)| {
                // internal column and stream column are not actual stored columns
                if is_internal_column(&name) || is_stream_column(&name) {
                    return Ok((name, Domain::full(&ty)));
                }

                let column_ids = self.schema.leaf_columns_of(&name);
                // virtual columns are not included in leaf columns
                // TODO: add range filter for virtual columns
                if column_ids.is_empty() {
                    return Ok((name, Domain::full(&ty)));
                }

                let stats = column_ids
                    .iter()
                    .filter_map(|column_id| match stats.get(column_id) {
                        None => {
                            if column_is_default(column_id)
                                && self.default_stats.contains_key(column_id)
                            {
                                Some(&self.default_stats[column_id])
                            } else {
                                None
                            }
                        }
                        other => other,
                    })
                    .collect();

                let domain = statistics_to_domain(stats, &ty);
                Ok((name, domain))
            })
            .collect::<Result<_>>()?;

        let mut visitor = RewriteVisitor {
            input_domains,
            func_ctx: &self.func_ctx,
            fn_registry: &BUILTIN_FUNCTIONS,
        };

        let expr = match visit_expr(&self.expr, &mut visitor).unwrap() {
            Some(expr) => Cow::Owned(expr),
            None => Cow::Borrowed(&self.expr),
        };

        let (new_expr, _) = ConstantFolder::fold_with_domain(
            &expr,
            &visitor.input_domains,
            &self.func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        // Only return false, which means to skip this block, when the expression is folded to a constant false.
        Ok(!matches!(
            new_expr,
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            })
        ))
    }

    #[fastrace::trace]
    pub fn apply_with_partition_columns(
        &self,
        stats: &StatisticsOfColumns,
        partition_columns: &HashMap<String, Scalar>,
    ) -> Result<bool> {
        let expr = self.expr.fill_const_column(partition_columns);
        RangeIndex {
            expr,
            func_ctx: self.func_ctx.clone(),
            schema: self.schema.clone(),
            default_stats: self.default_stats.clone(),
        }
        .apply(stats, |_| false)
    }

    pub fn supported_table_type(data_type: &TableDataType) -> bool {
        let data_type = DataType::from(data_type);
        Self::supported_type(&data_type)
    }
}

pub fn statistics_to_domain(mut stats: Vec<&ColumnStatistics>, data_type: &DataType) -> Domain {
    if stats.len() != data_type.num_leaf_columns() {
        return Domain::full(data_type);
    }
    match data_type {
        DataType::Nullable(box inner_ty) => {
            if stats.len() == 1 && (stats[0].min.is_null() || stats[0].max.is_null()) {
                return Domain::Nullable(NullableDomain {
                    has_null: true,
                    value: None,
                });
            }
            let has_null = if stats.len() == 1 && !matches!(inner_ty, &DataType::Array(_)) {
                stats[0].null_count > 0
            } else {
                // Only leaf columns have statistics,
                // nested columns are treated as having nullable values
                true
            };
            let domain = statistics_to_domain(stats, inner_ty);
            Domain::Nullable(NullableDomain {
                has_null,
                value: Some(Box::new(domain)),
            })
        }
        DataType::Tuple(inner_tys) => {
            let inner_domains = inner_tys
                .iter()
                .map(|inner_ty| {
                    let n = inner_ty.num_leaf_columns();
                    let stats = stats.drain(..n).collect();
                    statistics_to_domain(stats, inner_ty)
                })
                .collect::<Vec<_>>();
            Domain::Tuple(inner_domains)
        }
        DataType::Array(box inner_ty) => {
            let n = inner_ty.num_leaf_columns();
            let stats = stats.drain(..n).collect();
            let inner_domain = statistics_to_domain(stats, inner_ty);
            Domain::Array(Some(Box::new(inner_domain)))
        }
        DataType::Map(box inner_ty) => {
            let n = inner_ty.num_leaf_columns();
            let stats = stats.drain(..n).collect();
            let inner_domain = statistics_to_domain(stats, inner_ty);
            Domain::Map(Some(Box::new(inner_domain)))
        }
        _ => {
            let stat = stats[0];
            let mut min = stat.min().clone();
            let mut max = stat.max().clone();
            if min.is_null() || max.is_null() {
                return Domain::full(data_type);
            }
            let inferred_type = min.as_ref().infer_data_type();
            if inferred_type != *data_type {
                let cast_min = cast_scalar(Span::None, min.clone(), data_type, &BUILTIN_FUNCTIONS);
                let cast_max = cast_scalar(Span::None, max.clone(), data_type, &BUILTIN_FUNCTIONS);
                if let (Ok(cast_min), Ok(cast_max)) = (cast_min, cast_max) {
                    min = cast_min;
                    max = cast_max;
                } else {
                    return Domain::full(data_type);
                }
            }

            with_number_mapped_type!(|NUM_TYPE| match data_type {
                DataType::Number(NumberDataType::NUM_TYPE) => {
                    NumberType::<NUM_TYPE>::upcast_domain(SimpleDomain {
                        min: NumberType::<NUM_TYPE>::try_downcast_scalar(&min.as_ref()).unwrap(),
                        max: NumberType::<NUM_TYPE>::try_downcast_scalar(&max.as_ref()).unwrap(),
                    })
                }
                DataType::String => Domain::String(StringDomain {
                    min: min.clone().into_string().unwrap(),
                    max: Some(max.clone().into_string().unwrap()),
                }),
                DataType::Timestamp => TimestampType::upcast_domain(SimpleDomain {
                    min: TimestampType::try_downcast_scalar(&min.as_ref()).unwrap(),
                    max: TimestampType::try_downcast_scalar(&max.as_ref()).unwrap(),
                }),
                DataType::Date => DateType::upcast_domain(SimpleDomain {
                    min: DateType::try_downcast_scalar(&min.as_ref()).unwrap(),
                    max: DateType::try_downcast_scalar(&max.as_ref()).unwrap(),
                }),
                DataType::Decimal(size) => {
                    debug_assert_eq!(*size, min.as_decimal().unwrap().size());
                    debug_assert_eq!(*size, max.as_decimal().unwrap().size());

                    let domain = match min.as_decimal().unwrap() {
                        DecimalScalar::Decimal64(_, _) => {
                            let domain = SimpleDomain {
                                min: Decimal64Type::try_downcast_scalar(&min.as_ref()).unwrap(),
                                max: Decimal64Type::try_downcast_scalar(&max.as_ref()).unwrap(),
                            };
                            DecimalDomain::Decimal64(domain, *size)
                        }
                        DecimalScalar::Decimal128(_, _) => {
                            let domain = SimpleDomain {
                                min: Decimal128Type::try_downcast_scalar(&min.as_ref()).unwrap(),
                                max: Decimal128Type::try_downcast_scalar(&max.as_ref()).unwrap(),
                            };
                            DecimalDomain::Decimal128(domain, *size)
                        }
                        DecimalScalar::Decimal256(_, _) => {
                            let domain = SimpleDomain {
                                min: Decimal256Type::try_downcast_scalar(&min.as_ref()).unwrap(),
                                max: Decimal256Type::try_downcast_scalar(&max.as_ref()).unwrap(),
                            };
                            DecimalDomain::Decimal256(domain, *size)
                        }
                    };
                    Domain::Decimal(domain)
                }

                // Unsupported data type
                _ => Domain::full(data_type),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Domain;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_storages_common_table_meta::meta::ColumnStatistics;

    use super::statistics_to_domain;

    #[test]
    fn test_statistics_to_domain_null_min_max_full() {
        let stat = ColumnStatistics::new(Scalar::Null, Scalar::Null, 0, 0, None);
        let ty = DataType::Number(NumberDataType::Int64);
        let domain = statistics_to_domain(vec![&stat], &ty);
        assert_eq!(domain, Domain::full(&ty));
    }
}

impl Index for RangeIndex {
    fn supported_type(data_type: &DataType) -> bool {
        databend_storages_common_table_meta::meta::supported_stat_type(data_type)
    }
}
