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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::Domain;
use databend_common_expression::FunctionContext;
use databend_common_expression::StatEvaluator;
use databend_common_expression::stat_distribution::OwnedDistribution;
use databend_common_expression::stat_distribution::ReturnStat;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::ColumnBinding;
use crate::ColumnBindingBuilder;
use crate::ColumnSet;
use crate::Symbol;
use crate::Visibility;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::BoundColumnRef;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;

/// Evaluate scalar expression
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EvalScalar {
    pub items: Vec<ScalarItem>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScalarItem {
    pub scalar: ScalarExpr,
    // The index of the derived column in metadata
    pub index: Symbol,
}

impl ScalarItem {
    pub fn column_binding(&self, name: String) -> Result<ColumnBinding> {
        Ok(ColumnBindingBuilder::new(
            name,
            self.index,
            Box::new(self.scalar.data_type()?.into_owned()),
            Visibility::Visible,
        )
        .build())
    }

    pub fn bound_column_expr(&self, name: String) -> Result<ScalarExpr> {
        if let ScalarExpr::BoundColumnRef(_) = &self.scalar {
            return Ok(self.scalar.clone());
        }

        let column_binding = self.column_binding(name)?;
        Ok(BoundColumnRef {
            span: None,
            column: column_binding,
        }
        .into())
    }
}

impl EvalScalar {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for item in &self.items {
            used_columns.insert(item.index);
            item.scalar.collect_used_columns(&mut used_columns);
        }
        Ok(used_columns)
    }

    pub(crate) fn derive_item_stat(
        scalar: &ScalarExpr,
        input_statistics: &Statistics,
        cardinality: StatCardinality,
    ) -> Result<Option<ColumnStat>> {
        let expr = scalar.as_symbol_expr()?;
        let column_refs = expr.column_refs();
        let mut input_stats = HashMap::with_capacity(column_refs.len());
        for (index, data_type) in column_refs {
            let Some(column_stat) = input_statistics.column_stats.get(&index) else {
                return Ok(None);
            };
            let Ok(arg_stat) = column_stat.to_arg_stat(&data_type) else {
                return Ok(None);
            };
            input_stats.insert(index, arg_stat);
        }

        let Some(stat) = StatEvaluator::run(
            &expr,
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
            cardinality,
            &input_stats,
        )?
        else {
            return Ok(None);
        };
        Ok(Self::column_stat_from_return_stat(stat.into_owned()))
    }

    fn column_stat_from_return_stat(stat: ReturnStat) -> Option<ColumnStat> {
        // `ColumnStat` has no representation for an all-NULL column because its
        // min/max fields are non-optional. Do not retain the shadowed input stat
        // in that case; unknown is safer than a stale non-NULL distribution.
        let value_domain = match &stat.domain {
            Domain::Nullable(domain) => domain.value.as_deref()?,
            domain => domain,
        };
        let (min, max) = value_domain.to_minmax();
        let min = min.to_datum()?;
        let max = max.to_datum()?;
        let histogram = match stat.distribution {
            OwnedDistribution::Histogram(histogram) => Some(histogram),
            OwnedDistribution::Unknown | OwnedDistribution::Boolean(_) => None,
        };
        Some(ColumnStat {
            min,
            max,
            ndv: stat.ndv,
            null_count: stat.null_count,
            histogram,
        })
    }
}

impl Operator for EvalScalar {
    fn rel_op(&self) -> RelOp {
        RelOp::EvalScalar
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        Box::new(self.items.iter().map(|expr| &expr.scalar))
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let mut output_columns = input_prop.output_columns.clone();
        for item in self.items.iter() {
            output_columns.insert(item.index);
        }

        // Derive outer columns
        let mut outer_columns = input_prop
            .outer_columns
            .difference(&input_prop.output_columns)
            .cloned()
            .collect::<ColumnSet>();
        for item in &self.items {
            item.scalar.collect_used_columns(&mut outer_columns);
        }
        outer_columns.retain(|column| !input_prop.output_columns.contains(column));

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(input_prop.used_columns.iter().copied());

        // Derive orderings
        let orderings = input_prop.orderings.clone();
        let partition_orderings = input_prop.partition_orderings.clone();

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings,
            partition_orderings,
        }))
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let input = rel_expr.derive_cardinality_child(0)?;
        if self.items.iter().all(|item| {
            matches!(
                &item.scalar,
                ScalarExpr::BoundColumnRef(column) if column.column.index == item.index
            )
        }) {
            return Ok(input);
        }

        let cardinality = input
            .statistics
            .precise_cardinality
            .map(StatCardinality::exact)
            .unwrap_or_else(|| StatCardinality::estimate(input.cardinality));
        let defined_columns = self
            .items
            .iter()
            .map(|item| item.index)
            .collect::<ColumnSet>();
        debug_assert_eq!(
            defined_columns.len(),
            self.items.len(),
            "EvalScalar output indexes must be unique"
        );
        let Statistics {
            column_stats,
            top_n,
            count_min_sketch,
            ..
        } = &input.statistics;
        let item_column_stats = self
            .items
            .iter()
            .map(|item| {
                let stat = if let ScalarExpr::BoundColumnRef(column) = &item.scalar {
                    column_stats.get(&column.column.index).cloned()
                } else {
                    Self::derive_item_stat(&item.scalar, &input.statistics, cardinality)?
                };
                Ok(stat.map(|stat| (item.index, stat)))
            })
            .collect::<Result<Vec<_>>>()?;
        let column_stats = item_column_stats
            .into_iter()
            .flatten()
            .chain(column_stats.iter().filter_map(|(index, stat)| {
                if defined_columns.contains(index) {
                    None
                } else {
                    Some((*index, stat.clone()))
                }
            }))
            .collect();
        let top_n = self
            .items
            .iter()
            .filter_map(|item| {
                let ScalarExpr::BoundColumnRef(column) = &item.scalar else {
                    return None;
                };
                top_n
                    .get(&column.column.index)
                    .cloned()
                    .map(|top_n| (item.index, top_n))
            })
            .chain(top_n.iter().filter_map(|(index, top_n)| {
                if defined_columns.contains(index) {
                    None
                } else {
                    Some((*index, top_n.clone()))
                }
            }))
            .collect();
        let count_min_sketch = self
            .items
            .iter()
            .filter_map(|item| {
                let ScalarExpr::BoundColumnRef(column) = &item.scalar else {
                    return None;
                };
                count_min_sketch
                    .get(&column.column.index)
                    .cloned()
                    .map(|sketch| (item.index, sketch))
            })
            .chain(count_min_sketch.iter().filter_map(|(index, sketch)| {
                if defined_columns.contains(index) {
                    None
                } else {
                    Some((*index, sketch.clone()))
                }
            }))
            .collect();

        Ok(Arc::new(StatInfo {
            cardinality: input.cardinality,
            statistics: Statistics {
                precise_cardinality: input.statistics.precise_cardinality,
                column_stats,
                top_n,
                count_min_sketch,
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::stat_distribution::NdvEstimate;
    use databend_common_expression::stat_distribution::StatCount;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_statistics::Datum;

    use super::*;
    use crate::Visibility;
    use crate::optimizer::ir::SExpr;
    use crate::plans::CastExpr;
    use crate::plans::ConstantExpr;
    use crate::plans::DummyTableScan;
    use crate::plans::FunctionCall;

    fn int_type() -> DataType {
        DataType::Number(NumberDataType::Int64)
    }

    fn column_with_type(index: usize, data_type: DataType) -> ScalarExpr {
        BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                format!("c{index}"),
                Symbol::new(index),
                Box::new(data_type),
                Visibility::Visible,
            )
            .build(),
        }
        .into()
    }

    fn column(index: usize) -> ScalarExpr {
        column_with_type(index, int_type())
    }

    fn int_constant(value: i64) -> ScalarExpr {
        ConstantExpr {
            span: None,
            value: Scalar::Number(NumberScalar::Int64(value)),
        }
        .into()
    }

    fn typed_int_constant(value: i64, data_type: DataType) -> ScalarExpr {
        ScalarExpr::TypedConstantExpr(
            ConstantExpr {
                span: None,
                value: Scalar::Number(NumberScalar::Int64(value)),
            },
            data_type,
        )
    }

    fn column_stat(min: i64, max: i64, ndv: f64) -> ColumnStat {
        column_stat_with_nulls(min, max, ndv, 0)
    }

    fn column_stat_with_nulls(min: i64, max: i64, ndv: f64, null_count: u64) -> ColumnStat {
        ColumnStat {
            min: Datum::Int(min),
            max: Datum::Int(max),
            ndv: NdvEstimate::exact(ndv),
            null_count: StatCount::exact(null_count),
            histogram: None,
        }
    }

    fn derive(items: Vec<ScalarItem>, column_stats: HashMap<Symbol, ColumnStat>) -> Arc<StatInfo> {
        let input = SExpr::create(
            DummyTableScan::new(),
            vec![],
            None,
            None,
            Some(Arc::new(StatInfo {
                cardinality: 10.0,
                statistics: Statistics {
                    precise_cardinality: Some(10),
                    column_stats,
                    top_n: Default::default(),
                    count_min_sketch: Default::default(),
                },
            })),
        );
        let eval = SExpr::create_unary(EvalScalar { items }, input);
        RelExpr::with_s_expr(&eval).derive_cardinality().unwrap()
    }

    #[test]
    fn test_identity_projection_reuses_input_stats() {
        let input_stats = Arc::new(StatInfo {
            cardinality: 10.0,
            statistics: Statistics {
                precise_cardinality: Some(10),
                column_stats: HashMap::from([(Symbol::new(0), column_stat(1, 3, 3.0))]),
                top_n: Default::default(),
                count_min_sketch: Default::default(),
            },
        });
        let input = SExpr::create(
            DummyTableScan::new(),
            vec![],
            None,
            None,
            Some(input_stats.clone()),
        );
        let eval = SExpr::create_unary(
            EvalScalar {
                items: vec![ScalarItem {
                    scalar: column(0),
                    index: Symbol::new(0),
                }],
            },
            input,
        );

        let derived = RelExpr::with_s_expr(&eval).derive_cardinality().unwrap();
        assert!(Arc::ptr_eq(&derived, &input_stats));
    }

    #[test]
    fn test_derive_stats_copies_alias_and_derives_constant() {
        let stats = derive(
            vec![
                ScalarItem {
                    scalar: column(0),
                    index: Symbol::new(1),
                },
                ScalarItem {
                    scalar: int_constant(7),
                    index: Symbol::new(2),
                },
            ],
            HashMap::from([
                (Symbol::new(0), column_stat(1, 3, 3.0)),
                (Symbol::new(1), column_stat(100, 200, 10.0)),
                (Symbol::new(2), column_stat(300, 400, 10.0)),
            ]),
        );

        assert_eq!(stats.cardinality, 10.0);
        assert_eq!(stats.statistics.precise_cardinality, Some(10));
        let alias = &stats.statistics.column_stats[&Symbol::new(1)];
        assert_eq!(
            (alias.min.clone(), alias.max.clone()),
            (Datum::Int(1), Datum::Int(3))
        );
        assert_eq!(alias.ndv, NdvEstimate::exact(3.0));
        let constant = &stats.statistics.column_stats[&Symbol::new(2)];
        assert_eq!(
            (constant.min.clone(), constant.max.clone()),
            (Datum::Int(7), Datum::Int(7))
        );
        assert_eq!(constant.ndv, NdvEstimate::exact(1.0));
        assert_eq!(constant.null_count, StatCount::exact(0));
    }

    #[test]
    fn test_derive_stats_for_supported_function() {
        let nullable_int = DataType::Nullable(Box::new(int_type()));
        let stats = derive(
            vec![ScalarItem {
                scalar: ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: "plus".to_string(),
                    params: vec![],
                    arguments: vec![
                        column_with_type(0, nullable_int.clone()),
                        typed_int_constant(10, nullable_int.clone()),
                    ],
                    return_type: Box::new(nullable_int),
                }),
                index: Symbol::new(1),
            }],
            HashMap::from([
                (Symbol::new(0), column_stat_with_nulls(1, 3, 3.0, 2)),
                (Symbol::new(1), column_stat(100, 200, 10.0)),
            ]),
        );

        let derived = &stats.statistics.column_stats[&Symbol::new(1)];
        assert_eq!(
            (derived.min.clone(), derived.max.clone()),
            (Datum::Int(11), Datum::Int(13))
        );
        assert_eq!(derived.ndv, NdvEstimate::exact(3.0));
        assert_eq!(derived.null_count, StatCount::exact(2));
    }

    #[test]
    fn test_derive_stats_removes_stale_shadowed_stats() {
        let stats = derive(
            vec![
                ScalarItem {
                    scalar: ScalarExpr::TypedConstantExpr(
                        ConstantExpr {
                            span: None,
                            value: Scalar::Null,
                        },
                        DataType::Nullable(Box::new(int_type())),
                    ),
                    index: Symbol::new(1),
                },
                ScalarItem {
                    scalar: ScalarExpr::CastExpr(CastExpr {
                        span: None,
                        is_try: false,
                        argument: Box::new(column(0)),
                        target_type: Box::new(DataType::Number(NumberDataType::Int32)),
                    }),
                    index: Symbol::new(2),
                },
            ],
            HashMap::from([
                (Symbol::new(0), column_stat(1, 3, 3.0)),
                (Symbol::new(1), column_stat(100, 200, 10.0)),
                (Symbol::new(2), column_stat(300, 400, 10.0)),
            ]),
        );

        assert!(!stats.statistics.column_stats.contains_key(&Symbol::new(1)));
        assert!(!stats.statistics.column_stats.contains_key(&Symbol::new(2)));
        assert!(stats.statistics.column_stats.contains_key(&Symbol::new(0)));
    }
}
