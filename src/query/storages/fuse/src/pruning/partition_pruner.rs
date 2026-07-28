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

use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::ExprVisitor;
use databend_common_expression::FunctionContext;
use databend_common_expression::RangeConstraint;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::expr::Cast;
use databend_common_expression::expr::ColumnRef;
use databend_common_expression::expr::FunctionCall;
use databend_common_expression::type_check::check_function;
use databend_common_expression::visit_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::plans::ComparisonOp;
use databend_common_statistics::HistogramBounds;
use databend_common_statistics::HistogramRangeBounds;
use databend_storages_common_table_meta::meta::ClusterStatistics;

use crate::statistics::partition_values;

#[derive(Clone)]
pub struct PartitionPruningInfo {
    pub cluster_key_id: u32,
    pub partition_keys: Vec<RemoteExpr<String>>,
}

pub struct PartitionPruner {
    cluster_key_id: u32,
    partition_keys: Vec<Expr<String>>,
    filter: Expr<String>,
    func_ctx: FunctionContext,
}

impl PartitionPruner {
    pub fn try_create(
        func_ctx: FunctionContext,
        filter: Option<&Expr<String>>,
        info: Option<PartitionPruningInfo>,
    ) -> Option<Self> {
        let filter = filter?.clone();
        let info = info?;
        if info.partition_keys.is_empty() {
            return None;
        }

        let partition_keys = info
            .partition_keys
            .into_iter()
            .map(|expr| {
                ConstantFolder::fold(
                    &expr.as_expr(&BUILTIN_FUNCTIONS),
                    &func_ctx,
                    &BUILTIN_FUNCTIONS,
                )
                .0
            })
            .collect();

        Some(Self {
            cluster_key_id: info.cluster_key_id,
            partition_keys,
            filter,
            func_ctx,
        })
    }

    pub fn should_keep(&self, stats: Option<&ClusterStatistics>) -> bool {
        let Some(values) =
            partition_values(stats, Some(self.cluster_key_id), self.partition_keys.len())
        else {
            return true;
        };

        let replacements = self.partition_keys.iter().zip(values).collect::<Vec<_>>();
        let mut visitor = PartitionPredicateRewriter {
            replacements,
            func_ctx: &self.func_ctx,
        };
        if !visitor.conjunctive_predicate_possible(&self.filter) {
            return false;
        }
        let filter = visit_expr(&self.filter, &mut visitor)
            .unwrap()
            .unwrap_or_else(|| self.filter.clone());
        let (filter, _) = ConstantFolder::fold(&filter, &self.func_ctx, &BUILTIN_FUNCTIONS);

        !matches!(
            filter,
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            })
        )
    }
}

struct PartitionPredicateRewriter<'a> {
    replacements: Vec<(&'a Expr<String>, &'a Scalar)>,
    func_ctx: &'a FunctionContext,
}

impl PartitionPredicateRewriter<'_> {
    fn replace(&self, expr: Expr<String>) -> Option<Expr<String>> {
        self.replacements
            .iter()
            .find(|(partition_expr, _)| *partition_expr == &expr)
            .map(|(partition_expr, value)| {
                Expr::Constant(Constant {
                    span: None,
                    scalar: (*value).clone(),
                    data_type: partition_expr.data_type().clone(),
                })
            })
    }

    fn replace_comparison(&self, call: &FunctionCall<String>) -> Option<Expr<String>> {
        let constraint = RangeConstraint::try_from_function_call(call)?;
        let predicate_domain = predicate_domain(std::slice::from_ref(&constraint))?;
        (!self.column_domain_possible(&constraint.column_id, &predicate_domain)).then(|| {
            Expr::Constant(Constant {
                span: call.span,
                scalar: Scalar::Boolean(false),
                data_type: call.return_type.clone(),
            })
        })
    }

    fn conjunctive_predicate_possible(&self, filter: &Expr<String>) -> bool {
        let mut ranges = HashMap::new();
        collect_conjunctive_ranges(filter, &mut ranges);
        ranges.values().all(|constraints| {
            let Some(domain) = predicate_domain(constraints) else {
                return true;
            };
            self.column_domain_possible(&constraints[0].column_id, &domain)
        })
    }

    fn column_domain_possible(&self, column_id: &str, domain: &Domain) -> bool {
        self.replacements
            .iter()
            .all(|(partition_expr, partition_value)| {
                let mut input_domains = ConstantFolder::full_input_domains(partition_expr);
                if input_domains
                    .insert(column_id.to_string(), domain.clone())
                    .is_none()
                {
                    return true;
                }
                self.partition_value_possible(partition_expr, partition_value, &input_domains)
            })
    }

    fn partition_value_possible(
        &self,
        partition_expr: &Expr<String>,
        partition_value: &Scalar,
        input_domains: &HashMap<String, Domain>,
    ) -> bool {
        let expected = Expr::Constant(Constant {
            span: None,
            scalar: partition_value.clone(),
            data_type: partition_expr.data_type().clone(),
        });
        let Ok(equality) = check_function(
            None,
            "eq",
            &[],
            &[partition_expr.clone(), expected],
            &BUILTIN_FUNCTIONS,
        ) else {
            return true;
        };
        let (equality, _) = ConstantFolder::fold_with_domain(
            &equality,
            input_domains,
            self.func_ctx,
            &BUILTIN_FUNCTIONS,
        );
        !matches!(
            equality,
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            })
        )
    }
}

fn predicate_domain(constraints: &[RangeConstraint<String>]) -> Option<Domain> {
    let data_type = &constraints.first()?.data_type;
    if constraints
        .iter()
        .any(|constraint| constraint.data_type != *data_type || constraint.constant.is_null())
    {
        return None;
    }

    if let Some(constraint) = constraints
        .iter()
        .find(|constraint| constraint.operator == "eq")
    {
        return Some(constraint.constant.as_ref().domain(data_type));
    }

    let (min, max) = Domain::full(&data_type.remove_nullable()).to_minmax();
    let mut bounds = HistogramBounds::new(min.to_datum()?, max.to_datum()?);
    for constraint in constraints {
        let op = ComparisonOp::try_from_func_name(&constraint.operator)?;
        let value = constraint.constant.clone().to_datum()?;
        let (lower, upper) = op.range_bounds(value)?;
        bounds = match HistogramBounds::from_range_constraint(
            bounds.lower_bound(),
            bounds.upper_bound(),
            &lower,
            &upper,
        )
        .ok()?
        {
            HistogramRangeBounds::Bounds(bounds) => bounds,
            HistogramRangeBounds::Empty | HistogramRangeBounds::Imprecise => return None,
        };
    }
    Domain::from_datum(
        data_type,
        bounds.lower_bound().clone(),
        bounds.upper_bound().clone(),
        false,
    )
    .ok()
}

fn collect_conjunctive_ranges(
    expr: &Expr<String>,
    ranges: &mut HashMap<String, Vec<RangeConstraint<String>>>,
) {
    let Expr::FunctionCall(call) = expr else {
        return;
    };
    match call.function.signature.name.as_str() {
        "and" | "and_filters" => {
            for arg in &call.args {
                collect_conjunctive_ranges(arg, ranges);
            }
        }
        "is_true" if call.args.len() == 1 => collect_conjunctive_ranges(&call.args[0], ranges),
        _ => {
            let Some(constraint) = RangeConstraint::try_from_function_call(call) else {
                return;
            };
            if constraint.operator != "noteq" {
                ranges
                    .entry(constraint.column_id.clone())
                    .or_default()
                    .push(constraint);
            }
        }
    }
}

impl ExprVisitor<String> for PartitionPredicateRewriter<'_> {
    fn enter_column_ref(
        &mut self,
        column: &ColumnRef<String>,
    ) -> Result<Option<Expr<String>>, Self::Error> {
        Ok(self.replace(Expr::ColumnRef(column.clone())))
    }

    fn enter_cast(&mut self, cast: &Cast<String>) -> Result<Option<Expr<String>>, Self::Error> {
        if let Some(expr) = self.replace(Expr::Cast(cast.clone())) {
            Ok(Some(expr))
        } else {
            Self::visit_cast(cast, self)
        }
    }

    fn enter_function_call(
        &mut self,
        call: &FunctionCall<String>,
    ) -> Result<Option<Expr<String>>, Self::Error> {
        if let Some(expr) = self.replace(Expr::FunctionCall(call.clone())) {
            Ok(Some(expr))
        } else if let Some(expr) = self.replace_comparison(call) {
            Ok(Some(expr))
        } else {
            Self::visit_function_call(call, self)
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;

    use super::*;

    fn column_ref(id: &str, data_type: DataType) -> Expr<String> {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id: id.to_string(),
            data_type,
            display_name: "p".to_string(),
        })
    }

    fn int64(value: i64) -> Expr<String> {
        Expr::Constant(Constant {
            span: None,
            scalar: Scalar::Number(value.into()),
            data_type: DataType::Number(NumberDataType::Int64),
        })
    }

    fn timestamp(value: i64) -> Expr<String> {
        Expr::Constant(Constant {
            span: None,
            scalar: Scalar::Timestamp(value),
            data_type: DataType::Timestamp,
        })
    }

    fn call(name: &str, args: Vec<Expr<String>>) -> Expr<String> {
        check_function(None, name, &[], &args, &BUILTIN_FUNCTIONS).unwrap()
    }

    fn assert_comparison_pruned(
        partition_expr: &Expr<String>,
        partition_value: &Scalar,
        filter: Expr<String>,
    ) {
        let Expr::FunctionCall(call) = filter else {
            panic!("expected function call");
        };
        let func_ctx = FunctionContext::default();
        let rewriter = PartitionPredicateRewriter {
            replacements: vec![(partition_expr, partition_value)],
            func_ctx: &func_ctx,
        };
        assert!(matches!(
            rewriter.replace_comparison(&call),
            Some(Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            }))
        ));
    }

    fn assert_timestamp_range_partition_projection(
        partition_expr: Expr<String>,
        matching_value: Scalar,
        non_matching_value: Scalar,
    ) {
        const MICROS_PER_DAY: i64 = 86_400_000_000;

        let column = column_ref("order_time", DataType::Timestamp);
        let filter = call("and_filters", vec![
            call("gte", vec![column.clone(), timestamp(0)]),
            call("lt", vec![column, timestamp(MICROS_PER_DAY)]),
        ]);
        let func_ctx = FunctionContext::default();

        let rewriter = PartitionPredicateRewriter {
            replacements: vec![(&partition_expr, &matching_value)],
            func_ctx: &func_ctx,
        };
        assert!(rewriter.conjunctive_predicate_possible(&filter));

        let rewriter = PartitionPredicateRewriter {
            replacements: vec![(&partition_expr, &non_matching_value)],
            func_ctx: &func_ctx,
        };
        assert!(!rewriter.conjunctive_predicate_possible(&filter));
    }

    #[test]
    fn test_replace_partition_expr_by_structural_identity() {
        let partition_expr = column_ref("p", DataType::Number(NumberDataType::Int64));
        let partition_value = Scalar::Number(1_i64.into());
        let func_ctx = FunctionContext::default();
        let pruner = PartitionPredicateRewriter {
            replacements: vec![(&partition_expr, &partition_value)],
            func_ctx: &func_ctx,
        };

        let replaced = pruner.replace(partition_expr.clone());
        assert_eq!(
            replaced,
            Some(Expr::Constant(Constant {
                span: None,
                scalar: partition_value,
                data_type: DataType::Number(NumberDataType::Int64),
            }))
        );
    }

    #[test]
    fn test_does_not_replace_different_expr_with_same_sql_display() {
        let partition_expr = column_ref("p", DataType::Number(NumberDataType::Int64));
        let filter_expr = column_ref("p", DataType::Number(NumberDataType::UInt64));
        assert_eq!(partition_expr.sql_display(), filter_expr.sql_display());

        let partition_value = Scalar::Number(1_i64.into());
        let func_ctx = FunctionContext::default();
        let pruner = PartitionPredicateRewriter {
            replacements: vec![(&partition_expr, &partition_value)],
            func_ctx: &func_ctx,
        };

        assert_eq!(pruner.replace(filter_expr), None);
    }

    #[test]
    fn test_replace_source_column_range_comparison() {
        let data_type = DataType::Number(NumberDataType::Int64);
        let column = column_ref("p", data_type);

        let upper_partition_expr = call("plus", vec![column.clone(), int64(1)]);
        let upper_partition_value = Scalar::Number(20_i64.into());
        assert_comparison_pruned(
            &upper_partition_expr,
            &upper_partition_value,
            call("lte", vec![column.clone(), int64(10)]),
        );
        assert_comparison_pruned(
            &upper_partition_expr,
            &upper_partition_value,
            call("gte", vec![int64(10), column.clone()]),
        );

        let lower_partition_expr = call("minus", vec![column.clone(), int64(1)]);
        let lower_partition_value = Scalar::Number(0_i64.into());
        assert_comparison_pruned(
            &lower_partition_expr,
            &lower_partition_value,
            call("gte", vec![column.clone(), int64(10)]),
        );
        assert_comparison_pruned(
            &lower_partition_expr,
            &lower_partition_value,
            call("lte", vec![int64(10), column]),
        );
    }

    #[test]
    fn test_combines_conjunctive_source_column_range() {
        let data_type = DataType::Number(NumberDataType::Int64);
        let column = column_ref("p", data_type);
        let partition_expr = call("modulo", vec![column.clone(), int64(3)]);
        let partition_value = Scalar::Number(2_i64.into());
        let filter = call("and_filters", vec![
            call("gte", vec![column.clone(), int64(4)]),
            call("lte", vec![column, int64(4)]),
        ]);
        let func_ctx = FunctionContext::default();
        let rewriter = PartitionPredicateRewriter {
            replacements: vec![(&partition_expr, &partition_value)],
            func_ctx: &func_ctx,
        };

        assert!(!rewriter.conjunctive_predicate_possible(&filter));

        let matching_partition_value = Scalar::Number(1_i64.into());
        let rewriter = PartitionPredicateRewriter {
            replacements: vec![(&partition_expr, &matching_partition_value)],
            func_ctx: &func_ctx,
        };
        assert!(rewriter.conjunctive_predicate_possible(&filter));
    }

    #[test]
    fn test_does_not_combine_ranges_across_disjunction() {
        let data_type = DataType::Number(NumberDataType::Int64);
        let column = column_ref("p", data_type);
        let partition_expr = call("modulo", vec![column.clone(), int64(3)]);
        let partition_value = Scalar::Number(2_i64.into());
        let filter = call("or", vec![
            call("eq", vec![column.clone(), int64(4)]),
            call("eq", vec![column, int64(5)]),
        ]);
        let func_ctx = FunctionContext::default();
        let rewriter = PartitionPredicateRewriter {
            replacements: vec![(&partition_expr, &partition_value)],
            func_ctx: &func_ctx,
        };

        assert!(rewriter.conjunctive_predicate_possible(&filter));
    }

    #[test]
    fn test_projects_timestamp_range_through_day_partition() {
        let column = column_ref("order_time", DataType::Timestamp);
        assert_timestamp_range_partition_projection(
            call("to_date", vec![column.clone()]),
            Scalar::Date(0),
            Scalar::Date(1),
        );
        assert_timestamp_range_partition_projection(
            call("to_start_of_day", vec![column]),
            Scalar::Timestamp(0),
            Scalar::Timestamp(86_400_000_000),
        );
    }

    #[test]
    fn test_should_keep_returns_true_for_segment_without_partition_metadata() {
        // Segments written before PARTITION BY was added have no cluster statistics.
        // The pruner must keep them conservatively rather than silently dropping rows.
        let partition_expr = column_ref("p", DataType::Number(NumberDataType::Int64));
        let filter = call("eq", vec![partition_expr.clone(), {
            Expr::Constant(Constant {
                span: None,
                scalar: Scalar::Number(1_i64.into()),
                data_type: DataType::Number(NumberDataType::Int64),
            })
        }]);

        let pruner = PartitionPruner {
            cluster_key_id: 0,
            partition_keys: vec![partition_expr],
            filter,
            func_ctx: FunctionContext::default(),
        };

        // No cluster stats at all — must keep the segment.
        assert!(pruner.should_keep(None));
    }
}
