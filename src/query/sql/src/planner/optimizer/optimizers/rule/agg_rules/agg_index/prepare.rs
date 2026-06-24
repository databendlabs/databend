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

use std::cmp::Ordering;
use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FieldIndex;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;

use super::AggIndexViewInfo;
use super::ColumnExprEntry;
use super::ColumnIdentity;
use super::EquivalenceClasses;
use super::IndexOutputColumn;
use super::QueryInfo;
use super::ResidualClasses;
use super::ScalarExprMatcher;
use super::ScalarMatchContext;
use crate::ColumnEntry;
use crate::IndexType;
use crate::ScalarExpr;
use crate::Symbol;
use crate::Visibility;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ir::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;

struct ColumnExprSourceEntry {
    index: Symbol,
    source: ColumnExprSource,
}

#[derive(Debug)]
struct ColumnExprCandidate {
    expr: ScalarExpr,
    index: Symbol,
    source: ColumnExprSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ColumnExprSource {
    order: usize,
}

impl ColumnExprSource {
    fn prefer_over(self, other: Self, index: Symbol, other_index: Symbol) -> bool {
        match self.order.cmp(&other.order) {
            Ordering::Less => false,
            Ordering::Greater => true,
            Ordering::Equal => index < other_index,
        }
    }
}

#[derive(Debug)]
struct EqualityPredicate {
    left: ScalarExpr,
    right: ScalarExpr,
}

#[derive(Debug)]
struct RangePredicate {
    op: ComparisonOp,
    column: ScalarExpr,
    value: ConstantExpr,
}

#[derive(Debug)]
struct RangeClassEntry {
    column: ScalarExpr,
    values: RangeValues,
}

type ComparisonBound = Option<(Scalar, &'static str)>;

#[derive(Debug)]
pub(super) struct CompensatingRange {
    column: ScalarExpr,
    lower_bound: BoundValue,
    upper_bound: BoundValue,
}

impl CompensatingRange {
    pub(super) fn column(&self) -> &ScalarExpr {
        &self.column
    }

    pub(super) fn comparison_bounds(&self) -> (ComparisonBound, ComparisonBound) {
        (
            self.lower_bound.comparison_bound("gte", "gt"),
            self.upper_bound.comparison_bound("lte", "lt"),
        )
    }
}

impl QueryInfo {
    pub(super) fn new(
        table_index: IndexType,
        table_name: &str,
        base_columns: &[ColumnEntry],
        s_expr: &SExpr,
    ) -> Result<QueryInfo> {
        let RelOperator::EvalScalar(selection) = s_expr.plan() else {
            return Err(ErrorCode::Internal("Unsupported plan"));
        };

        let mut predicates: Option<&[ScalarExpr]> = None;
        let mut sort_items = None;
        let mut aggregate = None;
        let mut column_map = HashMap::new();
        let mut column_identities = HashMap::new();
        let mut column_expr_sources = Vec::new();
        let mut next_source_order = 0usize;

        let mut record_column_source = |index: Symbol, scalar: ScalarExpr| {
            column_map.insert(index, scalar);
            column_expr_sources.push(ColumnExprSourceEntry {
                index,
                source: ColumnExprSource {
                    order: next_source_order,
                },
            });
            next_source_order += 1;
        };

        for item in &selection.items {
            record_column_source(item.index, item.scalar.clone());
        }

        let mut s_expr = s_expr.unary_child();
        loop {
            match s_expr.plan() {
                RelOperator::EvalScalar(eval) => {
                    for item in &eval.items {
                        record_column_source(item.index, item.scalar.clone());
                    }
                }
                RelOperator::Aggregate(agg) => {
                    if agg.grouping_sets.is_some() {
                        return Err(ErrorCode::Internal("Grouping sets is not supported"));
                    }

                    aggregate = Some(agg.clone());
                    for item in &agg.aggregate_functions {
                        record_column_source(item.index, item.scalar.clone());
                    }
                    for item in &agg.group_items {
                        record_column_source(item.index, item.scalar.clone());
                    }
                    let child = s_expr.unary_child();
                    if let RelOperator::EvalScalar(eval) = child.plan() {
                        for item in &eval.items {
                            record_column_source(item.index, item.scalar.clone());
                        }
                        s_expr = child.unary_child();
                        continue;
                    }
                }
                RelOperator::Sort(sort) => {
                    sort_items = Some(sort.items.clone());
                }
                RelOperator::Filter(filter) => {
                    predicates = Some(filter.predicates.as_ref());
                }
                RelOperator::Scan(scan) => {
                    if let Some(prewhere) = &scan.prewhere {
                        debug_assert!(predicates.is_none());
                        predicates = Some(prewhere.predicates.as_ref());
                    }
                    break;
                }
                _ => {
                    return Err(ErrorCode::Internal("Unsupported plan"));
                }
            }
            s_expr = s_expr.unary_child();
        }

        for base_column in base_columns {
            if let ColumnEntry::BaseTableColumn(column) = base_column
                && column.virtual_expr.is_none()
            {
                column_identities.insert(base_column.index(), ColumnIdentity {
                    table_index: column.table_index,
                    column_id: column.column_id,
                    path_indices: column.path_indices.clone(),
                });
            }
            let column_binding = ColumnBindingBuilder::new(
                base_column.name(),
                base_column.index(),
                Box::new(base_column.data_type()),
                Visibility::Visible,
            )
            .table_name(Some(table_name.to_string()))
            .table_index(Some(table_index))
            .build();

            let column = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: column_binding,
            });
            record_column_source(base_column.index(), column);
        }

        let mut column_expr_candidates = Vec::new();
        let column_match_context = ScalarMatchContext {
            column_map: &column_map,
            column_identities: &column_identities,
        };
        let column_matcher = ScalarExprMatcher {
            left: column_match_context,
            right: column_match_context,
        };
        for (scalar, source) in column_expr_sources
            .iter()
            .filter_map(|source| column_map.get(&source.index).map(|scalar| (scalar, source)))
        {
            register_column_expr(
                &mut column_expr_candidates,
                scalar.clone(),
                source.index,
                source.source,
                &column_matcher,
            );
        }
        let column_exprs = column_expr_candidates
            .into_iter()
            .map(|entry| ColumnExprEntry {
                expr: entry.expr,
                index: entry.index,
            })
            .collect();

        let mut equi_classes = EquivalenceClasses::default();
        let mut range_classes = RangeClasses::default();
        let mut residual_classes = ResidualClasses::default();

        if let Some(predicates) = predicates {
            let mut preds_splitter = PredicatesSplitter::default();
            for pred in predicates {
                preds_splitter.split(pred, &column_map);
            }

            for equi_pred in &preds_splitter.equi_columns_preds {
                equi_classes.add_equivalence_class(
                    &equi_pred.left,
                    &equi_pred.right,
                    &column_matcher,
                );
            }
            for range_pred in &preds_splitter.range_preds {
                range_classes.add_range_class(
                    range_pred.op,
                    &range_pred.column,
                    &range_pred.value,
                    &column_matcher,
                );
            }
            for residual_pred in &preds_splitter.residual_preds {
                residual_classes.add_residual_pred(residual_pred.clone(), &column_matcher);
            }
        }

        let output_cols = selection
            .items
            .iter()
            .map(|item| {
                let actual_scalar = actual_column_ref(&item.scalar, &column_map);
                ScalarItem {
                    index: item.index,
                    scalar: actual_scalar.clone(),
                }
            })
            .collect();

        Ok(Self {
            equi_classes,
            range_classes,
            residual_classes,
            aggregate,
            sort_items,
            column_map,
            column_identities,
            column_exprs,
            output_cols,
        })
    }
}

impl AggIndexViewInfo {
    pub(super) fn new(
        table_index: IndexType,
        table_name: &str,
        base_columns: &[ColumnEntry],
        s_expr: &SExpr,
    ) -> Result<AggIndexViewInfo> {
        let query_info = QueryInfo::new(table_index, table_name, base_columns, s_expr)?;

        let mut index_fields = Vec::with_capacity(query_info.output_cols.len());
        let mut index_output_cols = Vec::with_capacity(query_info.output_cols.len());
        let factory = AggregateFunctionFactory::instance();
        for (index, item) in query_info.output_cols.iter().enumerate() {
            let aggr_scalar_item = query_info.aggregate.as_ref().and_then(|aggregate| {
                aggregate
                    .aggregate_functions
                    .iter()
                    .find(|agg_func| agg_func.index == item.index)
            });

            let (data_type, is_agg) = match aggr_scalar_item {
                Some(item) => {
                    let func = match &item.scalar {
                        ScalarExpr::AggregateFunction(func) => func,
                        _ => unreachable!(),
                    };
                    let func = factory.get(
                        &func.func_name,
                        func.params.clone(),
                        func.args
                            .iter()
                            .map(|arg| arg.data_type())
                            .collect::<Result<_>>()?,
                        func.sort_descs
                            .iter()
                            .map(|desc| desc.try_into())
                            .collect::<Result<_>>()?,
                    )?;
                    (func.serialize_data_type(), true)
                }
                None => (item.scalar.data_type().unwrap(), false),
            };

            let name = index.to_string();
            let table_ty = infer_schema_type(&data_type)?;
            let index_field = TableField::new(&name, table_ty);
            index_fields.push(index_field);

            let index_scalar = to_index_scalar(index, &data_type);
            index_output_cols.push(IndexOutputColumn {
                expr: item.scalar.clone(),
                index_scalar,
                is_agg,
            });
        }

        Ok(Self {
            query_info,
            index_fields,
            index_output_cols,
        })
    }
}

#[derive(Default)]
struct PredicatesSplitter {
    equi_columns_preds: Vec<EqualityPredicate>,
    range_preds: Vec<RangePredicate>,
    residual_preds: Vec<ScalarExpr>,
}

impl PredicatesSplitter {
    fn split(&mut self, pred: &ScalarExpr, column_map: &HashMap<Symbol, ScalarExpr>) {
        let ScalarExpr::FunctionCall(func) = pred else {
            self.residual_preds.push(pred.clone());
            return;
        };

        match func.func_name.as_str() {
            "and" | "and_filters" => {
                for arg in &func.arguments {
                    self.split(arg, column_map);
                }
            }
            "eq" if matches!(func.arguments[0], ScalarExpr::BoundColumnRef(_))
                && matches!(func.arguments[1], ScalarExpr::BoundColumnRef(_)) =>
            {
                let arg0 = actual_column_ref(&func.arguments[0], column_map);
                let arg1 = actual_column_ref(&func.arguments[1], column_map);
                self.equi_columns_preds.push(EqualityPredicate {
                    left: arg0.clone(),
                    right: arg1.clone(),
                });
            }
            "eq" | "lt" | "lte" | "gt" | "gte"
                if matches!(func.arguments[0], ScalarExpr::BoundColumnRef(_))
                    && matches!(func.arguments[1], ScalarExpr::ConstantExpr(_)) =>
            {
                let op = ComparisonOp::try_from_func_name(func.func_name.as_str()).unwrap();
                let column = actual_column_ref(&func.arguments[0], column_map).clone();
                let value = ConstantExpr::try_from(func.arguments[1].clone()).unwrap();
                self.range_preds.push(RangePredicate { op, column, value });
            }
            "eq" | "lt" | "lte" | "gt" | "gte"
                if matches!(func.arguments[0], ScalarExpr::ConstantExpr(_))
                    && matches!(func.arguments[1], ScalarExpr::BoundColumnRef(_)) =>
            {
                let op = ComparisonOp::try_from_func_name(func.func_name.as_str())
                    .unwrap()
                    .reverse();
                let value = ConstantExpr::try_from(func.arguments[0].clone()).unwrap();
                let column = actual_column_ref(&func.arguments[1], column_map).clone();
                self.range_preds.push(RangePredicate { op, column, value });
            }
            _ => {
                self.residual_preds.push(pred.clone());
            }
        }
    }
}

impl EquivalenceClasses {
    fn add_equivalence_class(
        &mut self,
        col1: &ScalarExpr,
        col2: &ScalarExpr,
        matcher: &ScalarExprMatcher<'_, '_>,
    ) {
        let mut merged = vec![col1.clone(), col2.clone()];
        let mut idx = 0;
        while idx < self.classes.len() {
            if matcher.list_contains(&self.classes[idx], col1)
                || matcher.list_contains(&self.classes[idx], col2)
            {
                let class = self.classes.remove(idx);
                for scalar in class {
                    if !matcher.list_contains(&merged, &scalar) {
                        merged.push(scalar);
                    }
                }
            } else {
                idx += 1;
            }
        }
        self.classes.push(merged);
    }
}

#[derive(Debug)]
struct RangeValues {
    bounds: Option<(BoundValue, BoundValue)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum BoundValue {
    Closed(Scalar),
    Open(Scalar),
    NegativeInfinite,
    PositiveInfinite,
}

impl BoundValue {
    fn cmp_scalar(s1: &Scalar, s2: &Scalar) -> Ordering {
        match (s1, s2) {
            (Scalar::Number(n1), Scalar::Number(n2)) => {
                if n1.is_integer() && n2.is_integer() {
                    let v1 = n1.integer_to_i128().unwrap();
                    let v2 = n2.integer_to_i128().unwrap();
                    v1.cmp(&v2)
                } else {
                    let v1 = n1.to_f64();
                    let v2 = n2.to_f64();
                    v1.cmp(&v2)
                }
            }
            (_, _) => s1.cmp(s2),
        }
    }

    fn cmp_lower_bound(&self, other: &Self) -> Ordering {
        match (self, other) {
            (BoundValue::NegativeInfinite, BoundValue::NegativeInfinite)
            | (BoundValue::PositiveInfinite, BoundValue::PositiveInfinite) => Ordering::Equal,
            (BoundValue::NegativeInfinite, _) => Ordering::Less,
            (_, BoundValue::NegativeInfinite) => Ordering::Greater,
            (BoundValue::PositiveInfinite, _) => Ordering::Greater,
            (_, BoundValue::PositiveInfinite) => Ordering::Less,
            (BoundValue::Open(v1), BoundValue::Open(v2))
            | (BoundValue::Open(v1), BoundValue::Closed(v2))
            | (BoundValue::Closed(v1), BoundValue::Open(v2))
            | (BoundValue::Closed(v1), BoundValue::Closed(v2)) => match Self::cmp_scalar(v1, v2) {
                Ordering::Equal => match (self, other) {
                    (BoundValue::Open(_), BoundValue::Closed(_)) => Ordering::Greater,
                    (BoundValue::Closed(_), BoundValue::Open(_)) => Ordering::Less,
                    _ => Ordering::Equal,
                },
                ordering => ordering,
            },
        }
    }

    fn cmp_upper_bound(&self, other: &Self) -> Ordering {
        match (self, other) {
            (BoundValue::NegativeInfinite, BoundValue::NegativeInfinite)
            | (BoundValue::PositiveInfinite, BoundValue::PositiveInfinite) => Ordering::Equal,
            (BoundValue::NegativeInfinite, _) => Ordering::Less,
            (_, BoundValue::NegativeInfinite) => Ordering::Greater,
            (BoundValue::PositiveInfinite, _) => Ordering::Greater,
            (_, BoundValue::PositiveInfinite) => Ordering::Less,
            (BoundValue::Open(v1), BoundValue::Open(v2))
            | (BoundValue::Open(v1), BoundValue::Closed(v2))
            | (BoundValue::Closed(v1), BoundValue::Open(v2))
            | (BoundValue::Closed(v1), BoundValue::Closed(v2)) => match Self::cmp_scalar(v1, v2) {
                Ordering::Equal => match (self, other) {
                    (BoundValue::Open(_), BoundValue::Closed(_)) => Ordering::Less,
                    (BoundValue::Closed(_), BoundValue::Open(_)) => Ordering::Greater,
                    _ => Ordering::Equal,
                },
                ordering => ordering,
            },
        }
    }

    fn cmp_lower_to_upper(lower: &Self, upper: &Self) -> Ordering {
        match (lower, upper) {
            (BoundValue::NegativeInfinite, _) | (_, BoundValue::PositiveInfinite) => Ordering::Less,
            (BoundValue::PositiveInfinite, _) | (_, BoundValue::NegativeInfinite) => {
                Ordering::Greater
            }
            (BoundValue::Open(lower_value), BoundValue::Open(upper_value))
            | (BoundValue::Open(lower_value), BoundValue::Closed(upper_value))
            | (BoundValue::Closed(lower_value), BoundValue::Open(upper_value))
            | (BoundValue::Closed(lower_value), BoundValue::Closed(upper_value)) => {
                match Self::cmp_scalar(lower_value, upper_value) {
                    Ordering::Equal => match (lower, upper) {
                        (BoundValue::Closed(_), BoundValue::Closed(_)) => Ordering::Equal,
                        _ => Ordering::Greater,
                    },
                    ordering => ordering,
                }
            }
        }
    }

    fn comparison_bound(
        &self,
        closed_name: &'static str,
        open_name: &'static str,
    ) -> Option<(Scalar, &'static str)> {
        match self {
            BoundValue::Closed(val) => Some((val.clone(), closed_name)),
            BoundValue::Open(val) => Some((val.clone(), open_name)),
            BoundValue::NegativeInfinite | BoundValue::PositiveInfinite => None,
        }
    }
}

impl RangeValues {
    fn new() -> Self {
        Self {
            bounds: Some((BoundValue::NegativeInfinite, BoundValue::PositiveInfinite)),
        }
    }

    fn insert(&mut self, lower_bound: BoundValue, upper_bound: BoundValue) {
        if let Some((orig_lower_bound, orig_upper_bound)) = &self.bounds {
            let lower_bound = if lower_bound.cmp_lower_bound(orig_lower_bound) == Ordering::Greater
            {
                lower_bound
            } else {
                orig_lower_bound.clone()
            };
            let upper_bound = if upper_bound.cmp_upper_bound(orig_upper_bound) == Ordering::Less {
                upper_bound
            } else {
                orig_upper_bound.clone()
            };
            if BoundValue::cmp_lower_to_upper(&lower_bound, &upper_bound) == Ordering::Greater {
                self.bounds = None;
                return;
            }
            self.bounds = Some((lower_bound, upper_bound));
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct RangeClasses {
    column_to_range_class: Vec<RangeClassEntry>,
}

impl RangeClasses {
    fn add_range_class(
        &mut self,
        op: ComparisonOp,
        col: &ScalarExpr,
        val: &ConstantExpr,
        matcher: &ScalarExprMatcher<'_, '_>,
    ) {
        let (lower_bound, upper_bound) = match op {
            ComparisonOp::Equal => (
                BoundValue::Closed(val.value.clone()),
                BoundValue::Closed(val.value.clone()),
            ),
            ComparisonOp::LT => (
                BoundValue::NegativeInfinite,
                BoundValue::Open(val.value.clone()),
            ),
            ComparisonOp::LTE => (
                BoundValue::NegativeInfinite,
                BoundValue::Closed(val.value.clone()),
            ),
            ComparisonOp::GT => (
                BoundValue::Open(val.value.clone()),
                BoundValue::PositiveInfinite,
            ),
            ComparisonOp::GTE => (
                BoundValue::Closed(val.value.clone()),
                BoundValue::PositiveInfinite,
            ),
            _ => unreachable!(),
        };
        if let Some(range_entry) = self
            .column_to_range_class
            .iter_mut()
            .find(|entry| matcher.expr_equal(&entry.column, col))
        {
            range_entry.values.insert(lower_bound, upper_bound);
            return;
        }
        let mut range_values = RangeValues::new();
        range_values.insert(lower_bound, upper_bound);
        self.column_to_range_class.push(RangeClassEntry {
            column: col.clone(),
            values: range_values,
        });
    }

    pub(super) fn check(
        &self,
        view_range_classes: &RangeClasses,
        query_info: &QueryInfo,
        view_info: &QueryInfo,
    ) -> (bool, Option<Vec<CompensatingRange>>) {
        let mut extra_ranges = Vec::new();
        let query_match_context = ScalarMatchContext {
            column_map: &query_info.column_map,
            column_identities: &query_info.column_identities,
        };
        let view_match_context = ScalarMatchContext {
            column_map: &view_info.column_map,
            column_identities: &view_info.column_identities,
        };
        let matcher = ScalarExprMatcher {
            left: query_match_context,
            right: view_match_context,
        };
        let reverse_matcher = ScalarExprMatcher {
            left: view_match_context,
            right: query_match_context,
        };
        for range_entry in self.column_to_range_class.iter() {
            if !view_range_classes
                .column_to_range_class
                .iter()
                .any(|entry| reverse_matcher.expr_equal(&entry.column, &range_entry.column))
            {
                if let Some((query_lower_bound, query_upper_bound)) = &range_entry.values.bounds {
                    extra_ranges.push(CompensatingRange {
                        column: range_entry.column.clone(),
                        lower_bound: query_lower_bound.clone(),
                        upper_bound: query_upper_bound.clone(),
                    });
                }
            }
        }

        for view_range_entry in view_range_classes.column_to_range_class.iter() {
            if let Some(query_range_entry) = self
                .column_to_range_class
                .iter()
                .find(|entry| matcher.expr_equal(&entry.column, &view_range_entry.column))
            {
                match (
                    &query_range_entry.values.bounds,
                    &view_range_entry.values.bounds,
                ) {
                    (
                        Some((query_lower_bound, query_upper_bound)),
                        Some((view_lower_bound, view_upper_bound)),
                    ) => {
                        let lower_res = view_lower_bound.cmp_lower_bound(query_lower_bound);
                        let upper_res = view_upper_bound.cmp_upper_bound(query_upper_bound);

                        match (lower_res, upper_res) {
                            (Ordering::Equal, Ordering::Equal) => continue,
                            (Ordering::Equal, Ordering::Greater) => {
                                extra_ranges.push(CompensatingRange {
                                    column: view_range_entry.column.clone(),
                                    lower_bound: BoundValue::NegativeInfinite,
                                    upper_bound: query_upper_bound.clone(),
                                });
                            }
                            (Ordering::Less, Ordering::Equal) => {
                                extra_ranges.push(CompensatingRange {
                                    column: view_range_entry.column.clone(),
                                    lower_bound: query_lower_bound.clone(),
                                    upper_bound: BoundValue::PositiveInfinite,
                                });
                            }
                            (Ordering::Less, Ordering::Greater) => {
                                extra_ranges.push(CompensatingRange {
                                    column: view_range_entry.column.clone(),
                                    lower_bound: query_lower_bound.clone(),
                                    upper_bound: query_upper_bound.clone(),
                                });
                            }
                            (_, _) => return (false, None),
                        }
                    }
                    (Some((query_lower_bound, query_upper_bound)), None) => {
                        extra_ranges.push(CompensatingRange {
                            column: view_range_entry.column.clone(),
                            lower_bound: query_lower_bound.clone(),
                            upper_bound: query_upper_bound.clone(),
                        });
                    }
                    (_, _) => return (false, None),
                }
            } else {
                return (false, None);
            }
        }
        if extra_ranges.is_empty() {
            (true, None)
        } else {
            (true, Some(extra_ranges))
        }
    }
}

impl ResidualClasses {
    fn add_residual_pred(&mut self, pred: ScalarExpr, matcher: &ScalarExprMatcher<'_, '_>) {
        if !matcher.list_contains(&self.residual_preds, &pred) {
            self.residual_preds.push(pred);
        }
    }
}

fn to_index_scalar(index: FieldIndex, data_type: &DataType) -> ScalarExpr {
    let col = BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            format!("index_col_{index}"),
            Symbol::from_field_index(index),
            Box::new(data_type.clone()),
            Visibility::Visible,
        )
        .build(),
    };
    ScalarExpr::BoundColumnRef(col)
}

fn actual_column_ref<'a>(
    col: &'a ScalarExpr,
    column_map: &'a HashMap<Symbol, ScalarExpr>,
) -> &'a ScalarExpr {
    if let ScalarExpr::BoundColumnRef(col) = col {
        if let Some(arg) = column_map.get(&col.column.index) {
            return arg;
        }
    }
    col
}

fn register_column_expr(
    column_exprs: &mut Vec<ColumnExprCandidate>,
    expr: ScalarExpr,
    index: Symbol,
    source: ColumnExprSource,
    matcher: &ScalarExprMatcher<'_, '_>,
) {
    if let Some(existing_entry) = column_exprs
        .iter_mut()
        .find(|entry| matcher.expr_equal(&entry.expr, &expr))
    {
        if source.prefer_over(existing_entry.source, index, existing_entry.index) {
            existing_entry.index = index;
            existing_entry.source = source;
        }
        return;
    }

    column_exprs.push(ColumnExprCandidate {
        expr,
        index,
        source,
    });
}

impl<'a, 'b> ScalarExprMatcher<'a, 'b> {
    fn resolve_mapped_scalar<'map>(
        scalar: &ScalarExpr,
        column_map: &'map HashMap<Symbol, ScalarExpr>,
    ) -> Option<&'map ScalarExpr> {
        let ScalarExpr::BoundColumnRef(col) = scalar else {
            return None;
        };

        let mapped = column_map.get(&col.column.index)?;
        match mapped {
            ScalarExpr::BoundColumnRef(mapped_col)
                if mapped_col.column.index == col.column.index
                    && mapped_col.column.table_index == col.column.table_index =>
            {
                None
            }
            _ => Some(mapped),
        }
    }

    fn expr_equal(&self, left: &ScalarExpr, right: &ScalarExpr) -> bool {
        if let Some(mapped_left) = Self::resolve_mapped_scalar(left, self.left.column_map) {
            return self.expr_equal(mapped_left, right);
        }
        if let Some(mapped_right) = Self::resolve_mapped_scalar(right, self.right.column_map) {
            return self.expr_equal(left, mapped_right);
        }

        match (left, right) {
            (ScalarExpr::BoundColumnRef(l), ScalarExpr::BoundColumnRef(r)) => {
                if let (Some(left_identity), Some(right_identity)) = (
                    self.left.column_identities.get(&l.column.index),
                    self.right.column_identities.get(&r.column.index),
                ) {
                    return left_identity == right_identity;
                }
                l.column.table_index == r.column.table_index
                    && l.column.column_name == r.column.column_name
            }
            (ScalarExpr::ConstantExpr(l), ScalarExpr::ConstantExpr(r))
            | (ScalarExpr::ConstantExpr(l), ScalarExpr::TypedConstantExpr(r, _))
            | (ScalarExpr::TypedConstantExpr(l, _), ScalarExpr::ConstantExpr(r))
            | (ScalarExpr::TypedConstantExpr(l, _), ScalarExpr::TypedConstantExpr(r, _)) => {
                l.value == r.value
            }
            (ScalarExpr::FunctionCall(l), ScalarExpr::FunctionCall(r)) => {
                l.func_name == r.func_name
                    && l.params == r.params
                    && l.arguments.len() == r.arguments.len()
                    && l.arguments
                        .iter()
                        .zip(r.arguments.iter())
                        .all(|(l, r)| self.expr_equal(l, r))
            }
            (ScalarExpr::CastExpr(l), ScalarExpr::CastExpr(r)) => {
                l.is_try == r.is_try
                    && l.target_type == r.target_type
                    && self.expr_equal(&l.argument, &r.argument)
            }
            (ScalarExpr::AggregateFunction(l), ScalarExpr::AggregateFunction(r)) => {
                l.func_name == r.func_name
                    && l.distinct == r.distinct
                    && l.params == r.params
                    && l.args.len() == r.args.len()
                    && l.sort_descs.len() == r.sort_descs.len()
                    && l.args
                        .iter()
                        .zip(r.args.iter())
                        .all(|(l, r)| self.expr_equal(l, r))
                    && l.sort_descs.iter().zip(r.sort_descs.iter()).all(|(l, r)| {
                        l.nulls_first == r.nulls_first
                            && l.asc == r.asc
                            && self.expr_equal(&l.expr, &r.expr)
                    })
            }
            (ScalarExpr::UDAFCall(l), ScalarExpr::UDAFCall(r)) => {
                l.name == r.name
                    && l.arguments.len() == r.arguments.len()
                    && l.arguments
                        .iter()
                        .zip(r.arguments.iter())
                        .all(|(l, r)| self.expr_equal(l, r))
            }
            (ScalarExpr::UDFCall(l), ScalarExpr::UDFCall(r)) => {
                l.handler == r.handler
                    && l.arguments.len() == r.arguments.len()
                    && l.arguments
                        .iter()
                        .zip(r.arguments.iter())
                        .all(|(l, r)| self.expr_equal(l, r))
            }
            _ => false,
        }
    }

    pub(super) fn list_contains(&self, list: &[ScalarExpr], target: &ScalarExpr) -> bool {
        list.iter().any(|expr| self.expr_equal(expr, target))
    }

    pub(super) fn find_column_index(
        &self,
        column_exprs: &[ColumnExprEntry],
        target: &ScalarExpr,
    ) -> Option<Symbol> {
        column_exprs.iter().find_map(|entry| {
            if self.expr_equal(&entry.expr, target) {
                Some(entry.index)
            } else {
                None
            }
        })
    }

    pub(super) fn find_index_output_col(
        &self,
        index_output_cols: &[IndexOutputColumn],
        target: &ScalarExpr,
    ) -> Option<(ScalarExpr, bool)> {
        index_output_cols.iter().find_map(|entry| {
            if self.expr_equal(&entry.expr, target) {
                Some((entry.index_scalar.clone(), entry.is_agg))
            } else {
                None
            }
        })
    }
}
