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
use std::collections::HashSet;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FieldIndex;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;

use super::rewrite::AggIndexMatcher;
use crate::ColumnEntry;
use crate::IndexType;
use crate::ScalarExpr;
use crate::Symbol;
use crate::Visibility;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ir::SExpr;
use crate::plans::Aggregate;
use crate::plans::BoundColumnRef;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::SortItem;

pub(super) struct PreparedAggIndexQuery {
    info: QueryInfo,
}

impl PreparedAggIndexQuery {
    pub(super) fn prepare(
        table_index: IndexType,
        table_name: &str,
        base_columns: &[ColumnEntry],
        s_expr: &SExpr,
    ) -> Result<Self> {
        Ok(Self {
            info: QueryInfo::new(table_index, table_name, base_columns, s_expr)?,
        })
    }

    pub(super) fn matcher(&self) -> AggIndexMatcher<'_> {
        AggIndexMatcher {
            query_info: &self.info,
        }
    }
}

#[derive(Debug)]
pub(super) struct QueryInfo {
    pub(super) equi_classes: EquivalenceClasses,
    pub(super) range_classes: RangeClasses,
    pub(super) residual_classes: ResidualClasses,
    pub(super) sort_items: Option<Vec<SortItem>>,
    pub(super) aggregate: Option<Aggregate>,
    pub(super) column_map: HashMap<Symbol, ScalarExpr>,
    pub(super) column_exprs: Vec<ColumnExprEntry>,
    pub(super) output_cols: Vec<ScalarItem>,
}

#[derive(Debug)]
pub(super) struct ColumnExprEntry {
    pub(super) expr: ScalarExpr,
    pub(super) index: Symbol,
    pub(super) source: ColumnExprSource,
}

struct ColumnExprSourceEntry {
    index: Symbol,
    source: ColumnExprSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ColumnExprSource {
    order: usize,
}

impl ColumnExprSource {
    fn new(order: usize) -> Self {
        Self { order }
    }

    fn prefer_over(self, other: Self, index: Symbol, other_index: Symbol) -> bool {
        match self.order.cmp(&other.order) {
            Ordering::Less => false,
            Ordering::Greater => true,
            Ordering::Equal => index < other_index,
        }
    }
}

#[derive(Debug)]
pub(super) struct IndexOutputColumn {
    pub(super) expr: ScalarExpr,
    pub(super) index_scalar: ScalarExpr,
    pub(super) is_agg: bool,
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

#[derive(Debug)]
pub(super) struct CompensatingRange {
    pub(super) column: ScalarExpr,
    pub(super) lower_bound: BoundValue,
    pub(super) upper_bound: BoundValue,
}

impl QueryInfo {
    fn new(
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
        let mut column_expr_sources = Vec::new();
        let mut next_source_order = 0usize;

        let mut record_column_source = |index: Symbol, scalar: ScalarExpr| {
            column_map.insert(index, scalar);
            column_expr_sources.push(ColumnExprSourceEntry {
                index,
                source: ColumnExprSource::new(next_source_order),
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

        let mut column_exprs = Vec::new();
        let column_matcher = ScalarExprMatcher::same(&column_map);
        for (scalar, source) in column_expr_sources
            .iter()
            .filter_map(|source| column_map.get(&source.index).map(|scalar| (scalar, source)))
        {
            register_column_expr(
                &mut column_exprs,
                scalar.clone(),
                source.index,
                source.source,
                &column_matcher,
            );
        }

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
            column_exprs,
            output_cols,
        })
    }

    fn rewrite_output_args(
        &self,
        args: &[ScalarExpr],
        index_output_cols: &[IndexOutputColumn],
        index_column_map: &HashMap<Symbol, ScalarExpr>,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> Result<Option<Vec<ScalarExpr>>> {
        let mut rewritten = Vec::with_capacity(args.len());
        let mut all_rewritten = true;
        for arg in args {
            let new_arg = self.check_output_cols(
                arg,
                index_output_cols,
                index_column_map,
                new_selection_set,
            )?;
            if let Some(new_arg) = new_arg {
                rewritten.push(new_arg);
            } else {
                all_rewritten = false;
            }
        }
        if all_rewritten {
            Ok(Some(rewritten))
        } else {
            Ok(None)
        }
    }

    pub(super) fn check_output_cols(
        &self,
        scalar: &ScalarExpr,
        index_output_cols: &[IndexOutputColumn],
        index_column_map: &HashMap<Symbol, ScalarExpr>,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> Result<Option<ScalarExpr>> {
        let output_matcher = ScalarExprMatcher::new(index_column_map, &self.column_map);
        let query_matcher = ScalarExprMatcher::same(&self.column_map);

        if let Some((new_scalar, is_agg)) =
            output_matcher.find_index_output_col(index_output_cols, scalar)
        {
            if let Some(index) = query_matcher.find_column_index(&self.column_exprs, scalar) {
                let new_item = ScalarItem {
                    index,
                    scalar: new_scalar.clone(),
                };
                new_selection_set.insert(new_item);
            }
            return if is_agg {
                Ok(None)
            } else {
                Ok(Some(new_scalar.clone()))
            };
        }

        let new_scalar = match scalar {
            ScalarExpr::BoundColumnRef(_) => {
                if let Some(actual_column) = query_matcher.resolve_right(scalar) {
                    return self.check_output_cols(
                        actual_column,
                        index_output_cols,
                        index_column_map,
                        new_selection_set,
                    );
                }
                return Err(ErrorCode::Internal("Can't found column from index"));
            }
            ScalarExpr::ConstantExpr(_) => scalar.clone(),
            ScalarExpr::FunctionCall(func) => {
                let Some(new_args) = self.rewrite_output_args(
                    &func.arguments,
                    index_output_cols,
                    index_column_map,
                    new_selection_set,
                )?
                else {
                    return Ok(None);
                };
                let mut new_func = func.clone();
                new_func.arguments = new_args;
                ScalarExpr::FunctionCall(new_func)
            }
            ScalarExpr::CastExpr(cast) => {
                if let Some(new_arg) = self.check_output_cols(
                    &cast.argument,
                    index_output_cols,
                    index_column_map,
                    new_selection_set,
                )? {
                    let mut new_cast = cast.clone();
                    new_cast.argument = Box::new(new_arg);
                    ScalarExpr::CastExpr(new_cast)
                } else {
                    return Ok(None);
                }
            }
            ScalarExpr::AggregateFunction(func) => {
                for expr in func.exprs() {
                    self.check_output_cols(
                        expr,
                        index_output_cols,
                        index_column_map,
                        new_selection_set,
                    )?;
                }
                return Ok(None);
            }
            ScalarExpr::UDAFCall(udaf) => {
                for arg in &udaf.arguments {
                    self.check_output_cols(
                        arg,
                        index_output_cols,
                        index_column_map,
                        new_selection_set,
                    )?;
                }
                return Ok(None);
            }
            ScalarExpr::UDFCall(udf) => {
                let Some(new_args) = self.rewrite_output_args(
                    &udf.arguments,
                    index_output_cols,
                    index_column_map,
                    new_selection_set,
                )?
                else {
                    return Ok(None);
                };
                let mut new_udf = udf.clone();
                new_udf.arguments = new_args;
                ScalarExpr::UDFCall(new_udf)
            }
            _ => unreachable!(),
        };

        if let Some(index) = query_matcher.find_column_index(&self.column_exprs, scalar) {
            let new_item = ScalarItem {
                index,
                scalar: new_scalar.clone(),
            };
            new_selection_set.insert(new_item);
            return Ok(None);
        }

        Ok(Some(new_scalar))
    }
}

#[derive(Debug)]
pub struct AggIndexViewInfo {
    pub(super) query_info: QueryInfo,
    pub(super) index_fields: Vec<TableField>,
    pub(super) index_output_cols: Vec<IndexOutputColumn>,
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

#[derive(Debug, Default)]
pub(super) struct EquivalenceClasses {
    classes: Vec<Vec<ScalarExpr>>,
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
                    matcher.push_unique_scalar(&mut merged, scalar);
                }
            } else {
                idx += 1;
            }
        }
        self.classes.push(merged);
    }

    pub(super) fn check(
        &self,
        view_equi_classes: &EquivalenceClasses,
        query_column_map: &HashMap<Symbol, ScalarExpr>,
        view_column_map: &HashMap<Symbol, ScalarExpr>,
    ) -> bool {
        let matcher = ScalarExprMatcher::new(query_column_map, view_column_map);
        for view_class in &view_equi_classes.classes {
            if self.classes.iter().any(|query_class| {
                view_class
                    .iter()
                    .all(|view_scalar| matcher.list_contains(query_class, view_scalar))
            }) {
                continue;
            }
            return false;
        }
        true
    }
}

#[derive(Eq, Clone, Debug)]
pub(super) enum BoundValue {
    Closed(Scalar),
    Open(Scalar),
    NegativeInfinite,
    PositiveInfinite,
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for BoundValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
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

        match (self, other) {
            (BoundValue::NegativeInfinite, BoundValue::NegativeInfinite) => Some(Ordering::Equal),
            (BoundValue::PositiveInfinite, BoundValue::PositiveInfinite) => Some(Ordering::Equal),
            (BoundValue::NegativeInfinite, _) => Some(Ordering::Less),
            (_, BoundValue::NegativeInfinite) => Some(Ordering::Greater),
            (BoundValue::PositiveInfinite, _) => Some(Ordering::Greater),
            (_, BoundValue::PositiveInfinite) => Some(Ordering::Less),
            (BoundValue::Open(v1), BoundValue::Open(v2)) => Some(cmp_scalar(v1, v2)),
            (BoundValue::Closed(v1), BoundValue::Closed(v2)) => Some(cmp_scalar(v1, v2)),
            (BoundValue::Open(v1), BoundValue::Closed(v2)) => {
                let res = cmp_scalar(v1, v2);
                if res == Ordering::Equal {
                    Some(Ordering::Less)
                } else {
                    Some(res)
                }
            }
            (BoundValue::Closed(v1), BoundValue::Open(v2)) => {
                let res = cmp_scalar(v1, v2);
                if res == Ordering::Equal {
                    Some(Ordering::Greater)
                } else {
                    Some(res)
                }
            }
        }
    }
}

impl Ord for BoundValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl PartialEq for BoundValue {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl BoundValue {
    pub(super) fn comparison_bound(
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

#[derive(Debug)]
struct RangeValues {
    bounds: Option<(BoundValue, BoundValue)>,
}

impl RangeValues {
    fn new() -> Self {
        Self {
            bounds: Some((BoundValue::NegativeInfinite, BoundValue::PositiveInfinite)),
        }
    }

    fn insert(&mut self, lower_bound: BoundValue, upper_bound: BoundValue) {
        if let Some((orig_lower_bound, orig_upper_bound)) = &self.bounds {
            if upper_bound.cmp(orig_lower_bound) == Ordering::Less
                || lower_bound.cmp(orig_upper_bound) == Ordering::Greater
            {
                self.bounds = None;
                return;
            }
            match (
                lower_bound.cmp(orig_lower_bound),
                upper_bound.cmp(orig_upper_bound),
            ) {
                (Ordering::Greater | Ordering::Equal, Ordering::Less | Ordering::Equal) => {
                    self.bounds = Some((lower_bound, upper_bound))
                }
                (Ordering::Less, Ordering::Greater) => {}
                (Ordering::Less, Ordering::Less | Ordering::Equal) => {
                    if matches!(
                        upper_bound.cmp(orig_lower_bound),
                        Ordering::Greater | Ordering::Equal
                    ) {
                        self.bounds = Some((orig_lower_bound.clone(), upper_bound));
                    }
                }
                (Ordering::Greater | Ordering::Equal, Ordering::Greater) => {
                    if matches!(
                        lower_bound.cmp(orig_upper_bound),
                        Ordering::Less | Ordering::Equal
                    ) {
                        self.bounds = Some((lower_bound, orig_upper_bound.clone()));
                    }
                }
            }
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
        query_column_map: &HashMap<Symbol, ScalarExpr>,
        view_column_map: &HashMap<Symbol, ScalarExpr>,
    ) -> (bool, Option<Vec<CompensatingRange>>) {
        let mut extra_ranges = Vec::new();
        let matcher = ScalarExprMatcher::new(query_column_map, view_column_map);
        let reverse_matcher = ScalarExprMatcher::new(view_column_map, query_column_map);
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
                        let lower_res = view_lower_bound.cmp(query_lower_bound);
                        let upper_res = view_upper_bound.cmp(query_upper_bound);

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

#[derive(Debug, Default)]
pub(super) struct ResidualClasses {
    residual_preds: Vec<ScalarExpr>,
}

impl ResidualClasses {
    fn add_residual_pred(&mut self, pred: ScalarExpr, matcher: &ScalarExprMatcher<'_, '_>) {
        if !matcher.list_contains(&self.residual_preds, &pred) {
            self.residual_preds.push(pred);
        }
    }

    pub(super) fn check(
        &self,
        view_residual_classes: &ResidualClasses,
        query_column_map: &HashMap<Symbol, ScalarExpr>,
        view_column_map: &HashMap<Symbol, ScalarExpr>,
    ) -> (bool, Option<Vec<ScalarExpr>>) {
        let matcher = ScalarExprMatcher::new(query_column_map, view_column_map);
        let reverse_matcher = ScalarExprMatcher::new(view_column_map, query_column_map);
        let mut extra_residual_preds = Vec::new();
        for view_residual_pred in &view_residual_classes.residual_preds {
            if !matcher.list_contains(&self.residual_preds, view_residual_pred) {
                return (false, None);
            }
        }
        for query_residual_pred in &self.residual_preds {
            if !reverse_matcher
                .list_contains(&view_residual_classes.residual_preds, query_residual_pred)
            {
                extra_residual_preds.push(query_residual_pred.clone());
            }
        }
        if extra_residual_preds.is_empty() {
            (true, None)
        } else {
            (true, Some(extra_residual_preds))
        }
    }
}

pub(super) fn to_index_scalar(index: FieldIndex, data_type: &DataType) -> ScalarExpr {
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
    column_exprs: &mut Vec<ColumnExprEntry>,
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

    column_exprs.push(ColumnExprEntry {
        expr,
        index,
        source,
    });
}

pub(super) struct ScalarExprMatcher<'a, 'b> {
    left_column_map: &'a HashMap<Symbol, ScalarExpr>,
    right_column_map: &'b HashMap<Symbol, ScalarExpr>,
}

impl<'a, 'b> ScalarExprMatcher<'a, 'b> {
    pub(super) fn new(
        left_column_map: &'a HashMap<Symbol, ScalarExpr>,
        right_column_map: &'b HashMap<Symbol, ScalarExpr>,
    ) -> Self {
        Self {
            left_column_map,
            right_column_map,
        }
    }

    pub(super) fn same(column_map: &'a HashMap<Symbol, ScalarExpr>) -> ScalarExprMatcher<'a, 'a> {
        ScalarExprMatcher::new(column_map, column_map)
    }

    fn resolve_left(&self, scalar: &ScalarExpr) -> Option<&'a ScalarExpr> {
        Self::resolve_scalar(scalar, self.left_column_map)
    }

    pub(super) fn resolve_right(&self, scalar: &ScalarExpr) -> Option<&'b ScalarExpr> {
        Self::resolve_scalar(scalar, self.right_column_map)
    }

    fn resolve_scalar<'map>(
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
        if let Some(mapped_left) = self.resolve_left(left) {
            return self.expr_equal(mapped_left, right);
        }
        if let Some(mapped_right) = self.resolve_right(right) {
            return self.expr_equal(left, mapped_right);
        }

        match (left, right) {
            (ScalarExpr::BoundColumnRef(l), ScalarExpr::BoundColumnRef(r)) => {
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

    pub(super) fn push_unique_scalar(&self, list: &mut Vec<ScalarExpr>, scalar: ScalarExpr) {
        if !self.list_contains(list, &scalar) {
            list.push(scalar);
        }
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
