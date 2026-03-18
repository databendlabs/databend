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
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FieldIndex;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use log::info;

use crate::AggIndexPlan;
use crate::ColumnEntry;
use crate::IndexType;
use crate::ScalarExpr;
use crate::Symbol;
use crate::Visibility;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ir::SExpr;
use crate::plans::AggIndexInfo;
use crate::plans::Aggregate;
use crate::plans::BoundColumnRef;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::SortItem;

pub fn try_rewrite(
    table_index: IndexType,
    table_name: &str,
    base_columns: &[ColumnEntry],
    s_expr: &SExpr,
    index_plans: &[AggIndexPlan],
) -> Result<Option<SExpr>> {
    if index_plans.is_empty() {
        return Ok(None);
    }

    let query_info = QueryInfo::new(table_index, table_name, base_columns, s_expr)?;
    let agg_index_rewriter = AggIndexRewriter { query_info };

    for index_plan in index_plans {
        let view_info = ViewInfo::new(table_index, table_name, base_columns, &index_plan.s_expr)?;
        if let Some(result) = agg_index_rewriter.try_rewrite_index(
            s_expr,
            index_plan.index_id,
            &index_plan.sql,
            &view_info,
        )? {
            return Ok(Some(result));
        }
    }

    Ok(None)
}

struct QueryInfo {
    equi_classes: EquivalenceClasses,
    range_classes: RangeClasses,
    residual_classes: ResidualClasses,
    sort_items: Option<Vec<SortItem>>,
    aggregate: Option<Aggregate>,
    column_map: HashMap<Symbol, ScalarExpr>,
    column_exprs: Vec<ColumnExprEntry>,
    output_cols: Vec<ScalarItem>,
}

struct ColumnExprEntry {
    expr: ScalarExpr,
    index: Symbol,
    source: ColumnExprSource,
}

struct ColumnExprSourceEntry {
    index: Symbol,
    source: ColumnExprSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ColumnExprSource {
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

struct IndexOutputColumn {
    expr: ScalarExpr,
    index_scalar: ScalarExpr,
    is_agg: bool,
}

struct EqualityPredicate {
    left: ScalarExpr,
    right: ScalarExpr,
}

struct RangePredicate {
    op: ComparisonOp,
    column: ScalarExpr,
    value: ConstantExpr,
}

struct RangeClassEntry {
    column: ScalarExpr,
    values: RangeValues,
}

struct CompensatingRange {
    column: ScalarExpr,
    lower_bound: BoundValue,
    upper_bound: BoundValue,
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

        // collect query info from the plan
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
                    // Finish the recursion.
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

        // split predicates as equal predicate, range predicate, and residual predicate.
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
        for arg in args {
            let Some(new_arg) = self.check_output_cols(
                arg,
                index_output_cols,
                index_column_map,
                new_selection_set,
            )?
            else {
                return Ok(None);
            };
            rewritten.push(new_arg);
        }
        Ok(Some(rewritten))
    }

    // check whether the scalar can be computed from index output columns.
    // if not, the aggregating index can't be used.
    fn check_output_cols(
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
            // agg function can't used
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
                // agg function can't push down
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
            _ => unreachable!(), // Window function and subquery will not appear in index.
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

// Record information of aggregating index plan.
struct ViewInfo {
    query_info: QueryInfo,
    index_fields: Vec<TableField>,
    index_output_cols: Vec<IndexOutputColumn>,
}

impl ViewInfo {
    fn new(
        table_index: IndexType,
        table_name: &str,
        base_columns: &[ColumnEntry],
        s_expr: &SExpr,
    ) -> Result<ViewInfo> {
        let query_info = QueryInfo::new(table_index, table_name, base_columns, s_expr)?;

        // collect the output columns of aggregating index,
        // query can use those columns to compute expressions.
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

#[derive(Default)]
struct EquivalenceClasses {
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

    // Equijoin subsumption test.
    fn check(
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
enum BoundValue {
    // column >= scalar value or column <= scalar value
    Closed(Scalar),
    // column > scalar value or column < scalar value
    Open(Scalar),
    // -∞
    NegativeInfinite,
    // +∞
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

#[derive(Debug)]
struct RangeValues {
    bounds: Option<(BoundValue, BoundValue)>,
}

// if a column have more than one predicates, we can merge them together to simplify the process.
//
//                 +------+
//                 | orig |
//                 +------+
//        +-----+
// case 1 | new |
//        +-----+
// upper < orig_lower
//                            +-----+
// case 2                     | new |
//                            +-----+
// lower > orig_upper
//                  +-----+
// case 3           | new |
//                  +-----+
// lower >= orig_lower && upper <= orig_upper
//               +-----------+
// case 4        |    new    |
//               +-----------+
// lower < orig_lower upper > orig_upper
//             +-----+
// case 5      | new |
//             +-----+
// upper >= orig_lower && upper <= orig_upper && lower <= orig_lower
//                       +-----+
// case 6                | new |
//                       +-----+
// lower >= orig_lower && lower <= orig_upper && upper >= orig_upper
impl RangeValues {
    fn new() -> Self {
        Self {
            bounds: Some((BoundValue::NegativeInfinite, BoundValue::PositiveInfinite)),
        }
    }

    fn insert(&mut self, lower_bound: BoundValue, upper_bound: BoundValue) {
        if let Some((orig_lower_bound, orig_upper_bound)) = &self.bounds {
            // case 1 and case 2
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
                // case 3
                (Ordering::Greater | Ordering::Equal, Ordering::Less | Ordering::Equal) => {
                    self.bounds = Some((lower_bound, upper_bound))
                }
                // case 4
                (Ordering::Less, Ordering::Greater) => {}
                (Ordering::Less, Ordering::Less | Ordering::Equal) => {
                    // case 5
                    if matches!(
                        upper_bound.cmp(orig_lower_bound),
                        Ordering::Greater | Ordering::Equal
                    ) {
                        self.bounds = Some((orig_lower_bound.clone(), upper_bound));
                    }
                }
                (Ordering::Greater | Ordering::Equal, Ordering::Greater) => {
                    // case 6
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

#[derive(Default)]
struct RangeClasses {
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

    // Range subsumption test.
    #[allow(clippy::type_complexity)]
    fn check(
        &self,
        view_range_classes: &RangeClasses,
        query_column_map: &HashMap<Symbol, ScalarExpr>,
        view_column_map: &HashMap<Symbol, ScalarExpr>,
    ) -> (bool, Option<Vec<CompensatingRange>>) {
        // if the range predicate in aggregating index and the query have three cases.
        // 1. the range of aggregating index and query are same, don't need extra filter ranges.
        // 2. the range of aggregating index filter less values than the query,
        //    we can add extra range predicate to implement the filter.
        //    for example: aggregating index: a > 10 and query: a > 15
        //    we can add extra range as a > 15
        // 3. the range of aggregating index filter more values than the query,
        //    this aggregating index don't match the query.
        //    for example: aggregating index: a > 10 and query: a > 5
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
                            (Ordering::Equal, Ordering::Equal) => {
                                continue;
                            }
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
                            (_, _) => {
                                return (false, None);
                            }
                        }
                    }
                    (Some((query_lower_bound, query_upper_bound)), None) => {
                        extra_ranges.push(CompensatingRange {
                            column: view_range_entry.column.clone(),
                            lower_bound: query_lower_bound.clone(),
                            upper_bound: query_upper_bound.clone(),
                        });
                    }
                    (_, _) => {
                        return (false, None);
                    }
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
struct ResidualClasses {
    residual_preds: Vec<ScalarExpr>,
}

impl ResidualClasses {
    fn add_residual_pred(&mut self, pred: ScalarExpr, matcher: &ScalarExprMatcher<'_, '_>) {
        if !matcher.list_contains(&self.residual_preds, &pred) {
            self.residual_preds.push(pred);
        }
    }

    // Residual subsumption test.
    fn check(
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
        // TODO: continue split residual predicates and check
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

// Aggregating index rewriting logic is based on "Optimizing Queries Using Materialized Views:
// A Practical, Scalable Solution" by Goldstein and Larson."
struct AggIndexRewriter {
    query_info: QueryInfo,
}

impl AggIndexRewriter {
    fn try_rewrite_index(
        &self,
        s_expr: &SExpr,
        index_id: u64,
        sql: &str,
        view_info: &ViewInfo,
    ) -> Result<Option<SExpr>> {
        let mut new_predicates = Vec::new();
        let mut new_selection_set = HashSet::new();

        if !self.check_predicates(view_info, &mut new_predicates, &mut new_selection_set) {
            return Ok(None);
        }

        if !self.check_output_expressions(view_info, &mut new_selection_set) {
            return Ok(None);
        }

        if !self.check_aggregation(view_info, &mut new_selection_set) {
            return Ok(None);
        }

        if !self.check_sort_items(view_info, &mut new_selection_set) {
            return Ok(None);
        }

        let mut new_selection: Vec<_> = new_selection_set.into_iter().collect();
        new_selection.sort_by_key(|i| i.index);

        let is_agg = self.query_info.aggregate.is_some();
        let num_agg_funcs = self
            .query_info
            .aggregate
            .as_ref()
            .map(|agg| agg.aggregate_functions.len())
            .unwrap_or_default();

        let result = push_down_index_scan(s_expr, AggIndexInfo {
            index_id,
            selection: new_selection,
            predicates: new_predicates,
            schema: TableSchemaRefExt::create(view_info.index_fields.clone()),
            is_agg,
            num_agg_funcs,
        })?;

        info!("Use aggregating index: {sql}");

        Ok(Some(result))
    }

    fn add_compensating_range_predicates(
        &self,
        view_info: &ViewInfo,
        extra_ranges: &[CompensatingRange],
        new_predicates: &mut Vec<ScalarExpr>,
    ) -> bool {
        let index_output_matcher = ScalarExprMatcher::new(
            &view_info.query_info.column_map,
            &self.query_info.column_map,
        );

        for extra_range in extra_ranges {
            let Some((new_scalar, _)) = index_output_matcher
                .find_index_output_col(&view_info.index_output_cols, &extra_range.column)
            else {
                return false;
            };

            let lower = extra_range.lower_bound.comparison_bound("gte", "gt");
            let upper = extra_range.upper_bound.comparison_bound("lte", "lt");

            if let (Some((lower_val, "gte")), Some((upper_val, "lte"))) = (&lower, &upper) {
                if lower_val == upper_val {
                    new_predicates.push(comparison_predicate(
                        "eq",
                        new_scalar.clone(),
                        lower_val.clone(),
                    ));
                    continue;
                }
            }

            push_optional_comparison_predicate(new_predicates, &new_scalar, lower);
            push_optional_comparison_predicate(new_predicates, &new_scalar, upper);
        }

        true
    }

    fn check_output_scalars<'a>(
        &self,
        scalars: impl IntoIterator<Item = &'a ScalarExpr>,
        view_info: &ViewInfo,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        for scalar in scalars {
            if self
                .query_info
                .check_output_cols(
                    scalar,
                    &view_info.index_output_cols,
                    &view_info.query_info.column_map,
                    new_selection_set,
                )
                .is_err()
            {
                return false;
            }
        }

        true
    }

    fn add_missing_residual_predicates(
        &self,
        view_info: &ViewInfo,
        extra_residual_preds: &[ScalarExpr],
        new_predicates: &mut Vec<ScalarExpr>,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        for extra_residual_pred in extra_residual_preds {
            match self.query_info.check_output_cols(
                extra_residual_pred,
                &view_info.index_output_cols,
                &view_info.query_info.column_map,
                new_selection_set,
            ) {
                Ok(Some(new_residual_pred)) => new_predicates.push(new_residual_pred),
                Ok(None) => {}
                Err(_) => return false,
            }
        }

        true
    }

    fn query_group_items(&self) -> &[ScalarItem] {
        self.query_info
            .aggregate
            .as_ref()
            .map(|agg| agg.group_items.as_slice())
            .unwrap_or_default()
    }

    fn view_group_items<'a>(&self, view_info: &'a ViewInfo) -> &'a [ScalarItem] {
        view_info
            .query_info
            .aggregate
            .as_ref()
            .map(|agg| agg.group_items.as_slice())
            .unwrap_or_default()
    }

    fn group_items_match(&self, view_info: &ViewInfo) -> bool {
        let query_group_items = self.query_group_items();
        let view_group_items = self.view_group_items(view_info);
        if query_group_items.len() != view_group_items.len() {
            return false;
        }

        let query_group_matcher = ScalarExprMatcher::same(&self.query_info.column_map);
        let mut query_group_names = Vec::with_capacity(query_group_items.len());
        for item in query_group_items {
            query_group_matcher.push_unique_scalar(&mut query_group_names, item.scalar.clone());
        }

        let view_group_matcher = ScalarExprMatcher::same(&view_info.query_info.column_map);
        let mut view_group_names = Vec::with_capacity(view_group_items.len());
        for item in view_group_items {
            view_group_matcher.push_unique_scalar(&mut view_group_names, item.scalar.clone());
        }

        let group_matcher = ScalarExprMatcher::new(
            &view_info.query_info.column_map,
            &self.query_info.column_map,
        );
        query_group_names.into_iter().all(|query_group_name| {
            group_matcher.list_contains(&view_group_names, &query_group_name)
        })
    }

    fn check_group_items_output_cols(
        &self,
        group_items: &[ScalarItem],
        view_info: &ViewInfo,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        self.check_output_scalars(
            group_items.iter().map(|item| &item.scalar),
            view_info,
            new_selection_set,
        )
    }

    fn check_predicates(
        &self,
        view_info: &ViewInfo,
        new_predicates: &mut Vec<ScalarExpr>,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        // 3.1.2 Do all required rows exist in the view?
        // 1. Compute equivalence classes for the query and the view.
        // 2. Check that every view equivalence class is a subset of a
        //    query equivalence class. If not, reject the view
        // 3. Compute range intervals for the query and the view.
        // 4. Check that every view range contains the corresponding
        //    query range. If not, reject the view.
        // 5. Check that every conjunct in the residual predicate of the
        //    view matches a conjunct in the residual predicate of the query.
        //    If not, reject the view.
        if !self.query_info.equi_classes.check(
            &view_info.query_info.equi_classes,
            &self.query_info.column_map,
            &view_info.query_info.column_map,
        ) {
            return false;
        }
        let (range_res, extra_ranges) = self.query_info.range_classes.check(
            &view_info.query_info.range_classes,
            &self.query_info.column_map,
            &view_info.query_info.column_map,
        );
        if !range_res {
            return false;
        }

        let (residual_res, extra_residual_preds) = self.query_info.residual_classes.check(
            &view_info.query_info.residual_classes,
            &self.query_info.column_map,
            &view_info.query_info.column_map,
        );
        if !residual_res {
            return false;
        }

        // 3.1.3 Can the required rows be selected?
        // 1. Construct compensating column equality predicates
        //    while comparing view equivalence classes against query equivalence classes as described in the previous section.
        //    Try to map every column reference to an output column (using the view equivalence classes).
        //    If this is not possible, reject the view.
        // 2. Construct compensating range predicates by comparing column ranges as described in the previous section.
        //    Try to map every column reference to an output column (using the query equivalence classes).
        //    If this is not possible, reject the view.
        // 3. Find the residual predicates of the query that are missing in the view.
        //    Try to map every column reference to an output column (using the query equivalence classes).
        //    If this is not possible, reject the view.
        if let Some(extra_ranges) = extra_ranges
            && !self.add_compensating_range_predicates(view_info, &extra_ranges, new_predicates)
        {
            return false;
        }

        if let Some(extra_residual_preds) = extra_residual_preds
            && !self.add_missing_residual_predicates(
                view_info,
                &extra_residual_preds,
                new_predicates,
                new_selection_set,
            )
        {
            return false;
        }

        true
    }

    fn check_output_expressions(
        &self,
        view_info: &ViewInfo,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        // 3.1.4 Can output expressions be computed?
        // Checking whether all output expressions of the query can be computed from the view
        // is similar to checking whether the additional predicates can be computed correctly.
        self.check_output_scalars(
            self.query_info.output_cols.iter().map(|item| &item.scalar),
            view_info,
            new_selection_set,
        )
    }

    fn check_aggregation(
        &self,
        view_info: &ViewInfo,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        // 3.3 Aggregation queries and views
        // 1. The SPJ part of the view produces all rows needed by
        //    the SPJ part of the query and with the right duplication factor.
        // 2. All columns required by compensating predicates (if any) are available in the view output.
        // 3. The view contains no aggregation or is less aggregated than the query,
        //    i.e, the groups formed by the query can be computed by further aggregation of groups output by the view.
        // 4. All columns required to perform further grouping (if necessary) are available in the view output.
        // 5. All columns required to compute output expressions are available in the view output.

        let query_group_items = self.query_group_items();
        let view_group_items = self.view_group_items(view_info);

        match (query_group_items.is_empty(), view_group_items.is_empty()) {
            // both query and view have group, check for same group items.
            (false, false) => {
                // TODO: query can support continue group
                if !self.group_items_match(view_info) {
                    return false;
                }
                if !self.check_group_items_output_cols(
                    query_group_items,
                    view_info,
                    new_selection_set,
                ) {
                    return false;
                }
            }
            // query have group, but view don't have group,
            // check group items in output rows.
            (false, true) => {
                if !self.check_group_items_output_cols(
                    query_group_items,
                    view_info,
                    new_selection_set,
                ) {
                    return false;
                }
            }
            // both query and view don't have group, don't need check.
            (true, true) => {}
            // query don't have group, but view have group, impossible to match.
            (true, false) => {
                return false;
            }
        }

        true
    }

    fn check_sort_items(
        &self,
        view_info: &ViewInfo,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> bool {
        let Some(sort_items) = &self.query_info.sort_items else {
            return true;
        };

        for item in sort_items {
            let Some(scalar) = self.query_info.column_map.get(&item.index) else {
                return false;
            };

            if !self.check_output_scalars([scalar], view_info, new_selection_set) {
                return false;
            }
        }

        true
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

// replace derived column with actual ScalarExpr.
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

fn comparison_predicate(func_name: &str, column: ScalarExpr, value: Scalar) -> ScalarExpr {
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: func_name.to_string(),
        params: vec![],
        arguments: vec![
            column,
            ScalarExpr::ConstantExpr(ConstantExpr { span: None, value }),
        ],
    })
}

fn push_optional_comparison_predicate(
    predicates: &mut Vec<ScalarExpr>,
    column: &ScalarExpr,
    bound: Option<(Scalar, &'static str)>,
) {
    let Some((value, func_name)) = bound else {
        return;
    };
    predicates.push(comparison_predicate(func_name, column.clone(), value));
}

struct ScalarExprMatcher<'a, 'b> {
    left_column_map: &'a HashMap<Symbol, ScalarExpr>,
    right_column_map: &'b HashMap<Symbol, ScalarExpr>,
}

impl<'a, 'b> ScalarExprMatcher<'a, 'b> {
    fn new(
        left_column_map: &'a HashMap<Symbol, ScalarExpr>,
        right_column_map: &'b HashMap<Symbol, ScalarExpr>,
    ) -> Self {
        Self {
            left_column_map,
            right_column_map,
        }
    }

    fn same(column_map: &'a HashMap<Symbol, ScalarExpr>) -> ScalarExprMatcher<'a, 'a> {
        ScalarExprMatcher::new(column_map, column_map)
    }

    fn resolve_left(&self, scalar: &ScalarExpr) -> Option<&'a ScalarExpr> {
        Self::resolve_scalar(scalar, self.left_column_map)
    }

    fn resolve_right(&self, scalar: &ScalarExpr) -> Option<&'b ScalarExpr> {
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
                l.column.index == r.column.index && l.column.table_index == r.column.table_index
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

    fn list_contains(&self, list: &[ScalarExpr], target: &ScalarExpr) -> bool {
        list.iter().any(|expr| self.expr_equal(expr, target))
    }

    fn push_unique_scalar(&self, list: &mut Vec<ScalarExpr>, scalar: ScalarExpr) {
        if !self.list_contains(list, &scalar) {
            list.push(scalar);
        }
    }

    fn find_column_index(
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

    fn find_index_output_col(
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

fn push_down_index_scan(s_expr: &SExpr, agg_info: AggIndexInfo) -> Result<SExpr> {
    Ok(match s_expr.plan() {
        RelOperator::Scan(scan) => {
            let mut new_scan = scan.clone();
            new_scan.agg_index = Some(agg_info);
            s_expr.replace_plan(Arc::new(new_scan.into()))
        }
        _ => {
            let child = push_down_index_scan(s_expr.child(0)?, agg_info)?;
            s_expr.replace_children(vec![Arc::new(child)])
        }
    })
}
