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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use log::info;

use crate::binder::ColumnBindingBuilder;
use crate::optimizer::SExpr;
use crate::plans::AggIndexInfo;
use crate::plans::Aggregate;
use crate::plans::BoundColumnRef;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::SortItem;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::IndexType;
use crate::ScalarExpr;
use crate::Visibility;

pub fn try_rewrite(
    table_index: IndexType,
    table_name: &str,
    base_columns: &[ColumnEntry],
    s_expr: &SExpr,
    index_plans: &[(u64, String, SExpr)],
) -> Result<Option<SExpr>> {
    if index_plans.is_empty() {
        return Ok(None);
    }

    let query_info = QueryInfo::new(table_index, table_name, base_columns, s_expr)?;
    let agg_index_rewriter = AggIndexRewriter::new(query_info);

    for (index_id, sql, view_s_expr) in index_plans.iter() {
        let view_info = ViewInfo::new(table_index, table_name, base_columns, view_s_expr)?;
        if let Some(result) =
            agg_index_rewriter.try_rewrite_index(s_expr, *index_id, sql, &view_info)?
        {
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
    column_map: HashMap<IndexType, ScalarExpr>,
    column_display_map: HashMap<String, IndexType>,
    output_cols: Vec<ScalarItem>,
}

impl QueryInfo {
    fn new(
        table_index: IndexType,
        table_name: &str,
        base_columns: &[ColumnEntry],
        s_expr: &SExpr,
    ) -> Result<QueryInfo> {
        if let RelOperator::EvalScalar(eval) = s_expr.plan() {
            let selection = eval;

            let mut predicates: std::option::Option<&[ScalarExpr]> = None;
            let mut sort_items = None;
            let mut aggregate = None;
            let mut column_map = HashMap::new();

            for item in &eval.items {
                column_map.insert(item.index, item.scalar.clone());
            }

            // collect query info from the plan
            let mut s_expr = s_expr.child(0)?;
            loop {
                match s_expr.plan() {
                    RelOperator::EvalScalar(eval) => {
                        for item in &eval.items {
                            column_map.insert(item.index, item.scalar.clone());
                        }
                    }
                    RelOperator::Aggregate(agg) => {
                        if agg.grouping_sets.is_some() {
                            return Err(ErrorCode::Internal("Grouping sets is not supported"));
                        }

                        aggregate = Some(agg.clone());
                        for item in &agg.aggregate_functions {
                            column_map.insert(item.index, item.scalar.clone());
                        }
                        for item in &agg.group_items {
                            column_map.insert(item.index, item.scalar.clone());
                        }
                        let child = s_expr.child(0)?;
                        if let RelOperator::EvalScalar(eval) = child.plan() {
                            for item in &eval.items {
                                column_map.insert(item.index, item.scalar.clone());
                            }
                            s_expr = child.child(0)?;
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
                s_expr = s_expr.child(0)?;
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
                column_map.insert(base_column.index(), column);
            }

            let mut column_display_map = HashMap::new();
            for (index, scalar) in column_map.iter() {
                let display_name = format_scalar(scalar, &column_map);
                if let Some(old_index) = column_display_map.get(&display_name) {
                    // use index from low level plan first.
                    if old_index < index {
                        continue;
                    }
                }
                column_display_map.insert(display_name, *index);
            }

            let mut preds_splitter = PredicatesSplitter::new();
            let mut equi_classes = EquivalenceClasses::new();
            let mut range_classes = RangeClasses::new();
            let mut residual_classes = ResidualClasses::new();

            // split predicates as equal predicate, range predicate, and residual predicate.
            if let Some(predicates) = predicates {
                for pred in predicates {
                    preds_splitter.split(pred, &column_map);
                }

                for equi_pred in &preds_splitter.equi_columns_preds {
                    equi_classes.add_equivalence_class(&equi_pred.0, &equi_pred.1);
                }
                for range_pred in &preds_splitter.range_preds {
                    range_classes.add_range_class(&range_pred.0, &range_pred.1, &range_pred.2);
                }
                for residual_pred in &preds_splitter.residual_preds {
                    let display_name = format_scalar(residual_pred, &column_map);
                    residual_classes.add_residual_pred(display_name, residual_pred);
                }
            }

            let mut output_cols = Vec::with_capacity(selection.items.len());
            for item in &selection.items {
                let actual_scalar = actual_column_ref(&item.scalar, &column_map);
                let actual_item = ScalarItem {
                    index: item.index,
                    scalar: actual_scalar.clone(),
                };
                output_cols.push(actual_item);
            }

            Ok(Self {
                equi_classes,
                range_classes,
                residual_classes,
                aggregate,
                sort_items,
                column_map,
                column_display_map,
                output_cols,
            })
        } else {
            Err(ErrorCode::Internal("Unsupported plan"))
        }
    }

    // check whether the scalar can be computed from index output columns.
    // if not, the aggregating index can't be used.
    fn check_output_cols(
        &self,
        scalar: &ScalarExpr,
        index_output_cols: &HashMap<String, (ScalarExpr, bool)>,
        new_selection_set: &mut HashSet<ScalarItem>,
    ) -> Result<Option<ScalarExpr>> {
        let display_name = format_scalar(scalar, &self.column_map);

        if let Some((new_scalar, is_agg)) = index_output_cols.get(&display_name) {
            if let Some(index) = self.column_display_map.get(&display_name) {
                let new_item = ScalarItem {
                    index: *index,
                    scalar: new_scalar.clone(),
                };
                new_selection_set.insert(new_item);
            }
            // agg function can't used
            if *is_agg {
                return Ok(None);
            } else {
                return Ok(Some(new_scalar.clone()));
            }
        }

        let new_scalar = match scalar {
            ScalarExpr::BoundColumnRef(_) => {
                let actual_column = actual_column_ref(scalar, &self.column_map);
                if !matches!(actual_column, ScalarExpr::BoundColumnRef(_)) {
                    return self.check_output_cols(
                        actual_column,
                        index_output_cols,
                        new_selection_set,
                    );
                }
                return Err(ErrorCode::Internal("Can't found column from index"));
            }
            ScalarExpr::ConstantExpr(_) => scalar.clone(),
            ScalarExpr::FunctionCall(func) => {
                let mut valid = true;
                let mut new_args = Vec::with_capacity(func.arguments.len());
                for arg in &func.arguments {
                    if let Some(new_arg) =
                        self.check_output_cols(arg, index_output_cols, new_selection_set)?
                    {
                        new_args.push(new_arg);
                    } else {
                        valid = false;
                    }
                }
                if !valid {
                    return Ok(None);
                }
                let mut new_func = func.clone();
                new_func.arguments = new_args;
                ScalarExpr::FunctionCall(new_func)
            }
            ScalarExpr::CastExpr(cast) => {
                if let Some(new_arg) =
                    self.check_output_cols(&cast.argument, index_output_cols, new_selection_set)?
                {
                    let mut new_cast = cast.clone();
                    new_cast.argument = Box::new(new_arg);
                    ScalarExpr::CastExpr(new_cast)
                } else {
                    return Ok(None);
                }
            }
            ScalarExpr::AggregateFunction(func) => {
                // agg function can't push down
                for arg in &func.args {
                    self.check_output_cols(arg, index_output_cols, new_selection_set)?;
                }
                return Ok(None);
            }
            ScalarExpr::UDFCall(udf) => {
                let mut valid = true;
                let mut new_args = Vec::with_capacity(udf.arguments.len());
                for arg in &udf.arguments {
                    if let Some(new_arg) =
                        self.check_output_cols(arg, index_output_cols, new_selection_set)?
                    {
                        new_args.push(new_arg);
                    } else {
                        valid = false;
                    }
                }
                if !valid {
                    return Ok(None);
                }
                let mut new_udf = udf.clone();
                new_udf.arguments = new_args;
                ScalarExpr::UDFCall(new_udf)
            }
            _ => unreachable!(), // Window function and subquery will not appear in index.
        };

        if let Some(index) = self.column_display_map.get(&display_name) {
            let new_item = ScalarItem {
                index: *index,
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
    index_output_cols: HashMap<String, (ScalarExpr, bool)>,
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
        let mut index_output_cols = HashMap::with_capacity(query_info.output_cols.len());
        for (index, item) in query_info.output_cols.iter().enumerate() {
            let display_name = format_scalar(&item.scalar, &query_info.column_map);

            let mut is_agg = false;
            if let Some(ref aggregate) = query_info.aggregate {
                for agg_func in &aggregate.aggregate_functions {
                    if item.index == agg_func.index {
                        is_agg = true;
                        break;
                    }
                }
            }
            // we store the value of aggregate function as binary data.
            let data_type = if is_agg {
                DataType::Binary
            } else {
                item.scalar.data_type().unwrap()
            };

            let name = format!("{index}");
            let table_ty = infer_schema_type(&data_type)?;
            let index_field = TableField::new(&name, table_ty);
            index_fields.push(index_field);

            let index_scalar = to_index_scalar(index, &data_type);
            index_output_cols.insert(display_name, (index_scalar, is_agg));
        }

        Ok(Self {
            query_info,
            index_fields,
            index_output_cols,
        })
    }
}

struct PredicatesSplitter {
    equi_columns_preds: Vec<(BoundColumnRef, BoundColumnRef)>,
    range_preds: Vec<(String, BoundColumnRef, ConstantExpr)>,
    residual_preds: Vec<ScalarExpr>,
}

impl PredicatesSplitter {
    fn new() -> Self {
        Self {
            equi_columns_preds: vec![],
            range_preds: vec![],
            residual_preds: vec![],
        }
    }

    fn split(&mut self, pred: &ScalarExpr, column_map: &HashMap<IndexType, ScalarExpr>) {
        if let ScalarExpr::FunctionCall(func) = pred {
            match func.func_name.as_str() {
                "and" => {
                    self.split(&func.arguments[0], column_map);
                    self.split(&func.arguments[1], column_map);
                }
                "eq" if matches!(func.arguments[0], ScalarExpr::BoundColumnRef(_))
                    && matches!(func.arguments[1], ScalarExpr::BoundColumnRef(_)) =>
                {
                    let arg0 = actual_column_ref(&func.arguments[0], column_map);
                    let arg1 = actual_column_ref(&func.arguments[1], column_map);

                    let col0 = BoundColumnRef::try_from(arg0.clone()).unwrap();
                    let col1 = BoundColumnRef::try_from(arg1.clone()).unwrap();
                    self.equi_columns_preds.push((col0, col1));
                }
                "eq" | "lt" | "lte" | "gt" | "gte"
                    if matches!(func.arguments[0], ScalarExpr::BoundColumnRef(_))
                        && matches!(func.arguments[1], ScalarExpr::ConstantExpr(_)) =>
                {
                    let func_name = func.func_name.clone();
                    let arg0 = actual_column_ref(&func.arguments[0], column_map);
                    let col = BoundColumnRef::try_from(arg0.clone()).unwrap();
                    let val = ConstantExpr::try_from(func.arguments[1].clone()).unwrap();
                    self.range_preds.push((func_name, col, val));
                }
                "eq" | "lt" | "lte" | "gt" | "gte"
                    if matches!(func.arguments[0], ScalarExpr::ConstantExpr(_))
                        && matches!(func.arguments[1], ScalarExpr::BoundColumnRef(_)) =>
                {
                    let func_name = reverse_op(func.func_name.as_str());

                    let val = ConstantExpr::try_from(func.arguments[0].clone()).unwrap();
                    let arg1 = actual_column_ref(&func.arguments[1], column_map);
                    let col = BoundColumnRef::try_from(arg1.clone()).unwrap();
                    self.range_preds.push((func_name, col, val));
                }
                _ => {
                    self.residual_preds.push(pred.clone());
                }
            }
        } else {
            self.residual_preds.push(pred.clone());
        }
    }
}

struct EquivalenceClasses {
    column_to_equivalence_class: HashMap<String, HashSet<String>>,
}

impl EquivalenceClasses {
    fn new() -> Self {
        Self {
            column_to_equivalence_class: HashMap::new(),
        }
    }

    fn add_equivalence_class(&mut self, col1: &BoundColumnRef, col2: &BoundColumnRef) {
        let mut equivalence_columns = HashSet::new();

        let col1_name = format_col(&col1.column);
        let col2_name = format_col(&col2.column);

        equivalence_columns.insert(col1_name.clone());
        equivalence_columns.insert(col2_name.clone());

        if let Some(c1) = self.column_to_equivalence_class.get(&col1_name) {
            for c in c1 {
                equivalence_columns.insert(c.clone());
            }
        }
        if let Some(c2) = self.column_to_equivalence_class.get(&col2_name) {
            for c in c2 {
                equivalence_columns.insert(c.clone());
            }
        }

        for column in &equivalence_columns {
            if let Some(orig_columns) = self.column_to_equivalence_class.get_mut(column) {
                for equi_column in &equivalence_columns {
                    if equi_column == column {
                        continue;
                    }
                    orig_columns.insert(equi_column.clone());
                }
            } else {
                let mut equi_cols = equivalence_columns.clone();
                equi_cols.remove(column);
                self.column_to_equivalence_class
                    .insert(column.clone(), equi_cols);
            }
        }
    }

    // Equijoin subsumption test.
    fn check(&self, view_equi_classes: &EquivalenceClasses) -> bool {
        for (col, view_equi_cols) in view_equi_classes.column_to_equivalence_class.iter() {
            if let Some(query_equi_cols) = self.column_to_equivalence_class.get(col) {
                // checking whether every non-trivial view equivalence class
                // is a subset of some query equivalence class
                if view_equi_cols.is_subset(query_equi_cols) {
                    continue;
                }
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

struct RangeClasses {
    column_to_range_class: BTreeMap<String, RangeValues>,
}

impl RangeClasses {
    fn new() -> Self {
        Self {
            column_to_range_class: BTreeMap::new(),
        }
    }

    fn add_range_class(&mut self, func_name: &str, col: &BoundColumnRef, val: &ConstantExpr) {
        let col_name = format_col(&col.column);

        let (lower_bound, upper_bound) = match func_name {
            "eq" => (
                BoundValue::Closed(val.value.clone()),
                BoundValue::Closed(val.value.clone()),
            ),
            "lt" => (
                BoundValue::NegativeInfinite,
                BoundValue::Open(val.value.clone()),
            ),
            "lte" => (
                BoundValue::NegativeInfinite,
                BoundValue::Closed(val.value.clone()),
            ),
            "gt" => (
                BoundValue::Open(val.value.clone()),
                BoundValue::PositiveInfinite,
            ),
            "gte" => (
                BoundValue::Closed(val.value.clone()),
                BoundValue::PositiveInfinite,
            ),
            _ => unreachable!(),
        };
        if !self.column_to_range_class.contains_key(&col_name) {
            self.column_to_range_class
                .insert(col_name.clone(), RangeValues::new());
        }
        if let Some(range_values) = self.column_to_range_class.get_mut(&col_name) {
            range_values.insert(lower_bound, upper_bound);
        }
    }

    // Range subsumption test.
    #[allow(clippy::type_complexity)]
    fn check(
        &self,
        view_range_classes: &RangeClasses,
    ) -> (bool, Option<BTreeMap<String, (BoundValue, BoundValue)>>) {
        // if the range predicate in aggregating index and the query have three cases.
        // 1. the range of aggregating index and query are same, don't need extra filter ranges.
        // 2. the range of aggregating index filter less values than the query,
        //    we can add extra range predicate to implemente the filter.
        //    for example: aggregating index: a > 10 and query: a > 15
        //    we can add extra range as a > 15
        // 3. the range of aggregating index filter more values than the query,
        //    this aggregating index don't match the query.
        //    for example: aggregating index: a > 10 and query: a > 5
        let mut extra_ranges = BTreeMap::new();
        for (col, query_range_values) in self.column_to_range_class.iter() {
            if !view_range_classes.column_to_range_class.contains_key(col) {
                if let Some((query_lower_bound, query_upper_bound)) = &query_range_values.bounds {
                    extra_ranges.insert(
                        col.clone(),
                        (query_lower_bound.clone(), query_upper_bound.clone()),
                    );
                }
            }
        }

        for (col, view_range_values) in view_range_classes.column_to_range_class.iter() {
            if let Some(query_range_values) = self.column_to_range_class.get(col) {
                match (&query_range_values.bounds, &view_range_values.bounds) {
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
                                extra_ranges.insert(
                                    col.clone(),
                                    (BoundValue::NegativeInfinite, query_upper_bound.clone()),
                                );
                            }
                            (Ordering::Less, Ordering::Equal) => {
                                extra_ranges.insert(
                                    col.clone(),
                                    (query_lower_bound.clone(), BoundValue::PositiveInfinite),
                                );
                            }
                            (Ordering::Less, Ordering::Greater) => {
                                extra_ranges.insert(
                                    col.clone(),
                                    (query_lower_bound.clone(), query_upper_bound.clone()),
                                );
                            }
                            (_, _) => {
                                return (false, None);
                            }
                        }
                    }
                    (Some((query_lower_bound, query_upper_bound)), None) => {
                        extra_ranges.insert(
                            col.clone(),
                            (query_lower_bound.clone(), query_upper_bound.clone()),
                        );
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

struct ResidualClasses {
    residual_preds: BTreeMap<String, ScalarExpr>,
}

impl ResidualClasses {
    fn new() -> Self {
        Self {
            residual_preds: BTreeMap::new(),
        }
    }

    fn add_residual_pred(&mut self, pred_display: String, pred: &ScalarExpr) {
        self.residual_preds.insert(pred_display, pred.clone());
    }

    // Residual subsumption test.
    fn check(&self, view_residual_classes: &ResidualClasses) -> (bool, Option<Vec<ScalarExpr>>) {
        let mut extra_residual_preds = Vec::new();
        for (view_residual_key, _) in view_residual_classes.residual_preds.iter() {
            if !self.residual_preds.contains_key(view_residual_key) {
                return (false, None);
            }
        }
        // TODO: continue split residual predicates and check
        for (query_residual_key, query_residual_pred) in self.residual_preds.iter() {
            if !view_residual_classes
                .residual_preds
                .contains_key(query_residual_key)
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
    fn new(query_info: QueryInfo) -> Self {
        Self { query_info }
    }

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
        if !self
            .query_info
            .equi_classes
            .check(&view_info.query_info.equi_classes)
        {
            return false;
        }
        let (range_res, extra_ranges) = self
            .query_info
            .range_classes
            .check(&view_info.query_info.range_classes);
        if !range_res {
            return false;
        }

        let (residual_res, extra_residual_preds) = self
            .query_info
            .residual_classes
            .check(&view_info.query_info.residual_classes);
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

        if let Some(extra_ranges) = extra_ranges {
            for (col, (lower_bound, upper_bound)) in extra_ranges.iter() {
                // materialized view output must contains the column
                if let Some((new_scalar, _)) = view_info.index_output_cols.get(col) {
                    let lower = match lower_bound {
                        BoundValue::Closed(val) => Some((val.clone(), "gte")),
                        BoundValue::Open(val) => Some((val.clone(), "gt")),
                        BoundValue::NegativeInfinite => None,
                        _ => unreachable!(),
                    };
                    let upper = match upper_bound {
                        BoundValue::Closed(val) => Some((val.clone(), "lte")),
                        BoundValue::Open(val) => Some((val.clone(), "lt")),
                        BoundValue::PositiveInfinite => None,
                        _ => unreachable!(),
                    };

                    if let (Some((lower_val, "gte")), Some((upper_val, "lte"))) = (&lower, &upper) {
                        // if lower and upper value equal, convert to equal function
                        if lower_val.eq(upper_val) {
                            let lower_val_scalar = ScalarExpr::ConstantExpr(ConstantExpr {
                                span: None,
                                value: lower_val.clone(),
                            });
                            let pred = ScalarExpr::FunctionCall(FunctionCall {
                                span: None,
                                func_name: "eq".to_string(),
                                params: vec![],
                                arguments: vec![new_scalar.clone(), lower_val_scalar],
                            });
                            new_predicates.push(pred);
                            continue;
                        }
                    }

                    if let Some((lower_val, func_name)) = lower {
                        let lower_val_scalar = ScalarExpr::ConstantExpr(ConstantExpr {
                            span: None,
                            value: lower_val,
                        });
                        let pred = ScalarExpr::FunctionCall(FunctionCall {
                            span: None,
                            func_name: func_name.to_string(),
                            params: vec![],
                            arguments: vec![new_scalar.clone(), lower_val_scalar],
                        });
                        new_predicates.push(pred);
                    }
                    if let Some((upper_val, func_name)) = upper {
                        let upper_val_scalar = ScalarExpr::ConstantExpr(ConstantExpr {
                            span: None,
                            value: upper_val,
                        });
                        let pred = ScalarExpr::FunctionCall(FunctionCall {
                            span: None,
                            func_name: func_name.to_string(),
                            params: vec![],
                            arguments: vec![new_scalar.clone(), upper_val_scalar],
                        });
                        new_predicates.push(pred);
                    }
                } else {
                    return false;
                }
            }
        }

        if let Some(extra_residual_preds) = extra_residual_preds {
            for extra_residual_pred in extra_residual_preds {
                match self.query_info.check_output_cols(
                    &extra_residual_pred,
                    &view_info.index_output_cols,
                    new_selection_set,
                ) {
                    Ok(Some(new_residual_pred)) => {
                        new_predicates.push(new_residual_pred);
                    }
                    Ok(None) => {}
                    Err(_) => {
                        return false;
                    }
                }
            }
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
        for output_item in self.query_info.output_cols.iter() {
            if self
                .query_info
                .check_output_cols(
                    &output_item.scalar,
                    &view_info.index_output_cols,
                    new_selection_set,
                )
                .is_err()
            {
                return false;
            }
        }
        true
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

        let query_group_items = self
            .query_info
            .aggregate
            .clone()
            .map(|agg| agg.group_items)
            .unwrap_or_default();

        let view_group_items = view_info
            .query_info
            .aggregate
            .clone()
            .clone()
            .map(|agg| agg.group_items)
            .unwrap_or_default();

        match (query_group_items.is_empty(), view_group_items.is_empty()) {
            // both query and view have group, check for same group items.
            (false, false) => {
                // TODO: query can support continue group
                if query_group_items.len() != view_group_items.len() {
                    return false;
                }
                let mut query_group_names = HashSet::with_capacity(query_group_items.len());
                for item in &query_group_items {
                    let query_group_name = format_scalar(&item.scalar, &self.query_info.column_map);
                    query_group_names.insert(query_group_name);
                }
                let mut view_group_names = HashSet::with_capacity(view_group_items.len());
                for item in &view_group_items {
                    let view_group_name =
                        format_scalar(&item.scalar, &view_info.query_info.column_map);
                    view_group_names.insert(view_group_name);
                }
                for query_group_name in query_group_names {
                    if !view_group_names.contains(&query_group_name) {
                        return false;
                    }
                }

                for item in query_group_items {
                    if self
                        .query_info
                        .check_output_cols(
                            &item.scalar,
                            &view_info.index_output_cols,
                            new_selection_set,
                        )
                        .is_err()
                    {
                        return false;
                    }
                }
            }
            // query have group, but view don't have group,
            // check group items in output rows.
            (false, true) => {
                for item in query_group_items {
                    if self
                        .query_info
                        .check_output_cols(
                            &item.scalar,
                            &view_info.index_output_cols,
                            new_selection_set,
                        )
                        .is_err()
                    {
                        return false;
                    }
                }
            }
            // both query and view don't have group, don't need check.
            (true, true) => {}
            // query don't have group, but view have group, impossibile to match.
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
        if let Some(sort_items) = &self.query_info.sort_items {
            for item in sort_items {
                if let Some(scalar) = self.query_info.column_map.get(&item.index) {
                    if self
                        .query_info
                        .check_output_cols(scalar, &view_info.index_output_cols, new_selection_set)
                        .is_err()
                    {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }

        true
    }
}

fn to_index_scalar(index: IndexType, data_type: &DataType) -> ScalarExpr {
    let col = BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            format!("index_col_{index}"),
            index,
            Box::new(data_type.clone()),
            Visibility::Visible,
        )
        .build(),
    };
    ScalarExpr::BoundColumnRef(col)
}

#[inline(always)]
fn format_col(column: &ColumnBinding) -> String {
    match &column.table_name {
        Some(table_name) => {
            format!("{}.{}", table_name, column.column_name)
        }
        None => column.column_name.clone(),
    }
}

#[inline(always)]
fn reverse_op(op: &str) -> String {
    match op {
        "gt" => "lt".to_string(),
        "gte" => "lte".to_string(),
        "lt" => "gt".to_string(),
        "lte" => "gte".to_string(),
        "eq" => "eq".to_string(),
        _ => unreachable!(),
    }
}

// replace derived column with actual ScalarExpr.
fn actual_column_ref<'a>(
    col: &'a ScalarExpr,
    column_map: &'a HashMap<IndexType, ScalarExpr>,
) -> &'a ScalarExpr {
    if let ScalarExpr::BoundColumnRef(col) = col {
        if let Some(arg) = column_map.get(&col.column.index) {
            return arg;
        }
    }
    col
}

fn format_scalar(scalar: &ScalarExpr, column_map: &HashMap<IndexType, ScalarExpr>) -> String {
    match scalar {
        ScalarExpr::BoundColumnRef(_) => match actual_column_ref(scalar, column_map) {
            ScalarExpr::BoundColumnRef(col) => format_col(&col.column),
            s => format_scalar(s, column_map),
        },
        ScalarExpr::ConstantExpr(val) => format!("{}", val.value),
        ScalarExpr::FunctionCall(func) => format!(
            "{}({})",
            &func.func_name,
            func.arguments
                .iter()
                .map(|arg| { format_scalar(arg, column_map) })
                .collect::<Vec<String>>()
                .join(", ")
        ),
        ScalarExpr::CastExpr(cast) => {
            let func_name = if cast.is_try { "try_cast" } else { "cast" };
            format!(
                "{}({} as {})",
                func_name,
                format_scalar(&cast.argument, column_map),
                cast.target_type
            )
        }
        ScalarExpr::AggregateFunction(agg) => {
            let params = agg
                .params
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let args = agg
                .args
                .iter()
                .map(|arg| format_scalar(arg, column_map))
                .collect::<Vec<_>>()
                .join(", ");
            if !params.is_empty() {
                format!("{}<{}>({})", &agg.func_name, params, args)
            } else {
                format!("{}({})", &agg.func_name, args)
            }
        }
        ScalarExpr::UDFCall(udf) => format!(
            "{}({})",
            &udf.func_name,
            udf.arguments
                .iter()
                .map(|arg| { format_scalar(arg, column_map) })
                .collect::<Vec<String>>()
                .join(", ")
        ),

        _ => unreachable!(), // Window function and subquery will not appear in index.
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
