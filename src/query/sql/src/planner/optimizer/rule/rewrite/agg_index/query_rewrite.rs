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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use itertools::Itertools;
use log::info;

use crate::binder::split_conjunctions;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::SExpr;
use crate::plans::AggIndexInfo;
use crate::plans::Aggregate;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::SortItem;
use crate::plans::UDFCall;
use crate::plans::VisitorMut;
use crate::ColumnEntry;
use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;
use crate::Visibility;

pub fn try_rewrite(
    table_index: IndexType,
    base_columns: &[ColumnEntry],
    s_expr: &SExpr,
    index_plans: &[(u64, String, SExpr)],
) -> Result<Option<SExpr>> {
    if index_plans.is_empty() {
        return Ok(None);
    }

    let query_info = collect_information(s_expr)?;
    if !query_info.can_apply_index() {
        return Ok(None);
    }

    let col_index_map = base_columns
        .iter()
        .map(|col| (col.name(), col.index()))
        .collect::<HashMap<_, _>>();

    let query_predicates = query_info.predicates.map(distinguish_predicates);
    let query_sort_items = query_info.formatted_sort_items();
    let query_group_items = query_info.formatted_group_items();

    if query_info.aggregation.is_some() {
        // if have agg funcs, check sort items are subset of group items.
        if !query_sort_items
            .iter()
            .all(|item| query_group_items.contains(item))
        {
            return Ok(None);
        }
    }

    // Search all index plans, find the first matched index to rewrite the query.
    for (index_id, sql, plan) in index_plans.iter() {
        let plan = rewrite_index_plan(table_index, &col_index_map, plan);

        let index_info = collect_information(&plan)?;
        debug_assert!(index_info.can_apply_index());

        // 1. Check query output and try to rewrite it.
        let index_selection = index_info.formatted_selection()?;

        // group items should be in selection.
        if !query_group_items
            .iter()
            .all(|item| index_selection.contains_key(item))
        {
            continue;
        }

        let mut new_selection = Vec::with_capacity(query_info.selection.items.len());
        let mut flag = true;
        let mut is_agg = false;
        let mut num_agg_funcs = 0;

        match (&query_info.aggregation, &index_info.aggregation) {
            (Some((query_agg, _)), Some(_)) => {
                is_agg = true;
                // Check if group items are the same.
                let index_group_items = index_info.formatted_group_items();
                if query_group_items != index_group_items {
                    continue;
                }
                // If the query is an aggregation query, the index selection is to rewrite the input `EvalScalar` operator of `Aggregate` operators.
                // In another word, is to rewrite the input items of the aggregation.
                // The input of aggregation will only have the arguments of the aggregate functions and group by columns (expressions).
                // So just need to find if the aggregate functions call and group by exprs exist in index selection.
                for expr in query_agg.group_items.iter() {
                    if let Some(rewritten) = try_create_column_binding(
                        &index_selection,
                        &query_info.format_scalar(&expr.scalar),
                    ) {
                        new_selection.push(ScalarItem {
                            index: expr.index,
                            scalar: rewritten.into(),
                        });
                    } else {
                        flag = false;
                        break;
                    }
                }
                if flag {
                    num_agg_funcs = query_agg.aggregate_functions.len();
                    for agg in query_agg.aggregate_functions.iter() {
                        if let Some(mut rewritten) = try_create_column_binding(
                            &index_selection,
                            &query_info.format_scalar(&agg.scalar),
                        ) {
                            rewritten.column.data_type = Box::new(DataType::Binary);
                            new_selection.push(ScalarItem {
                                index: agg.index,
                                scalar: rewritten.into(),
                            });
                        } else {
                            flag = false;
                            break;
                        }
                    }
                }
            }
            (Some((_, input)), None) => {
                // Check if we can use the output of the index as the input of the query's `Aggregate` operators.
                for (index, scalar) in input {
                    if let Some(rewritten) =
                        rewrite_by_selection(&query_info, scalar, &index_selection)
                    {
                        new_selection.push(ScalarItem {
                            index: *index,
                            scalar: rewritten,
                        });
                    } else {
                        flag = false;
                        break;
                    }
                }
            }

            (None, Some(_)) => {
                continue;
            }
            (None, None) => {
                // If the query is not an aggregation query, the index selection is to rewrite the final output `EvalScalar` operator.
                // In another word, is to rewrite `query_info.selection`.
                for item in query_info.selection.items.iter() {
                    if let Some(rewritten) =
                        rewrite_by_selection(&query_info, &item.scalar, &index_selection)
                    {
                        new_selection.push(ScalarItem {
                            index: item.index,
                            scalar: rewritten,
                        });
                    } else {
                        flag = false;
                        break;
                    }
                }
            }
        }

        if !flag {
            continue;
        }

        // 2. Check filter predicates.
        let output_bound_cols = index_info.output_bound_cols();
        let index_predicates = index_info.predicates.map(distinguish_predicates);
        let mut new_predicates = Vec::new();
        match (&query_predicates, &index_predicates) {
            (Some((qe, qr, qo)), Some((ie, ir, io))) => {
                if !check_predicates_equal(qe, ie) {
                    continue;
                }
                if !check_predicates_other(qo, io) {
                    continue;
                }
                if let Some(preds) =
                    check_predicates_range(qr, ir, &output_bound_cols, &index_selection)
                {
                    new_predicates.extend(preds);
                } else {
                    continue;
                }
            }
            (Some((qe, qr, qo)), None) => {
                if !qe.is_empty() {
                    let preds = qe
                        .iter()
                        .flat_map(|(left, right)| [(*left).clone(), (*right).clone()])
                        .collect::<Vec<_>>();
                    new_predicates.extend(preds);
                }
                if !qo.is_empty() {
                    let preds = qo.iter().map(|p| (*p).clone()).collect::<Vec<_>>();
                    new_predicates.extend(preds);
                }
                if let Some(preds) = check_predicates_range(
                    qr,
                    &HashMap::new(),
                    &output_bound_cols,
                    &index_selection,
                ) {
                    new_predicates.extend(preds);
                } else {
                    continue;
                }
            }
            (None, Some(_)) => {
                // Not matched.
                continue;
            }
            (None, None) => { /* Matched */ }
        }

        // 3. Construct the index output schema
        let agg_func_indices = index_info
            .aggregation
            .as_ref()
            .map(|(agg, _)| {
                agg.aggregate_functions
                    .iter()
                    .map(|f| f.index)
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();
        let index_fields = index_selection
            .iter()
            .sorted_by_key(|(_, (idx, _))| *idx)
            .map(|(_, (idx, ty))| {
                if let ScalarExpr::BoundColumnRef(col) = &index_info.selection.items[*idx].scalar {
                    if agg_func_indices.contains(&col.column.index) {
                        // If the item is an aggregation function,
                        // the actual data in the index is the temp state of the function.
                        // (E.g. `sum` function will store serialized `sum_state` in index data.)
                        // So the data type will be `Binary`.
                        return Ok(TableField::new(&idx.to_string(), TableDataType::Binary));
                    }
                }

                Ok(TableField::new(&idx.to_string(), infer_schema_type(ty)?))
            })
            .collect::<Result<Vec<_>>>()?;

        new_selection.sort_by_key(|i| i.index);

        let result = push_down_index_scan(s_expr, AggIndexInfo {
            index_id: *index_id,
            selection: new_selection,
            predicates: new_predicates,
            schema: TableSchemaRefExt::create(index_fields),
            is_agg,
            num_agg_funcs,
        })?;

        info!("Use aggregating index: {sql}");

        return Ok(Some(result));
    }

    Ok(None)
}

/// Rewrite base column index in the original index plan by `columns`.
fn rewrite_index_plan(
    table_index: IndexType,
    columns: &HashMap<String, IndexType>,
    s_expr: &SExpr,
) -> SExpr {
    match s_expr.plan() {
        RelOperator::EvalScalar(eval) => {
            let mut new_expr = eval.clone();
            for item in new_expr.items.iter_mut() {
                rewrite_scalar_index(table_index, columns, &mut item.scalar);
            }
            SExpr::create_unary(
                Arc::new(new_expr.into()),
                Arc::new(rewrite_index_plan(
                    table_index,
                    columns,
                    s_expr.child(0).unwrap(),
                )),
            )
        }
        RelOperator::Filter(filter) => {
            let mut new_expr = filter.clone();
            for pred in new_expr.predicates.iter_mut() {
                rewrite_scalar_index(table_index, columns, pred);
            }
            SExpr::create_unary(
                Arc::new(new_expr.into()),
                Arc::new(rewrite_index_plan(
                    table_index,
                    columns,
                    s_expr.child(0).unwrap(),
                )),
            )
        }
        RelOperator::Aggregate(agg) => {
            let mut new_expr = agg.clone();
            for item in new_expr.group_items.iter_mut() {
                rewrite_scalar_index(table_index, columns, &mut item.scalar);
            }
            for item in new_expr.aggregate_functions.iter_mut() {
                rewrite_scalar_index(table_index, columns, &mut item.scalar);
            }
            SExpr::create_unary(
                Arc::new(new_expr.into()),
                Arc::new(rewrite_index_plan(
                    table_index,
                    columns,
                    s_expr.child(0).unwrap(),
                )),
            )
        }
        RelOperator::Scan(scan) => {
            let mut new_expr = scan.clone();
            new_expr.table_index = table_index;
            SExpr::create_leaf(Arc::new(new_expr.into()))
        } // Terminate the recursion.
        _ => s_expr.replace_children(vec![Arc::new(rewrite_index_plan(
            table_index,
            columns,
            s_expr.child(0).unwrap(),
        ))]),
    }
}

fn rewrite_scalar_index(
    table_index: IndexType,
    columns: &HashMap<String, IndexType>,
    scalar: &mut ScalarExpr,
) {
    struct RewriteVisitor<'a> {
        table_index: IndexType,
        columns: &'a HashMap<String, IndexType>,
    }

    impl<'a> VisitorMut<'a> for RewriteVisitor<'a> {
        fn visit_bound_column_ref(&mut self, col: &'a mut BoundColumnRef) -> Result<()> {
            if let Some(index) = self.columns.get(&col.column.column_name) {
                col.column.table_index = Some(self.table_index);
                col.column.index = *index;
            }
            Ok(())
        }
    }

    let mut visitor = RewriteVisitor {
        table_index,
        columns,
    };
    visitor.visit(scalar).unwrap();
}

/// [`Range`] is to represent the value range of a column according to the predicates.
///
/// Notes that only conjunctions will be parsed, and disjunctions will be ignored.
#[derive(Default, Debug)]
struct Range<'a> {
    min: Option<&'a Scalar>,
    min_close: bool,
    max: Option<&'a Scalar>,
    max_close: bool,
}

impl<'a> PartialEq for Range<'a> {
    fn eq(&self, other: &Self) -> bool {
        // We cannot compare Scalar directly because when the NumberScalar types
        // are different but the internal values are the same, the comparison
        // result is false.
        // So we need to compare the internal values of the Scalar, for example,
        // `NumberScalar(UInt8(1)) == NumberScalar(UInt32(1))` should return true.
        fn scalar_equal(left: Option<&Scalar>, right: Option<&Scalar>) -> bool {
            match (left, right) {
                (Some(left), Some(right)) => {
                    if let (Scalar::Number(left), Scalar::Number(right)) = (left, right) {
                        match (left.is_integer(), right.is_integer()) {
                            (true, true) => {
                                left.integer_to_i128().unwrap() == right.integer_to_i128().unwrap()
                            }
                            (false, false) => {
                                left.float_to_f64().unwrap() == right.float_to_f64().unwrap()
                            }
                            _ => false,
                        }
                    } else {
                        left == right
                    }
                }
                (None, None) => true,
                _ => false,
            }
        }

        if self.min_close != other.min_close
            || self.max_close != other.max_close
            || !scalar_equal(self.min, other.min)
            || !scalar_equal(self.max, other.max)
        {
            return false;
        }

        true
    }
}

impl<'a> Eq for Range<'a> {}

impl<'a> Range<'a> {
    fn new(val: &'a Scalar, op: &str) -> Self {
        let mut range = Range::default();
        range.set_bound(val, op);
        range
    }

    #[inline]
    fn set_bound(&mut self, val: &'a Scalar, op: &str) {
        match op {
            "gt" => self.set_min(val, false),
            "gte" => self.set_min(val, true),
            "lt" => self.set_max(val, false),
            "lte" => self.set_max(val, true),
            _ => unreachable!(),
        }
    }

    #[inline]
    fn set_min(&mut self, val: &'a Scalar, close: bool) {
        match self.min {
            Some(min) if val < min => {
                self.min = Some(val);
                self.min_close = close;
            }
            Some(min) if val == min => {
                self.min_close = self.min_close || close;
            }
            None => {
                self.min = Some(val);
                self.min_close = close;
            }
            _ => {}
        }
    }

    #[inline]
    fn set_max(&mut self, val: &'a Scalar, close: bool) {
        match self.max {
            Some(max) if val > max => {
                self.max = Some(val);
                self.max_close = close;
            }
            Some(max) if val == max => {
                self.max_close = self.max_close || close;
            }
            None => {
                self.max = Some(val);
                self.max_close = close;
            }
            _ => {}
        }
    }

    #[inline]
    fn is_valid(&self) -> bool {
        match (self.min, self.max) {
            (Some(min), Some(max)) => min < max || (min == max && self.min_close && self.max_close),
            _ => true,
        }
    }

    /// If current range contains the other range.
    #[inline]
    fn contains(&self, other: &Range) -> bool {
        if !self.is_valid() || !other.is_valid() {
            return false;
        }

        // We need to compare the internal values of the Scalar, for example,
        // `NumberScalar(UInt8(2)) > NumberScalar(UInt32(1))` should return true.
        match (self.min, other.min) {
            (Some(left), Some(right)) => {
                if let (Scalar::Number(left), Scalar::Number(right)) = (left, right) {
                    match (left.is_integer(), right.is_integer()) {
                        (true, true) => {
                            let left = left.integer_to_i128().unwrap();
                            let right = right.integer_to_i128().unwrap();
                            if left > right || (left == right && self.min_close && !other.min_close)
                            {
                                return false;
                            }
                        }
                        (false, false) => {
                            let left = left.float_to_f64().unwrap();
                            let right = right.float_to_f64().unwrap();
                            if left > right || (left == right && self.min_close && !other.min_close)
                            {
                                return false;
                            }
                        }
                        _ => return false,
                    }
                } else if left > right || (left == right && self.min_close && !other.min_close) {
                    return false;
                }
            }
            (Some(_), None) => {
                return false;
            }
            _ => {}
        }

        match (self.max, other.max) {
            (Some(left), Some(right)) => {
                if let (Scalar::Number(left), Scalar::Number(right)) = (left, right) {
                    match (left.is_integer(), right.is_integer()) {
                        (true, true) => {
                            let left = left.integer_to_i128().unwrap();
                            let right = right.integer_to_i128().unwrap();
                            if left < right || (left == right && self.min_close && !other.min_close)
                            {
                                return false;
                            }
                        }
                        (false, false) => {
                            let left = left.float_to_f64().unwrap();
                            let right = right.float_to_f64().unwrap();
                            if left < right || (left == right && self.min_close && !other.min_close)
                            {
                                return false;
                            }
                        }
                        _ => return false,
                    }
                } else if left < right || (left == right && self.min_close && !other.min_close) {
                    return false;
                }
            }
            (Some(_), None) => {
                return false;
            }
            _ => {}
        }

        true
    }

    fn to_scalar(&self, index: IndexType, data_type: &DataType) -> Option<ScalarExpr> {
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
        match (self.min, self.max) {
            (Some(min), Some(max)) => Some(
                FunctionCall {
                    span: None,
                    func_name: "and".to_string(),
                    params: vec![],
                    arguments: vec![
                        FunctionCall {
                            span: None,
                            func_name: if self.min_close {
                                "gte".to_string()
                            } else {
                                "gt".to_string()
                            },
                            params: vec![],
                            arguments: vec![
                                col.clone().into(),
                                ConstantExpr {
                                    span: None,
                                    value: min.clone(),
                                }
                                .into(),
                            ],
                        }
                        .into(),
                        FunctionCall {
                            span: None,
                            func_name: if self.max_close {
                                "lte".to_string()
                            } else {
                                "lt".to_string()
                            },
                            params: vec![],
                            arguments: vec![
                                col.into(),
                                ConstantExpr {
                                    span: None,
                                    value: max.clone(),
                                }
                                .into(),
                            ],
                        }
                        .into(),
                    ],
                }
                .into(),
            ),
            (Some(min), None) => Some(
                FunctionCall {
                    span: None,
                    func_name: if self.min_close {
                        "gte".to_string()
                    } else {
                        "gt".to_string()
                    },
                    params: vec![],
                    arguments: vec![
                        col.into(),
                        ConstantExpr {
                            span: None,
                            value: min.clone(),
                        }
                        .into(),
                    ],
                }
                .into(),
            ),
            (None, Some(max)) => Some(
                FunctionCall {
                    span: None,
                    func_name: if self.max_close {
                        "lte".to_string()
                    } else {
                        "lt".to_string()
                    },
                    params: vec![],
                    arguments: vec![
                        col.into(),
                        ConstantExpr {
                            span: None,
                            value: max.clone(),
                        }
                        .into(),
                    ],
                }
                .into(),
            ),
            _ => None,
        }
    }
}

/// Each element is the operands of each equal predicate.
type EqualPredicates<'a> = Vec<(&'a ScalarExpr, &'a ScalarExpr)>;
/// Each element is the operands and the operator of each range predicate.
/// Currently, range predicates should have one column and one constant.
type RangePredicates<'a> = HashMap<IndexType, Range<'a>>;
/// Each element is the full expression of each other predicate .
type OtherPredicates<'a> = Vec<&'a ScalarExpr>;

type Predicates<'a> = (
    EqualPredicates<'a>,
    RangePredicates<'a>,
    OtherPredicates<'a>,
);

type AggregationInfo<'a> = (&'a Aggregate, HashMap<IndexType, &'a ScalarExpr>);

/// The data structure to record the selection.
///
/// - Key: the formatted expression.
/// - Value: (index, data type) of the expression
type SelectionMap<'a> = HashMap<String, (IndexType, DataType)>;

// Record information helping to rewrite the query plan.
pub struct RewriteInfomartion<'a> {
    table_index: IndexType,
    pub selection: &'a EvalScalar,
    pub predicates: Option<&'a [ScalarExpr]>,
    pub sort_items: Option<&'a [SortItem]>,
    pub aggregation: Option<AggregationInfo<'a>>,
}

impl RewriteInfomartion<'_> {
    fn output_bound_cols(&self) -> ColumnSet {
        let mut cols = ColumnSet::new();
        for item in self.selection.items.iter() {
            if let ScalarExpr::BoundColumnRef(col) = &item.scalar {
                cols.insert(col.column.index);
            }
        }

        cols
    }

    fn can_apply_index(&self) -> bool {
        if let Some((agg, _)) = self.aggregation {
            if agg.grouping_sets.is_some() {
                // Grouping sets is not supported.
                return false;
            }
        }

        true
    }

    fn formatted_group_items(&self) -> Vec<String> {
        if let Some((agg, _)) = self.aggregation {
            let mut cols = Vec::with_capacity(agg.group_items.len());
            for item in agg.group_items.iter() {
                cols.push(self.format_scalar(&item.scalar));
            }
            cols.sort();
            return cols;
        }
        vec![]
    }

    fn formatted_sort_items(&self) -> Vec<String> {
        if let Some(sorts) = self.sort_items {
            let mut cols = Vec::with_capacity(sorts.len());
            for item in sorts {
                cols.push(format_col_name(item.index));
            }
            cols.sort();
            return cols;
        }
        vec![]
    }

    fn formatted_selection(&self) -> Result<SelectionMap<'_>> {
        let mut outputs = HashMap::with_capacity(self.selection.items.len());
        for (index, item) in self.selection.items.iter().enumerate() {
            let ty = item.scalar.data_type()?;
            let key = self.format_scalar(&item.scalar);
            outputs.insert(key, (index, ty));
        }
        Ok(outputs)
    }

    // If the column ref is already rewritten, recover it.
    fn actual_column_ref<'a>(&'a self, col: &'a ScalarExpr) -> &'a ScalarExpr {
        if let ScalarExpr::BoundColumnRef(col) = col {
            if let Some((agg, args)) = &self.aggregation {
                // Check if the col is an aggregation function.
                if let Some(func) = agg
                    .aggregate_functions
                    .iter()
                    .find(|item| item.index == col.column.index)
                {
                    return &func.scalar;
                }
                // Check if the col is an argument of aggregation function.
                if let Some(arg) = args.get(&col.column.index) {
                    return arg;
                }
            }
        }
        col
    }

    fn format_scalar(&self, scalar: &ScalarExpr) -> String {
        match scalar {
            ScalarExpr::BoundColumnRef(_) => match self.actual_column_ref(scalar) {
                ScalarExpr::BoundColumnRef(col) => format_col_name(col.column.index),
                s => self.format_scalar(s),
            },
            ScalarExpr::ConstantExpr(val) => format!("{}", val.value),
            ScalarExpr::FunctionCall(func) => format!(
                "{}({})",
                &func.func_name,
                func.arguments
                    .iter()
                    .map(|arg| { self.format_scalar(arg) })
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            ScalarExpr::CastExpr(cast) => format!(
                "CAST({} AS {})",
                self.format_scalar(&cast.argument),
                cast.target_type
            ),
            ScalarExpr::AggregateFunction(agg) => {
                format!(
                    "{}<{}>({})",
                    &agg.func_name,
                    agg.params
                        .iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    agg.args
                        .iter()
                        .map(|arg| { self.format_scalar(arg) })
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            ScalarExpr::UDFCall(udf) => format!(
                "{}({})",
                &udf.func_name,
                udf.arguments
                    .iter()
                    .map(|arg| { self.format_scalar(arg) })
                    .collect::<Vec<String>>()
                    .join(", ")
            ),

            _ => unreachable!(), // Window function and subquery will not appear in index.
        }
    }
}

fn collect_information(s_expr: &SExpr) -> Result<RewriteInfomartion<'_>> {
    // The plan tree should be started with [`EvalScalar`].
    if let RelOperator::EvalScalar(eval) = s_expr.plan() {
        let mut info = RewriteInfomartion {
            table_index: 0,
            selection: eval,
            predicates: None,
            sort_items: None,
            aggregation: None,
        };
        collect_information_impl(s_expr.child(0)?, &mut info)?;
        return Ok(info);
    }

    unreachable!()
}

fn collect_information_impl<'a>(
    s_expr: &'a SExpr,
    info: &mut RewriteInfomartion<'a>,
) -> Result<()> {
    match s_expr.plan() {
        RelOperator::Aggregate(agg) => {
            let child = s_expr.child(0)?;
            if let RelOperator::EvalScalar(eval) = child.plan() {
                // This eval scalar hold aggregation's arguments.
                let inputs = eval
                    .items
                    .iter()
                    .map(|item| (item.index, &item.scalar))
                    .collect();
                info.aggregation.replace((agg, inputs));
                collect_information_impl(child.child(0)?, info)
            } else {
                collect_information_impl(child, info)
            }
        }
        RelOperator::Sort(sort) => {
            info.sort_items.replace(&sort.items);
            collect_information_impl(s_expr.child(0)?, info)
        }
        RelOperator::Filter(filter) => {
            info.predicates.replace(&filter.predicates);
            collect_information_impl(s_expr.child(0)?, info)
        }
        RelOperator::Scan(scan) => {
            if let Some(prewhere) = &scan.prewhere {
                debug_assert!(info.predicates.is_none());
                info.predicates.replace(&prewhere.predicates);
            }
            info.table_index = scan.table_index;
            // Finish the recursion.
            Ok(())
        }
        _ => collect_information_impl(s_expr.child(0)?, info),
    }
}

/// Collect three kinds of predicates:
/// 1. `Equal`. Such as `column = constant`.
/// 2. `Range`. Such as `column op constant`m `op` should be `gt`, `gte`, `lt` or `lte`.
/// 3. `Other`. Predicates except `Equal` and `Range`.
fn distinguish_predicates(predicates: &[ScalarExpr]) -> Predicates<'_> {
    let mut equal_predicates = vec![];
    let mut range_predicates = HashMap::new();
    let mut other_predicates = vec![];

    for pred in predicates {
        match pred {
            ScalarExpr::FunctionCall(FunctionCall {
                func_name,
                arguments,
                ..
            }) => match func_name.as_str() {
                "eq" => {
                    let left = &arguments[0];
                    let right = &arguments[1];
                    equal_predicates.push((left, right));
                }
                "gt" | "gte" | "lt" | "lte" => {
                    let left = &arguments[0];
                    let right = &arguments[1];
                    match (left, right) {
                        (ScalarExpr::BoundColumnRef(col), ScalarExpr::ConstantExpr(val)) => {
                            range_predicates
                                .entry(col.column.index)
                                .and_modify(|v: &mut Range| v.set_bound(&val.value, func_name))
                                .or_insert(Range::new(&val.value, func_name));
                        }
                        (ScalarExpr::ConstantExpr(val), ScalarExpr::BoundColumnRef(col)) => {
                            range_predicates
                                .entry(col.column.index)
                                .and_modify(|v: &mut Range| v.set_bound(&val.value, func_name))
                                .or_insert(Range::new(&val.value, &reverse_op(func_name)));
                        }
                        _ => other_predicates.push(pred),
                    }
                }
                _ => other_predicates.push(pred),
            },
            _ => other_predicates.push(pred),
        }
    }

    (equal_predicates, range_predicates, other_predicates)
}

#[inline(always)]
fn format_col_name(index: IndexType) -> String {
    format!("col_{index}")
}

#[inline(always)]
fn reverse_op(op: &str) -> String {
    match op {
        "gt" => "lt".to_string(),
        "gte" => "lte".to_string(),
        "lt" => "gt".to_string(),
        "lte" => "gte".to_string(),
        _ => op.to_string(),
    }
}

/// Check if equal predicates of the index fit the query.
///
/// For each predicate of index, it should be in the query.
fn check_predicates_equal(query: &EqualPredicates, index: &EqualPredicates) -> bool {
    // TBD: if there is a better way.
    for (left, right) in index {
        if !query
            .iter()
            .any(|(l, r)| (l == left && r == right) || (l == right && r == left))
        {
            return false;
        }
    }
    true
}

/// Check if range predicates of the index fit the query.
///
/// For each predicate of query, the column side should be found in the index.
/// And the range of the predicate in index should be more wide than the one in query.
///
/// For example:
///
/// - Valid: query predicate: `a > 1`, index predicate: `a > 0`
/// - Invalid: query predicate: `a > 1`, index predicate: `a > 2`
///
/// Returns an [`Option`]:
///
/// - If not matched, returns [None].
/// - If matched, returns columns need to be filtered.
fn check_predicates_range(
    query: &RangePredicates,
    index: &RangePredicates,
    index_output_bound_cols: &ColumnSet,
    index_selection: &SelectionMap<'_>,
) -> Option<Vec<ScalarExpr>> {
    let mut out = Vec::new();
    for (col, query_range) in query {
        if let Some(index_range) = index.get(col) {
            if !index_range.contains(query_range) {
                return None;
            }
            // If query range is not equal to index range,
            // we need to filter the index data.
            // So we need to check if the columns in query predicates exist in index output columns.
            if index_range != query_range {
                if !index_output_bound_cols.contains(col) {
                    return None;
                }
                let (new_index, ty) = &index_selection[&format_col_name(*col)];
                out.push((*col, *new_index, ty))
            }
        } else if !index_output_bound_cols.contains(col) {
            // If the column is not in index predicates, it should be in index output columns.
            return None;
        } else {
            let (new_index, ty) = &index_selection[&format_col_name(*col)];
            out.push((*col, *new_index, ty))
        }
    }

    Some(
        out.iter()
            .filter_map(|(col, new_index, ty)| {
                let range = &query[col];
                Some(split_conjunctions(&range.to_scalar(*new_index, ty)?))
            })
            .flatten()
            .collect(),
    )
}

/// Check if other predicates of the index fit the query.
///
/// For each predicate of index, it should be in the query.
fn check_predicates_other(query: &OtherPredicates, index: &OtherPredicates) -> bool {
    // TBD: if there is a better way.
    for pred in index {
        if !query.iter().any(|p| p == pred) {
            return false;
        }
    }
    true
}

fn try_create_column_binding(
    index_selection: &SelectionMap<'_>,
    formatted_scalar: &str,
) -> Option<BoundColumnRef> {
    if let Some((index, ty)) = index_selection.get(formatted_scalar) {
        Some(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                format!("index_col_{index}"),
                *index,
                Box::new(ty.clone()),
                Visibility::Visible,
            )
            .build(),
        })
    } else {
        None
    }
}

fn rewrite_by_selection(
    query_info: &RewriteInfomartion<'_>,
    scalar: &ScalarExpr,
    index_selection: &SelectionMap<'_>,
) -> Option<ScalarExpr> {
    if let Some(col) = try_create_column_binding(index_selection, &query_info.format_scalar(scalar))
    {
        Some(col.into())
    } else {
        rewrite_query_item(query_info, scalar, index_selection)
    }
}

/// Check if `query_item` scalar contains output from index,
/// and rewrite scalar with output from index.
///
/// If `query_item` contains items that are not in index outputs,
/// returns [None];
/// else returns the rewritten scalar.
fn rewrite_query_item(
    query_info: &RewriteInfomartion<'_>,
    query_item: &ScalarExpr,
    index_selection: &SelectionMap<'_>,
) -> Option<ScalarExpr> {
    // Every call will format the scalars,
    // a more efficient way to be determined.
    match query_item {
        ScalarExpr::BoundColumnRef(col) => Some(
            try_create_column_binding(index_selection, &format_col_name(col.column.index))?.into(),
        ),
        ScalarExpr::ConstantExpr(_) => Some(query_item.clone()),
        ScalarExpr::CastExpr(cast) => {
            let new_arg = rewrite_by_selection(query_info, &cast.argument, index_selection)?;
            Some(
                CastExpr {
                    span: None,
                    is_try: cast.is_try,
                    argument: Box::new(new_arg),
                    target_type: cast.target_type.clone(),
                }
                .into(),
            )
        }
        ScalarExpr::FunctionCall(func) => {
            let mut new_args = Vec::with_capacity(func.arguments.len());
            for arg in func.arguments.iter() {
                let new_arg = rewrite_by_selection(query_info, arg, index_selection)?;
                new_args.push(new_arg);
            }
            Some(
                FunctionCall {
                    span: None,
                    func_name: func.func_name.clone(),
                    params: func.params.clone(),
                    arguments: new_args,
                }
                .into(),
            )
        }
        ScalarExpr::UDFCall(udf) => {
            let mut new_args = Vec::with_capacity(udf.arguments.len());
            for arg in udf.arguments.iter() {
                let new_arg = rewrite_by_selection(query_info, arg, index_selection)?;
                new_args.push(new_arg);
            }
            Some(
                UDFCall {
                    span: udf.span,
                    name: udf.name.clone(),
                    func_name: udf.func_name.clone(),
                    display_name: udf.display_name.clone(),
                    udf_type: udf.udf_type.clone(),
                    arg_types: udf.arg_types.clone(),
                    return_type: udf.return_type.clone(),
                    arguments: new_args,
                }
                .into(),
            )
        }
        // TODO UDF interpreter
        ScalarExpr::AggregateFunction(_) => None, /* Aggregate function must appear in index selection. */
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
