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

use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ConstantFolder;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::function_stat::DomainStatBounds;
use databend_common_expression::stat_distribution::NdvEstimate;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use educe::Educe;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use super::AggregateFunction;
use super::NthValueFunction;
use crate::ColumnSet;
use crate::ScalarExpr;
use crate::Symbol;
use crate::binder::WindowOrderByInfo;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::cap_stat_info_by_rows;
use crate::plans::EvalScalar;
use crate::plans::LagLeadFunction;
use crate::plans::NtileFunction;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarItem;

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Window {
    #[educe(PartialEq(ignore), Hash(ignore))]
    pub span: Span,

    // aggregate scalar expressions, such as: sum(col1), count(*);
    // or general window functions, such as: row_number(), rank();
    pub index: Symbol,
    pub function: WindowFuncType,
    pub arguments: Vec<ScalarItem>,

    // partition by scalar expressions
    pub partition_by: Vec<ScalarItem>,
    // order by
    pub order_by: Vec<WindowOrderByInfo>,
    // window frames
    pub frame: WindowFuncFrame,
    // limit for potentially possible push-down
    pub limit: Option<usize>,
    // per-partition top-n for ranking window push-down
    pub top: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WindowGroup {
    pub windows: Vec<Window>,
    pub scalar_items: Vec<ScalarItem>,
}

impl WindowGroup {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();

        for item in &self.scalar_items {
            used_columns.insert(item.index);
            item.scalar.collect_used_columns(&mut used_columns);
        }

        for window in &self.windows {
            window.collect_used_columns(&mut used_columns)?;
        }

        Ok(used_columns)
    }

    fn required_distribution(&self, rel_expr: &RelExpr) -> Result<Distribution> {
        let Some(first) = self.windows.first() else {
            return Ok(Distribution::Any);
        };

        if first.partition_by.is_empty() {
            return Ok(Distribution::Serial);
        }

        let partition_by = first
            .partition_by
            .iter()
            .map(|item| item.scalar.clone())
            .collect::<Vec<_>>();
        if !self.windows.iter().all(|window| {
            window
                .partition_by
                .iter()
                .map(|item| &item.scalar)
                .eq(partition_by.iter())
        }) {
            return Ok(Distribution::Serial);
        }

        let child_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        if child_physical_prop.distribution == Distribution::Serial {
            return Ok(Distribution::Serial);
        }

        Ok(Distribution::GlobalHash(partition_by))
    }
}

impl Window {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        self.collect_used_columns(&mut used_columns)?;
        Ok(used_columns)
    }

    pub fn collect_used_columns(&self, used_columns: &mut ColumnSet) -> Result<()> {
        used_columns.insert(self.index);
        self.function.collect_used_columns(used_columns);
        for arg in &self.arguments {
            used_columns.insert(arg.index);
            arg.scalar.collect_used_columns(used_columns);
        }
        for part in &self.partition_by {
            used_columns.insert(part.index);
            part.scalar.collect_used_columns(used_columns);
        }
        for sort in &self.order_by {
            used_columns.insert(sort.order_by_item.index);
            sort.order_by_item.scalar.collect_used_columns(used_columns);
        }

        Ok(())
    }

    pub fn arguments_columns(&self) -> Result<ColumnSet> {
        let mut col_set = ColumnSet::new();
        for arg in self.arguments.iter() {
            col_set.insert(arg.index);
            arg.scalar.collect_used_columns(&mut col_set);
        }
        Ok(col_set)
    }

    // `Window.partition_by_columns` used in `RulePushDownFilterWindow` only consider `partition_by` field,
    // like `Aggregate.group_columns` only consider `group_items` field.
    pub fn partition_by_columns(&self) -> Result<ColumnSet> {
        let mut col_set = ColumnSet::new();
        for part in self.partition_by.iter() {
            col_set.insert(part.index);
            part.scalar.collect_used_columns(&mut col_set);
        }
        Ok(col_set)
    }

    pub fn order_by_columns(&self) -> Result<ColumnSet> {
        let mut col_set = ColumnSet::new();
        for sort in self.order_by.iter() {
            col_set.insert(sort.order_by_item.index);
            sort.order_by_item.scalar.collect_used_columns(&mut col_set);
        }
        Ok(col_set)
    }

    fn derive_output_stat(&self, stat_info: &StatInfo) -> Result<Option<ColumnStat>> {
        if stat_info.cardinality == 0.0 {
            return Ok(None);
        }

        match &self.function {
            WindowFuncType::RowNumber => Ok(Some(self.derive_row_number_stat(stat_info))),
            WindowFuncType::Rank | WindowFuncType::DenseRank => {
                Ok(Some(self.derive_rank_stat(stat_info)))
            }
            WindowFuncType::Ntile(ntile) => Ok(Some(self.derive_ntile_stat(stat_info, ntile.n))),
            WindowFuncType::LagLead(lag_lead) => self.derive_lag_lead_stat(stat_info, lag_lead),
            _ => Ok(None),
        }
    }

    fn derive_row_number_stat(&self, stat_info: &StatInfo) -> ColumnStat {
        let upper = self.ranking_upper(stat_info);
        let partitions = self.estimated_partition_count(stat_info);
        let expected = (stat_info.cardinality / partitions)
            .ceil()
            .clamp(1.0, upper as f64);
        ColumnStat::UInt {
            min: 1,
            max: upper,
            ndv: NdvEstimate::new(expected, upper as f64),
            null_count: StatCount::exact(0),
            histogram: None,
        }
    }

    fn derive_rank_stat(&self, stat_info: &StatInfo) -> ColumnStat {
        let upper = self.ranking_upper(stat_info);
        ColumnStat::UInt {
            min: 1,
            max: upper,
            ndv: NdvEstimate::upper_bound(upper as f64),
            null_count: StatCount::exact(0),
            histogram: None,
        }
    }

    fn derive_ntile_stat(&self, stat_info: &StatInfo, buckets: u64) -> ColumnStat {
        let upper = buckets.min(cardinality_upper(stat_info.cardinality)).max(1);
        ColumnStat::UInt {
            min: 1,
            max: upper,
            ndv: NdvEstimate::upper_bound(upper as f64),
            null_count: StatCount::exact(0),
            histogram: None,
        }
    }

    fn derive_lag_lead_stat(
        &self,
        stat_info: &StatInfo,
        lag_lead: &LagLeadFunction,
    ) -> Result<Option<ColumnStat>> {
        let cardinality = stat_cardinality(stat_info);
        let Some(mut output) =
            self.derive_scalar_stat(lag_lead.arg.as_ref(), stat_info, cardinality)?
        else {
            return Ok(None);
        };

        if lag_lead.offset == 0 {
            return Ok(Some(output));
        }

        let boundary_rows = (self.estimated_partition_count(stat_info) * lag_lead.offset as f64)
            .min(stat_info.cardinality);
        let source_rows = (stat_info.cardinality - boundary_rows).max(0.0);
        let argument_may_be_null = output.null_count().upper() > 0.0;
        let argument_null_rate = output.null_count().expected() / stat_info.cardinality;
        let expected_nulls_from_argument = argument_null_rate * source_rows;

        let (expected_null_count, default_may_be_null) = if let Some(default) = &lag_lead.default {
            match self.derive_default_stat(default.as_ref(), stat_info, cardinality)? {
                Some(FoldedConstantStat::Value(default_stat)) => {
                    let default_may_be_null = default_stat.null_count().upper() > 0.0;
                    let default_null_rate =
                        default_stat.null_count().expected() / stat_info.cardinality;
                    output = merge_column_stats(output, default_stat, stat_info.cardinality)?;
                    (
                        expected_nulls_from_argument + default_null_rate * boundary_rows,
                        default_may_be_null,
                    )
                }
                Some(FoldedConstantStat::Null) => {
                    (expected_nulls_from_argument + boundary_rows, true)
                }
                None => return Ok(None),
            }
        } else {
            (expected_nulls_from_argument + boundary_rows, true)
        };

        let nullable = argument_may_be_null || default_may_be_null;
        output.set_null_count(if nullable {
            StatCount::estimate(
                expected_null_count.min(stat_info.cardinality),
                stat_info.cardinality,
            )
        } else {
            StatCount::exact(0)
        });
        output.set_ndv(output.ndv().reduce(stat_info.cardinality));
        output.clear_histogram();
        Ok(Some(output))
    }

    fn derive_scalar_stat(
        &self,
        scalar: &ScalarExpr,
        stat_info: &StatInfo,
        cardinality: StatCardinality,
    ) -> Result<Option<ColumnStat>> {
        if let Some(stat) =
            EvalScalar::derive_item_stat(scalar, &stat_info.statistics, cardinality)?
        {
            return Ok(Some(stat));
        }

        let source = self.scalar_source(scalar);
        if let Some(stat) =
            EvalScalar::derive_item_stat(source, &stat_info.statistics, cardinality)?
        {
            return Ok(Some(stat));
        }

        Ok(match fold_constant_stat(source)? {
            Some(FoldedConstantStat::Value(stat)) => Some(stat),
            Some(FoldedConstantStat::Null) | None => None,
        })
    }

    fn derive_default_stat(
        &self,
        scalar: &ScalarExpr,
        stat_info: &StatInfo,
        cardinality: StatCardinality,
    ) -> Result<Option<FoldedConstantStat>> {
        if let Some(stat) =
            EvalScalar::derive_item_stat(scalar, &stat_info.statistics, cardinality)?
        {
            return Ok(Some(FoldedConstantStat::Value(stat)));
        }

        let source = self.scalar_source(scalar);
        if let Some(stat) =
            EvalScalar::derive_item_stat(source, &stat_info.statistics, cardinality)?
        {
            return Ok(Some(FoldedConstantStat::Value(stat)));
        }

        fold_constant_stat(source)
    }

    fn scalar_source<'a>(&'a self, scalar: &'a ScalarExpr) -> &'a ScalarExpr {
        let ScalarExpr::BoundColumnRef(column) = scalar else {
            return scalar;
        };

        self.arguments
            .iter()
            .find(|item| item.index == column.column.index)
            .map(|item| &item.scalar)
            .unwrap_or(scalar)
    }

    fn ranking_upper(&self, stat_info: &StatInfo) -> u64 {
        self.top
            .map(|top| top as u64)
            .unwrap_or_else(|| cardinality_upper(stat_info.cardinality))
            .min(cardinality_upper(stat_info.cardinality))
            .max(1)
    }

    fn estimated_partition_count(&self, stat_info: &StatInfo) -> f64 {
        if self.partition_by.is_empty() {
            return 1.0;
        }

        let cardinality_upper = stat_info.cardinality.max(1.0);

        self.partition_by
            .iter()
            .try_fold(1.0, |partitions, item| {
                let ndv = stat_info
                    .statistics
                    .column_stats
                    .get(&item.index)?
                    .ndv()
                    .expected?;
                Some((partitions * ndv.max(1.0)).min(cardinality_upper))
            })
            .unwrap_or(1.0)
            .clamp(1.0, cardinality_upper)
    }
}

fn cardinality_upper(cardinality: f64) -> u64 {
    cardinality.ceil().max(1.0) as u64
}

enum FoldedConstantStat {
    Null,
    Value(ColumnStat),
}

fn fold_constant_stat(expr: &ScalarExpr) -> Result<Option<FoldedConstantStat>> {
    let expr = expr.as_expr()?;
    let (expr, _) = ConstantFolder::fold(&expr, &FunctionContext::default(), &BUILTIN_FUNCTIONS);
    let Ok(constant) = expr.into_constant() else {
        return Ok(None);
    };
    if constant.scalar == Scalar::Null {
        return Ok(Some(FoldedConstantStat::Null));
    }
    let DomainStatBounds::Bounds(bounds) = constant
        .scalar
        .as_ref()
        .domain(&constant.data_type)
        .stat_bounds()
    else {
        return Ok(None);
    };
    let stat = ColumnStat::new(bounds, NdvEstimate::exact(1.0), StatCount::exact(0), None)
        .map_err(ErrorCode::Internal)?;
    Ok(Some(FoldedConstantStat::Value(stat)))
}

fn stat_cardinality(stat_info: &StatInfo) -> StatCardinality {
    stat_info
        .statistics
        .precise_cardinality
        .map(StatCardinality::exact)
        .unwrap_or_else(|| StatCardinality::estimate(stat_info.cardinality))
}

fn merge_column_stats(left: ColumnStat, right: ColumnStat, cardinality: f64) -> Result<ColumnStat> {
    let left_bounds = left.bounds();
    let right_bounds = right.bounds();
    let (left_bounds, right_bounds) = match (left_bounds, right_bounds) {
        (None, None) => {
            return Ok(ColumnStat::AllNull {
                null_count: StatCount::sum(left.null_count(), right.null_count()),
            });
        }
        (None, Some(_)) => {
            return Ok(merge_all_null_with_values(right, cardinality));
        }
        (Some(_), None) => {
            return Ok(merge_all_null_with_values(left, cardinality));
        }
        (Some(left), Some(right)) => (left, right),
    };
    let left_ndv = left.ndv();
    let right_ndv = right.ndv();
    let ndv_upper = (left_ndv.upper + right_ndv.upper).min(cardinality);
    let ndv = match (left_ndv.expected, right_ndv.expected) {
        (Some(left), Some(right)) => NdvEstimate::new((left + right).min(ndv_upper), ndv_upper),
        _ => NdvEstimate::upper_bound(ndv_upper),
    };
    ColumnStat::new(
        left_bounds.union(right_bounds)?,
        ndv,
        StatCount::exact(0),
        None,
    )
    .map_err(ErrorCode::Internal)
}

fn merge_all_null_with_values(mut values: ColumnStat, cardinality: f64) -> ColumnStat {
    values.set_null_count(StatCount::exact(0));
    values.set_ndv(values.ndv().reduce(cardinality));
    values.clear_histogram();
    values
}

impl Operator for WindowGroup {
    fn rel_op(&self) -> RelOp {
        RelOp::WindowGroup
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        let scalar_items = self.scalar_items.iter().map(|expr| &expr.scalar);
        let windows = self
            .windows
            .iter()
            .flat_map(|window| window.scalar_expr_iter());
        Box::new(scalar_items.chain(windows))
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();
        match self.required_distribution(rel_expr)? {
            Distribution::Any => {}
            distribution => required.distribution = distribution,
        }
        Ok(required)
    }

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        let mut required = required.clone();
        match self.required_distribution(rel_expr)? {
            Distribution::Any => {}
            distribution => required.distribution = distribution,
        }
        Ok(vec![vec![required]])
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        let mut output_columns = input_prop.output_columns.clone();
        for item in &self.scalar_items {
            output_columns.insert(item.index);
        }
        for window in &self.windows {
            output_columns.insert(window.index);
        }

        let outer_columns = input_prop
            .outer_columns
            .difference(&output_columns)
            .cloned()
            .collect();

        let mut used_columns = self.used_columns()?;
        used_columns.extend(input_prop.used_columns.clone());

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings: input_prop.orderings.clone(),
            partition_orderings: input_prop.partition_orderings.clone(),
        }))
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let input = rel_expr.derive_cardinality_child(0)?;
        let mut stat_info = input.as_ref().clone();
        let cardinality = stat_cardinality(&stat_info);
        for item in &self.scalar_items {
            if let Some(stat) =
                EvalScalar::derive_item_stat(&item.scalar, &stat_info.statistics, cardinality)?
            {
                stat_info.statistics.column_stats.insert(item.index, stat);
            }
        }
        for window in &self.windows {
            if let Some(stat) = window.derive_output_stat(&stat_info)? {
                stat_info.statistics.column_stats.insert(window.index, stat);
            }
        }
        Ok(Arc::new(stat_info))
    }
}

impl Operator for Window {
    fn rel_op(&self) -> RelOp {
        RelOp::Window
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        let iter = self.order_by.iter().map(|o| &o.order_by_item.scalar);
        let iter = iter.chain(self.partition_by.iter().map(|expr| &expr.scalar));
        let iter = iter.chain(self.arguments.iter().map(|expr| &expr.scalar));

        match &self.function {
            WindowFuncType::Aggregate(agg) => Box::new(iter.chain(agg.exprs())),
            WindowFuncType::LagLead(lag_lead_function) => {
                Box::new(iter.chain(std::iter::once(lag_lead_function.arg.as_ref())))
            }
            WindowFuncType::NthValue(nth_value_function) => {
                Box::new(iter.chain(std::iter::once(nth_value_function.arg.as_ref())))
            }
            _ => Box::new(iter),
        }
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();
        if self.partition_by.is_empty() {
            required.distribution = Distribution::Serial;
        }
        Ok(required.clone())
    }

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        let mut required = required.clone();
        if self.partition_by.is_empty() {
            required.distribution = Distribution::Serial;
        }
        Ok(vec![vec![required.clone()]])
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let mut output_columns = input_prop.output_columns.clone();
        output_columns.insert(self.index);

        // Derive outer columns
        let outer_columns = input_prop
            .outer_columns
            .difference(&output_columns)
            .cloned()
            .collect();

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(input_prop.used_columns.clone());

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
        let mut stat_info = input.as_ref().clone();
        if let Some(stat) = self.derive_output_stat(&stat_info)? {
            stat_info.statistics.column_stats.insert(self.index, stat);
        }
        if let Some(limit) = self.limit {
            stat_info = cap_stat_info_by_rows(stat_info, limit);
        }
        Ok(Arc::new(stat_info))
    }
}

#[derive(Default, Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct WindowFuncFrame {
    pub units: WindowFuncFrameUnits,
    pub start_bound: WindowFuncFrameBound,
    pub end_bound: WindowFuncFrameBound,
}

impl Display for WindowFuncFrame {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:?}: {:?} ~ {:?}",
            self.units, self.start_bound, self.end_bound
        )
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, EnumAsInner)]
pub enum WindowFuncFrameUnits {
    #[default]
    Rows,
    Range,
}

#[derive(Default, Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum WindowFuncFrameBound {
    /// `CURRENT ROW`
    #[default]
    CurrentRow,
    /// `<N> PRECEDING` or `UNBOUNDED PRECEDING`
    Preceding(Option<Scalar>),
    /// `<N> FOLLOWING` or `UNBOUNDED FOLLOWING`.
    Following(Option<Scalar>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum WindowFuncType {
    Aggregate(AggregateFunction),
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    LagLead(LagLeadFunction),
    NthValue(NthValueFunction),
    Ntile(NtileFunction),
    CumeDist,
}

impl WindowFuncType {
    pub fn from_name(name: &str) -> Result<WindowFuncType> {
        match name {
            "row_number" => Ok(WindowFuncType::RowNumber),
            "rank" => Ok(WindowFuncType::Rank),
            "dense_rank" => Ok(WindowFuncType::DenseRank),
            "percent_rank" => Ok(WindowFuncType::PercentRank),
            "cume_dist" => Ok(WindowFuncType::CumeDist),
            _ => Err(ErrorCode::UnknownFunction(format!(
                "Unknown window function: {}",
                name
            ))),
        }
    }

    pub fn func_name(&self) -> String {
        match self {
            WindowFuncType::Aggregate(agg) => agg.func_name.to_string(),
            WindowFuncType::RowNumber => "row_number".to_string(),
            WindowFuncType::Rank => "rank".to_string(),
            WindowFuncType::DenseRank => "dense_rank".to_string(),
            WindowFuncType::PercentRank => "percent_rank".to_string(),
            WindowFuncType::LagLead(lag_lead) if lag_lead.is_lag => "lag".to_string(),
            WindowFuncType::LagLead(_) => "lead".to_string(),
            WindowFuncType::NthValue(_) => "nth_value".to_string(),
            WindowFuncType::Ntile(_) => "ntile".to_string(),
            WindowFuncType::CumeDist => "cume_dist".to_string(),
        }
    }

    pub fn used_columns(&self) -> ColumnSet {
        let mut used_columns = ColumnSet::new();
        self.collect_used_columns(&mut used_columns);
        used_columns
    }

    pub fn collect_used_columns(&self, used_columns: &mut ColumnSet) {
        match self {
            WindowFuncType::Aggregate(agg) => {
                for expr in agg.exprs() {
                    expr.collect_used_columns(used_columns);
                }
            }
            WindowFuncType::LagLead(func) => {
                func.arg.collect_used_columns(used_columns);
                if let Some(default) = &func.default {
                    default.collect_used_columns(used_columns);
                }
            }
            WindowFuncType::NthValue(func) => func.arg.collect_used_columns(used_columns),
            _ => {}
        }
    }

    pub fn return_type(&self) -> DataType {
        match self {
            WindowFuncType::Aggregate(agg) => *agg.return_type.clone(),
            WindowFuncType::RowNumber | WindowFuncType::Rank | WindowFuncType::DenseRank => {
                DataType::Number(NumberDataType::UInt64)
            }
            WindowFuncType::PercentRank | WindowFuncType::CumeDist => {
                DataType::Number(NumberDataType::Float64)
            }
            WindowFuncType::LagLead(lag_lead) => *lag_lead.return_type.clone(),
            WindowFuncType::NthValue(nth_value) => *nth_value.return_type.clone(),
            WindowFuncType::Ntile(buckets) => *buckets.return_type.clone(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct WindowPartition {
    pub partition_by: Vec<ScalarItem>,
    pub top: Option<usize>,
    pub func: WindowFuncType,
}
