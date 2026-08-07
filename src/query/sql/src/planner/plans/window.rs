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
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
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
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatInfo;
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

    fn visit_scalar_expr_mut(&mut self, visitor: &mut dyn FnMut(&mut ScalarExpr)) {
        for item in &mut self.scalar_items {
            visitor(&mut item.scalar);
        }
        for window in &mut self.windows {
            window.visit_scalar_expr_mut(visitor);
        }
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
        rel_expr.derive_cardinality_child(0)
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

    fn visit_scalar_expr_mut(&mut self, visitor: &mut dyn FnMut(&mut ScalarExpr)) {
        for item in &mut self.order_by {
            visitor(&mut item.order_by_item.scalar);
        }
        for item in &mut self.partition_by {
            visitor(&mut item.scalar);
        }
        for item in &mut self.arguments {
            visitor(&mut item.scalar);
        }

        match &mut self.function {
            WindowFuncType::Aggregate(aggregate) => {
                for expr in aggregate.exprs_mut() {
                    visitor(expr);
                }
            }
            WindowFuncType::LagLead(function) => {
                visitor(&mut function.arg);
                if let Some(default) = &mut function.default {
                    visitor(default);
                }
            }
            WindowFuncType::NthValue(function) => visitor(&mut function.arg),
            _ => {}
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
        rel_expr.derive_cardinality_child(0)
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
