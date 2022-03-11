// Copyright 2021 Datafuse Labs.
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
use std::fmt;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::check_pattern_type;
use common_functions::scalars::FunctionFactory;
use common_functions::scalars::PatternType;
use common_planners::lit;
use common_planners::Expression;
use common_planners::ExpressionMonotonicityVisitor;
use common_planners::Expressions;
use common_planners::RequireColumnsVisitor;

use crate::pipelines::transforms::ExpressionExecutor;

pub type BlockStatistics = HashMap<u32, ColumnStatistics>;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct ColumnStatistics {
    pub min: DataValue,
    pub max: DataValue,
    pub null_count: u64,
    pub in_memory_size: u64,
}

#[derive(Debug, Clone)]
pub struct RangeFilter {
    origin: DataSchemaRef,
    schema: DataSchemaRef,
    executor: Arc<ExpressionExecutor>,
    stat_columns: StatColumns,
}

impl RangeFilter {
    pub fn try_create(expr: &Expression, schema: DataSchemaRef) -> Result<Self> {
        let mut stat_columns: StatColumns = Vec::new();
        let verifiable_expr = build_verifiable_expr(expr, &schema, &mut stat_columns);
        let input_fields = stat_columns
            .iter()
            .map(|c| c.stat_field.clone())
            .collect::<Vec<_>>();
        let input_schema = Arc::new(DataSchema::new(input_fields));

        let output_fields = vec![verifiable_expr.to_data_field(&input_schema)?];
        let output_schema = DataSchemaRefExt::create(output_fields);
        let expr_executor = ExpressionExecutor::try_create(
            "verifiable expression executor in RangeFilter",
            input_schema.clone(),
            output_schema,
            vec![verifiable_expr],
            false,
        )?;

        Ok(Self {
            origin: schema,
            schema: input_schema,
            executor: Arc::new(expr_executor),
            stat_columns,
        })
    }

    pub fn eval(&self, stats: &BlockStatistics) -> Result<bool> {
        let mut columns = Vec::with_capacity(self.stat_columns.len());
        for col in self.stat_columns.iter() {
            let val_opt = col.apply_stat_value(stats, self.origin.clone())?;
            if val_opt.is_none() {
                return Ok(true);
            }
            columns.push(val_opt.unwrap());
        }
        let data_block = DataBlock::create(self.schema.clone(), columns);
        let executed_data_block = self.executor.execute(&data_block)?;

        match executed_data_block.column(0).get(0) {
            DataValue::Null => Ok(false),
            other => other.as_bool(),
        }
    }
}

/// convert expr to Verifiable Expression
/// Rules: (section 5.2 of http://vldb.org/pvldb/vol14/p3083-edara.pdf)
pub fn build_verifiable_expr(
    expr: &Expression,
    schema: &DataSchemaRef,
    stat_columns: &mut StatColumns,
) -> Expression {
    let unhandled = lit(true);

    let (exprs, op) = match expr {
        Expression::Literal { .. } => return expr.clone(),
        Expression::ScalarFunction { op, args } => (args.clone(), op.clone()),
        Expression::BinaryExpression { left, op, right } => match op.to_lowercase().as_str() {
            "and" => {
                let left = build_verifiable_expr(left, schema, stat_columns);
                let right = build_verifiable_expr(right, schema, stat_columns);
                return left.and(right);
            }
            "or" => {
                let left = build_verifiable_expr(left, schema, stat_columns);
                let right = build_verifiable_expr(right, schema, stat_columns);
                return left.or(right);
            }
            _ => (
                vec![left.as_ref().clone(), right.as_ref().clone()],
                op.clone(),
            ),
        },
        _ => return unhandled,
    };

    VerifiableExprBuilder::try_create(exprs, op.to_lowercase().as_str(), schema, stat_columns)
        .map_or(unhandled.clone(), |mut v| v.build().unwrap_or(unhandled))
}

fn inverse_operator(op: &str) -> Result<&str> {
    match op {
        "<" => Ok(">"),
        "<=" => Ok(">="),
        ">" => Ok("<"),
        ">=" => Ok("<="),
        "like" | "not like" | "ilike" | "not ilike" => Err(ErrorCode::UnknownException(format!(
            "cannot inverse the operator: {:?}",
            op
        ))),
        _ => Ok(op),
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum StatType {
    Min,
    Max,
    Nulls,
}

impl fmt::Display for StatType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StatType::Min => write!(f, "min"),
            StatType::Max => write!(f, "max"),
            StatType::Nulls => write!(f, "nulls"),
        }
    }
}

pub type StatColumns = Vec<StatColumn>;
pub type ColumnFields = HashMap<u32, DataField>;

#[derive(Debug, Clone)]
pub struct StatColumn {
    column_fields: ColumnFields,
    stat_type: StatType,
    stat_field: DataField,
    expr: Expression,
}

impl StatColumn {
    fn create(
        column_fields: ColumnFields,
        stat_type: StatType,
        field: &DataField,
        expr: Expression,
    ) -> Self {
        let column_new = format!("{}_{}", stat_type, field.name());
        let data_type = if matches!(stat_type, StatType::Nulls) {
            u64::to_data_type()
        } else {
            field.data_type().clone()
        };
        let stat_field = DataField::new(column_new.as_str(), data_type);

        Self {
            column_fields,
            stat_type,
            stat_field,
            expr,
        }
    }

    fn apply_stat_value(
        &self,
        stats: &BlockStatistics,
        schema: DataSchemaRef,
    ) -> Result<Option<ColumnRef>> {
        if self.stat_type == StatType::Nulls {
            // The len of column_fields is 1.
            let (k, _) = self.column_fields.iter().next().unwrap();
            let stat = stats.get(k).ok_or_else(|| {
                ErrorCode::UnknownException(format!(
                    "Unable to get the colStats by ColumnId: {}",
                    k
                ))
            })?;
            return Ok(Some(Series::from_data(vec![stat.null_count])));
        }

        let mut single_point = true;
        let mut variables = HashMap::with_capacity(self.column_fields.len());
        for (k, v) in &self.column_fields {
            let stat = stats.get(k).ok_or_else(|| {
                ErrorCode::UnknownException(format!(
                    "Unable to get the colStats by ColumnId: {}",
                    k
                ))
            })?;

            if single_point && stat.min != stat.max {
                single_point = false;
            }

            let min_col = v.data_type().create_constant_column(&stat.min, 1)?;
            let variable_left = Some(ColumnWithField::new(min_col, v.clone()));

            let max_col = v.data_type().create_constant_column(&stat.max, 1)?;
            let variable_right = Some(ColumnWithField::new(max_col, v.clone()));
            variables.insert(v.name().clone(), (variable_left, variable_right));
        }

        let monotonicity = ExpressionMonotonicityVisitor::check_expression(
            schema,
            &self.expr,
            variables,
            single_point,
        );
        if !monotonicity.is_monotonic {
            return Ok(None);
        }

        let column_with_field_opt = match self.stat_type {
            StatType::Min => {
                if monotonicity.is_positive {
                    monotonicity.left
                } else {
                    monotonicity.right
                }
            }
            StatType::Max => {
                if monotonicity.is_positive {
                    monotonicity.right
                } else {
                    monotonicity.left
                }
            }
            _ => unreachable!(),
        };

        Ok(column_with_field_opt.map(|v| v.column().slice(0, 1)))
    }
}

struct VerifiableExprBuilder<'a> {
    op: &'a str,
    args: Expressions,
    fields: Vec<(DataField, ColumnFields)>,
    stat_columns: &'a mut StatColumns,
}

impl<'a> VerifiableExprBuilder<'a> {
    fn try_create(
        exprs: Expressions,
        op: &'a str,
        schema: &'a DataSchemaRef,
        stat_columns: &'a mut StatColumns,
    ) -> Result<Self> {
        let (args, cols, op) = match exprs.len() {
            1 => {
                let cols = RequireColumnsVisitor::collect_columns_from_expr(&exprs[0])?;
                match cols.len() {
                    1 => (exprs, vec![cols], op),
                    _ => {
                        return Err(ErrorCode::UnknownException(
                            "Multi-column expressions are not currently supported",
                        ));
                    }
                }
            }
            2 => {
                let lhs_cols = RequireColumnsVisitor::collect_columns_from_expr(&exprs[0])?;
                let rhs_cols = RequireColumnsVisitor::collect_columns_from_expr(&exprs[1])?;
                match (lhs_cols.len(), rhs_cols.len()) {
                    (0, 0) => {
                        return Err(ErrorCode::UnknownException(
                            "Constant expression donot need to be handled",
                        ))
                    }
                    (_, 0) => (vec![exprs[0].clone(), exprs[1].clone()], vec![lhs_cols], op),
                    (0, _) => {
                        let op = inverse_operator(op)?;
                        (vec![exprs[1].clone(), exprs[0].clone()], vec![rhs_cols], op)
                    }
                    _ => {
                        if !lhs_cols.is_disjoint(&rhs_cols) {
                            return Err(ErrorCode::UnknownException(
                                "Unsupported condition for left and right have same columns",
                            ));
                        }

                        if !matches!(op, "=" | "<" | "<=" | ">" | ">=") {
                            return Err(ErrorCode::UnknownException(format!(
                                "Unsupported operator '{:?}' for multi-column expression",
                                op
                            )));
                        }

                        if !check_maybe_monotonic(&exprs[1])? {
                            return Err(ErrorCode::UnknownException(
                                "Only support the monotonic expression",
                            ));
                        }

                        (
                            vec![exprs[0].clone(), exprs[1].clone()],
                            vec![lhs_cols, rhs_cols],
                            op,
                        )
                    }
                }
            }
            _ => {
                return Err(ErrorCode::UnknownException(
                    "Expressions with more than two args are not currently supported",
                ));
            }
        };

        if !check_maybe_monotonic(&args[0])? {
            return Err(ErrorCode::UnknownException(
                "Only support the monotonic expression",
            ));
        }

        let mut fields = Vec::with_capacity(cols.len());

        let left_cols = get_column_fields(schema, cols[0].clone())?;
        let left_field = args[0].to_data_field(schema)?;
        fields.push((left_field, left_cols));

        if cols.len() > 1 {
            let right_cols = get_column_fields(schema, cols[1].clone())?;
            let right_field = args[1].to_data_field(schema)?;
            fields.push((right_field, right_cols));
        }

        Ok(Self {
            op,
            args,
            fields,
            stat_columns,
        })
    }

    fn build(&mut self) -> Result<Expression> {
        // TODO: support in/not in.
        match self.op {
            "isnull" => {
                let nulls_expr = self.nulls_column_expr(0)?;
                let scalar_expr = lit(0u64);
                Ok(nulls_expr.gt(scalar_expr))
            }
            "isnotnull" => {
                let left_min = self.min_column_expr(0)?;
                Ok(Expression::create_scalar_function("isNotNull", vec![
                    left_min,
                ]))
            }
            "=" => {
                // left = right => min_left <= max_right and max_left >= min_right
                let left_min = self.min_column_expr(0)?;
                let left_max = self.max_column_expr(0)?;

                let right_min = if self.fields.len() == 1 {
                    self.args[1].clone()
                } else {
                    self.min_column_expr(1)?
                };
                let right_max = if self.fields.len() == 1 {
                    self.args[1].clone()
                } else {
                    self.max_column_expr(1)?
                };

                Ok(left_min.lt_eq(right_max).and(left_max.gt_eq(right_min)))
            }
            "!=" => {
                let left_min = self.min_column_expr(0)?;
                let left_max = self.max_column_expr(0)?;
                Ok(left_min
                    .not_eq(self.args[1].clone())
                    .or(left_max.not_eq(self.args[1].clone())))
            }
            ">" => {
                // left > right => max_left > min_right
                let left_max = self.max_column_expr(0)?;

                let right_min = if self.fields.len() == 1 {
                    self.args[1].clone()
                } else {
                    self.min_column_expr(1)?
                };

                Ok(left_max.gt(right_min))
            }
            ">=" => {
                // left >= right => max_left >= min_right
                let left_max = self.max_column_expr(0)?;

                let right_min = if self.fields.len() == 1 {
                    self.args[1].clone()
                } else {
                    self.min_column_expr(1)?
                };

                Ok(left_max.gt_eq(right_min))
            }
            "<" => {
                // left < right => min_left < max_right
                let left_min = self.min_column_expr(0)?;

                let right_max = if self.fields.len() == 1 {
                    self.args[1].clone()
                } else {
                    self.max_column_expr(1)?
                };

                Ok(left_min.lt(right_max))
            }
            "<=" => {
                // left <= right => min_left <= max_right
                let left_min = self.min_column_expr(0)?;

                let right_max = if self.fields.len() == 1 {
                    self.args[1].clone()
                } else {
                    self.max_column_expr(1)?
                };

                Ok(left_min.lt_eq(right_max))
            }
            "like" => {
                if let Expression::Literal {
                    value: DataValue::String(v),
                    ..
                } = &self.args[1]
                {
                    // e.g. col like 'a%' => max_col >= 'a' and min_col < 'b'
                    let left = left_bound_for_like_pattern(v);
                    if !left.is_empty() {
                        let right = right_bound_for_like_pattern(left.clone());
                        let max_expr = self.max_column_expr(0)?;
                        if right.is_empty() {
                            return Ok(max_expr.gt_eq(lit(left)));
                        } else {
                            let min_expr = self.min_column_expr(0)?;
                            return Ok(max_expr.gt_eq(lit(left)).and(min_expr.lt(lit(right))));
                        }
                    }
                }
                Err(ErrorCode::UnknownException(
                    "Cannot build atom expression by the operator: like",
                ))
            }
            "not like" => {
                if let Expression::Literal {
                    value: DataValue::String(v),
                    ..
                } = &self.args[1]
                {
                    // Only support such as 'abc' or 'ab%'.
                    match check_pattern_type(v, true) {
                        // e.g. col not like 'abc' => min_col != 'abc' or max_col != 'abc'
                        PatternType::OrdinalStr => {
                            let const_arg = left_bound_for_like_pattern(v);
                            let max_expr = self.max_column_expr(0)?;
                            let min_expr = self.min_column_expr(0)?;
                            return Ok(min_expr
                                .not_eq(lit(const_arg.clone()))
                                .or(max_expr.not_eq(lit(const_arg))));
                        }
                        // e.g. col not like 'ab%' => min_col < 'ab' or max_col >= 'ac'
                        PatternType::EndOfPercent => {
                            let left = left_bound_for_like_pattern(v);
                            if !left.is_empty() {
                                let right = right_bound_for_like_pattern(left.clone());
                                let min_expr = self.min_column_expr(0)?;
                                if right.is_empty() {
                                    return Ok(min_expr.lt(lit(left)));
                                } else {
                                    let max_expr = self.max_column_expr(0)?;
                                    return Ok(min_expr
                                        .lt(lit(left))
                                        .or(max_expr.gt_eq(lit(right))));
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Err(ErrorCode::UnknownException(
                    "Cannot build atom expression by the operator: not like",
                ))
            }
            other => Err(ErrorCode::UnknownException(format!(
                "Cannot build atom expression by the operator: {:?}",
                other
            ))),
        }
    }

    fn stat_column_expr(&mut self, stat_type: StatType, index: usize) -> Result<Expression> {
        let (data_field, column_fields) = self.fields[index].clone();
        let stat_col = StatColumn::create(
            column_fields,
            stat_type,
            &data_field,
            self.args[index].clone(),
        );
        if !self
            .stat_columns
            .iter()
            .any(|c| c.stat_type == stat_type && c.stat_field.name() == data_field.name())
        {
            self.stat_columns.push(stat_col.clone());
        }
        Ok(Expression::Column(stat_col.stat_field.name().to_owned()))
    }

    fn min_column_expr(&mut self, index: usize) -> Result<Expression> {
        self.stat_column_expr(StatType::Min, index)
    }

    fn max_column_expr(&mut self, index: usize) -> Result<Expression> {
        self.stat_column_expr(StatType::Max, index)
    }

    fn nulls_column_expr(&mut self, index: usize) -> Result<Expression> {
        self.stat_column_expr(StatType::Nulls, index)
    }
}

fn is_like_pattern_escape(c: u8) -> bool {
    c == b'%' || c == b'_' || c == b'\\'
}

pub fn left_bound_for_like_pattern(pattern: &[u8]) -> Vec<u8> {
    let mut index = 0;
    let len = pattern.len();
    let mut prefix: Vec<u8> = Vec::with_capacity(len);
    while index < len {
        match pattern[index] {
            b'%' | b'_' => break,
            b'\\' => {
                if index < len - 1 {
                    index += 1;
                    if !is_like_pattern_escape(pattern[index]) {
                        prefix.push(pattern[index - 1]);
                    }
                }
            }
            _ => {}
        }
        prefix.push(pattern[index]);
        index += 1;
    }
    prefix
}

pub fn right_bound_for_like_pattern(prefix: Vec<u8>) -> Vec<u8> {
    let mut res = prefix;
    while !res.is_empty() && *res.last().unwrap() == u8::MAX {
        res.pop();
    }

    if !res.is_empty() {
        if let Some(last) = res.last_mut() {
            *last += 1;
        }
    }

    res
}

fn get_maybe_monotonic(op: &str, args: Expressions) -> Result<bool> {
    let factory = FunctionFactory::instance();
    let function_features = factory.get_features(op)?;
    if !function_features.maybe_monotonic {
        return Ok(false);
    }

    for arg in args {
        if !check_maybe_monotonic(&arg)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn check_maybe_monotonic(expr: &Expression) -> Result<bool> {
    match expr {
        Expression::Literal { .. } => Ok(true),
        Expression::Column { .. } => Ok(true),
        Expression::BinaryExpression { op, left, right } => {
            get_maybe_monotonic(op, vec![left.as_ref().clone(), right.as_ref().clone()])
        }
        Expression::UnaryExpression { op, expr } => {
            get_maybe_monotonic(op, vec![expr.as_ref().clone()])
        }
        Expression::ScalarFunction { op, args } => get_maybe_monotonic(op, args.clone()),
        Expression::Cast { expr, .. } => check_maybe_monotonic(expr),
        _ => Ok(false),
    }
}

fn get_column_fields(schema: &DataSchemaRef, cols: HashSet<String>) -> Result<ColumnFields> {
    let mut column_fields = HashMap::with_capacity(cols.len());
    for col in &cols {
        let (index, field) = schema
            .column_with_name(col.as_str())
            .ok_or_else(|| ErrorCode::UnknownException("Unable to find the column name"))?;
        column_fields.insert(index as u32, field.clone());
    }
    Ok(column_fields)
}
