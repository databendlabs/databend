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
//

use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::lit;
use common_planners::Expression;
use common_planners::Expressions;
use common_planners::RewriteHelper;

use crate::datasources::table::fuse::ColStats;
use crate::datasources::table::fuse::ColumnId;
use crate::optimizers::RequireColumnsVisitor;
use crate::pipelines::transforms::ExpressionExecutor;

#[derive(Debug, Clone)]
pub struct RangeFilter {
    schema: DataSchemaRef,
    executor: Arc<ExpressionExecutor>,
    stat_columns: StatColumns,
}

impl RangeFilter {
    pub fn new(expr: &Expression, schema: DataSchemaRef) -> Result<Self> {
        let mut stat_columns: StatColumns = Vec::new();
        let varifiable_expr = build_verifiable_expr(expr, schema, &mut stat_columns)?;
        let input_fields = stat_columns
            .iter()
            .map(|c| c.stat_field.clone())
            .collect::<Vec<_>>();
        let input_schema = Arc::new(DataSchema::new(input_fields));

        let output_fields = vec![varifiable_expr.to_data_field(&input_schema)?];
        let output_schema = DataSchemaRefExt::create(output_fields);
        let expr_executor = ExpressionExecutor::try_create(
            "variable expression executor in RangeFilter",
            input_schema.clone(),
            output_schema,
            vec![varifiable_expr],
            false,
        )?;

        Ok(Self {
            schema: input_schema,
            executor: Arc::new(expr_executor),
            stat_columns,
        })
    }

    pub fn range_filter(&self, stats: HashMap<ColumnId, ColStats>) -> Result<bool> {
        let data_block = build_stats_block(self.schema.clone(), stats, self.stat_columns.clone())?;
        let executed_data_block = self.executor.execute(&data_block)?;
        assert_eq!(executed_data_block.num_rows(), 1);
        assert_eq!(executed_data_block.num_columns(), 1);

        let value = executed_data_block.column(0).to_values()?.remove(0);
        let val = value
            .to_array()?
            .cast_with_type(&DataType::Boolean)?
            .bool()?
            .inner()
            .value(0);
        Ok(val)
    }
}

fn build_stats_block(
    schema: DataSchemaRef,
    stats: HashMap<ColumnId, ColStats>,
    stat_columns: StatColumns,
) -> Result<DataBlock> {
    let columns = stat_columns
        .iter()
        .map(|c| {
            let stat = stats.get(&c.column_id).ok_or_else(|| {
                ErrorCode::UnknownException(format!(
                    "Unable to get the colStats by ColumnId: {}",
                    c.column_id
                ))
            })?;
            let val = match c.stat_type {
                StatType::Max => stat.max.clone(),
                StatType::Min => stat.min.clone(),
                StatType::Nulls => DataValue::UInt64(Some(stat.null_count as u64)),
                StatType::Rows => DataValue::UInt64(Some(stat.row_count as u64)),
            };
            val.to_array()
        })
        .collect::<Result<Vec<_>>>()?;
    let block = DataBlock::create_by_array(schema, columns);
    Ok(block)
}

fn build_verifiable_expr(
    expr: &Expression,
    schema: DataSchemaRef,
    stat_columns: &mut StatColumns,
) -> Result<Expression> {
    let unhandled = lit(true);
    let (exprs, op) = match expr {
        Expression::Literal { ref value, .. } => {
            let val = value
                .to_array()?
                .cast_with_type(&DataType::Boolean)?
                .bool()?
                .inner()
                .value(0);
            if val {
                return Ok(lit(true));
            } else {
                return Ok(lit(false));
            }
        }
        Expression::ScalarFunction { op, args } => (args.clone(), op.clone()),
        Expression::BinaryExpression { left, op, right } => match op.to_lowercase().as_str() {
            "and" => {
                let left = build_verifiable_expr(left, schema.clone(), stat_columns)?;
                let right = build_verifiable_expr(right, schema, stat_columns)?;
                return Ok(left.and(right));
            }
            "or" => {
                let left = build_verifiable_expr(left, schema.clone(), stat_columns)?;
                let right = build_verifiable_expr(right, schema, stat_columns)?;
                return Ok(left.or(right));
            }
            _ => (
                vec![left.as_ref().clone(), right.as_ref().clone()],
                op.clone(),
            ),
        },
        _ => return Ok(unhandled),
    };

    let res = VerifiableExprBuilder::new(
        exprs,
        op.to_lowercase().as_str(),
        schema.as_ref(),
        stat_columns,
    )
    .map_or(unhandled.clone(), |mut v| v.build().unwrap_or(unhandled));

    Ok(res)
}

// TODO: need add monotonic check for the expression, will move to FunctionFactory.
fn is_monotonic_expression(expr: &Expression) -> Monotonic {
    match expr {
        Expression::Column(_) => Monotonic {
            is_monotonic: true,
            is_positive: true,
        },
        Expression::UnaryExpression { op, expr } => match op.as_str() {
            "-" => {
                let mut monotonic = is_monotonic_expression(expr);
                if monotonic.is_monotonic {
                    monotonic.is_positive = !monotonic.is_positive;
                }
                monotonic
            }
            _ => Monotonic {
                is_monotonic: false,
                is_positive: true,
            },
        },
        _ => Monotonic {
            is_monotonic: false,
            is_positive: true,
        },
    }
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
    Rows,
}

impl StatType {
    fn to_string(&self) -> Cow<'static, str> {
        match *self {
            StatType::Min => "min".into(),
            StatType::Max => "max".into(),
            StatType::Nulls => "nulls".into(),
            StatType::Rows => "rows".into(),
        }
    }
}

impl fmt::Display for StatType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[derive(Debug, Clone)]
struct StatColumn {
    column_id: ColumnId,
    stat_type: StatType,
    stat_field: DataField,
}

impl StatColumn {
    fn new(
        column_name: String,
        column_id: ColumnId,
        stat_type: StatType,
        field: &DataField,
    ) -> Self {
        let column_new = format!("{}_{}", stat_type, column_name);
        let data_type = match stat_type {
            StatType::Nulls => DataType::UInt64,
            StatType::Rows => DataType::UInt64,
            _ => field.data_type().clone(),
        };
        let stat_field = DataField::new(column_new.as_str(), data_type, field.is_nullable());
        Self {
            column_id,
            stat_type,
            stat_field,
        }
    }
}

type StatColumns = Vec<StatColumn>;

fn collect_columns_from_expr(expr: &Expression) -> Result<HashSet<String>> {
    let mut visitor = RequireColumnsVisitor::default();
    visitor = expr.accept(visitor)?;
    Ok(visitor.required_columns)
}

struct Monotonic {
    is_monotonic: bool,
    is_positive: bool,
}

struct VerifiableExprBuilder<'a> {
    args: Expressions,
    op: &'a str,
    column_name: String,
    column_id: ColumnId,
    monotonic: Monotonic,
    field: &'a DataField,
    stat_columns: &'a mut StatColumns,
}

impl<'a> VerifiableExprBuilder<'a> {
    fn new(
        exprs: Expressions,
        op: &'a str,
        schema: &'a DataSchema,
        stat_columns: &'a mut StatColumns,
    ) -> Result<Self> {
        let (args, cols, op) = match exprs.len() {
            1 => {
                let cols = collect_columns_from_expr(&exprs[0])?;
                match cols.len() {
                    1 => (exprs, cols, op),
                    _ => {
                        return Err(ErrorCode::UnknownException(
                            "Multi-column expressions are not currently supported",
                        ))
                    }
                }
            }
            2 => {
                let lhs_cols = collect_columns_from_expr(&exprs[0])?;
                let rhs_cols = collect_columns_from_expr(&exprs[1])?;
                match (lhs_cols.len(), rhs_cols.len()) {
                    (1, 0) => (vec![exprs[0].clone(), exprs[1].clone()], lhs_cols, op),
                    (0, 1) => {
                        let op = inverse_operator(op)?;
                        (vec![exprs[1].clone(), exprs[0].clone()], rhs_cols, op)
                    }
                    _ => {
                        return Err(ErrorCode::UnknownException(
                            "Multi-column expressions are not currently supported",
                        ))
                    }
                }
            }
            _ => {
                return Err(ErrorCode::UnknownException(
                    "Expressions with more than two args are not currently supported",
                ))
            }
        };

        let monotonic = is_monotonic_expression(&args[0]);
        if !monotonic.is_monotonic {
            return Err(ErrorCode::UnknownException(
                "Only support the monotonic expression",
            ));
        }
        let column_name = cols.iter().next().unwrap().clone();
        let (index, field) = schema
            .column_with_name(column_name.as_str())
            .ok_or_else(|| ErrorCode::UnknownException("Unable to find the column name"))?;
        let column_id = index as ColumnId;

        Ok(Self {
            args,
            op,
            field,
            column_name,
            column_id,
            monotonic,
            stat_columns,
        })
    }

    fn build(&mut self) -> Result<Expression> {
        // TODO: support like/not like/in/not in.
        match self.op {
            "isnull" => {
                let nulls_expr = self.nulls_column_expr()?;
                let scalar_expr = lit(0u64);
                Ok(nulls_expr.gt(scalar_expr))
            }
            "isnotnull" => {
                let nulls_expr = self.nulls_column_expr()?;
                let rows_expr = self.rows_column_expr()?;
                Ok(nulls_expr.lt(rows_expr))
            }
            "=" => {
                let min_expr = self.min_column_expr()?;
                let max_expr = self.max_column_expr()?;
                Ok(min_expr
                    .lt_eq(self.args[1].clone())
                    .and(max_expr.gt_eq(self.args[1].clone())))
            }
            "!=" => {
                let min_expr = self.min_column_expr()?;
                let max_expr = self.max_column_expr()?;
                Ok(min_expr
                    .gt(self.args[1].clone())
                    .or(max_expr.lt(self.args[1].clone())))
            }
            ">" => {
                let max_expr = self.max_column_expr()?;
                Ok(max_expr.gt(self.args[1].clone()))
            }
            ">=" => {
                let max_expr = self.max_column_expr()?;
                Ok(max_expr.gt_eq(self.args[1].clone()))
            }
            "<" => {
                let min_expr = self.min_column_expr()?;
                Ok(min_expr.lt(self.args[1].clone()))
            }
            "<=" => {
                let min_expr = self.min_column_expr()?;
                Ok(min_expr.lt_eq(self.args[1].clone()))
            }
            other => Err(ErrorCode::UnknownException(format!(
                "Cannot build atom expression by the operator: {:?}",
                other
            ))),
        }
    }

    fn stat_column_expr(&mut self, stat_type: StatType) -> Result<Expression> {
        let stat_col = StatColumn::new(
            self.column_name.clone(),
            self.column_id,
            stat_type,
            self.field,
        );
        if !self
            .stat_columns
            .iter()
            .any(|c| c.column_id == self.column_id && c.stat_type == stat_type)
        {
            self.stat_columns.push(stat_col.clone());
        }
        RewriteHelper::rewrite_column_expr(
            &self.args[0],
            self.column_name.as_str(),
            stat_col.stat_field.name(),
        )
    }

    fn min_column_expr(&mut self) -> Result<Expression> {
        if self.monotonic.is_positive {
            self.stat_column_expr(StatType::Min)
        } else {
            self.stat_column_expr(StatType::Max)
        }
    }

    fn max_column_expr(&mut self) -> Result<Expression> {
        if self.monotonic.is_positive {
            self.stat_column_expr(StatType::Max)
        } else {
            self.stat_column_expr(StatType::Min)
        }
    }

    fn nulls_column_expr(&mut self) -> Result<Expression> {
        self.stat_column_expr(StatType::Nulls)
    }

    fn rows_column_expr(&mut self) -> Result<Expression> {
        self.stat_column_expr(StatType::Rows)
    }
}

#[cfg(test)]
mod tests {
    use common_datavalues::DataField;
    use common_datavalues::DataSchemaRefExt;
    use common_datavalues::DataType;
    use common_exception::Result;
    use common_planners::*;

    use super::*;

    #[test]
    fn test_build_verifiable_function() -> Result<()> {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("a", DataType::Int64, false),
            DataField::new("b", DataType::Int32, false),
            DataField::new("c", DataType::Int32, false),
        ]);

        let mut stat_columns: StatColumns = Vec::new();

        let expr = col("a").lt(lit(1)).and(col("b").gt(lit(3)));
        let res = build_verifiable_expr(&expr, schema.clone(), &mut stat_columns)?;
        let expect = "((min_a < 1) and (max_b > 3))";
        let actual = format!("{:?}", res);
        assert_eq!(expect, actual);

        let expr = neg(col("a")).lt(lit(1)).and(col("b").gt(lit(3)));
        let res = build_verifiable_expr(&expr, schema, &mut stat_columns)?;
        let expect = "(((- max_a) < 1) and (max_b > 3))";
        let actual = format!("{:?}", res);
        assert_eq!(expect, actual);
        Ok(())
    }
}
