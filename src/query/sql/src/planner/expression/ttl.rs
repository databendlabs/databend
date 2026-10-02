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

use std::sync::Arc;

use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::Expr as AstExpr;
use databend_common_ast::ast::IntervalKind;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::TypeName;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_expression::eval_function;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use parking_lot::RwLock;

use super::expression_parser::bind_context_from_schema;
use crate::Metadata;
use crate::ScalarBinder;
use crate::planner::semantic::NameResolutionContext;

/// A TTL is either an absolute expiration column or a retention column plus
/// a nonnegative, single-unit interval literal. Keep this classification shared
/// by definition admission, schema revalidation and manual cleanup.
fn ttl_parts(expr: &AstExpr) -> Result<(&AstExpr, Option<&AstExpr>)> {
    let (column, interval) = match expr {
        AstExpr::ColumnRef { .. } => (expr, None),
        AstExpr::BinaryOp {
            op: BinaryOperator::Plus,
            left,
            right,
            ..
        } => (left.as_ref(), Some(right.as_ref())),
        _ => return Err(invalid_ttl()),
    };
    if !matches!(column, AstExpr::ColumnRef { column, .. }
        if column.database.is_none() && column.table.is_none()
            && matches!(column.column, ColumnID::Name(_)))
    {
        return Err(invalid_ttl());
    }
    if let Some(interval) = interval {
        if !matches!(interval, AstExpr::Interval { expr, unit, .. }
            if matches!(expr.as_ref(), AstExpr::Literal { value: Literal::UInt64(_), .. })
                && matches!(unit, IntervalKind::MicroSecond | IntervalKind::Second
                    | IntervalKind::Minute | IntervalKind::Hour | IntervalKind::Day
                    | IntervalKind::Week | IntervalKind::Month | IntervalKind::Quarter
                    | IntervalKind::Year))
        {
            return Err(invalid_ttl());
        }
    }
    Ok((column, interval))
}

fn invalid_ttl() -> ErrorCode {
    ErrorCode::SemanticError(
        "TTL must be a time column or a time column + INTERVAL <nonnegative integer> <unit>",
    )
}

/// Bind the time column and evaluate the interval once. This is a transient
/// binding result; TableMeta continues to store only the normalized SQL text.
pub(crate) fn bind_ttl_definition<'a>(
    ctx: Arc<dyn TableContext>,
    schema: &TableSchemaRef,
    ast: &'a AstExpr,
    names: &NameResolutionContext,
) -> Result<(&'a AstExpr, Option<Scalar>)> {
    let (column, interval) = ttl_parts(ast)?;
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let mut bind_context = bind_context_from_schema(schema, &metadata);
    let mut binder = ScalarBinder::new(&mut bind_context, ctx.clone(), names, metadata, &[]);
    binder.forbid_virtual_computed_column();
    let (_, data_type) = binder.bind(column)?;
    if !matches!(
        data_type.remove_nullable(),
        DataType::Date | DataType::Timestamp | DataType::TimestampTz
    ) {
        return Err(ErrorCode::SemanticError(
            "TTL column must be DATE, TIMESTAMP or TIMESTAMP_TZ",
        ));
    }
    let interval = if let Some(AstExpr::Interval { expr, unit, span }) = interval {
        // Match SQL INTERVAL lowering: convert the checked literal and unit to
        // an interval string, then use the existing strict function evaluator.
        let (value, _) = eval_function(
            *span,
            "to_interval",
            [(
                Value::Scalar(Scalar::String(format!("{expr:#} {unit}"))),
                DataType::String,
            )],
            &ctx.get_function_context()?,
            1,
            &BUILTIN_FUNCTIONS,
        )?;
        Some(
            value
                .index(0)
                .ok_or_else(|| ErrorCode::Internal("Missing TTL interval value"))?
                .to_owned(),
        )
    } else {
        None
    };
    Ok((column, interval))
}

fn timestamp_literal(micros: i64) -> Result<AstExpr> {
    Ok(parse_expr(
        &tokenize_sql(&format!("to_timestamp({micros}, 6)"))?,
        Dialect::default(),
    )?)
}

/// Build a column-to-constant DELETE predicate, preserving ordinary SQL time-zone
/// arithmetic. Retention TTL uses column < cutoff - interval, not column +
/// interval <= cutoff: these are deliberately different at calendar boundaries.
pub(crate) fn materialize_ttl_predicate(
    ctx: Arc<dyn TableContext>,
    schema: &TableSchemaRef,
    sql: &str,
) -> Result<AstExpr> {
    let ast = parse_expr(&tokenize_sql(sql)?, Dialect::default())?;
    let (column, interval) = bind_ttl_definition(
        ctx.clone(),
        schema,
        &ast,
        &NameResolutionContext::preserve_identifier_case(),
    )?;
    let func_ctx = ctx.get_function_context()?;
    let cutoff = func_ctx.now.timestamp_micros();
    let (op, threshold) = if let Some(interval) = interval {
        let (value, _) = eval_function(
            None,
            "minus",
            [
                (
                    Value::Scalar(Scalar::Timestamp(cutoff)),
                    DataType::Timestamp,
                ),
                (Value::Scalar(interval), DataType::Interval),
            ],
            &func_ctx,
            1,
            &BUILTIN_FUNCTIONS,
        )?;
        let Some(ScalarRef::Timestamp(threshold)) = value.index(0) else {
            return Err(ErrorCode::Internal("TTL cutoff must be a TIMESTAMP"));
        };
        (BinaryOperator::Lt, threshold)
    } else {
        (BinaryOperator::Lte, cutoff)
    };
    Ok(AstExpr::BinaryOp {
        span: None,
        op,
        left: Box::new(AstExpr::Cast {
            span: None,
            expr: Box::new(column.clone()),
            target_type: TypeName::Timestamp,
            pg_style: false,
        }),
        right: Box::new(timestamp_literal(threshold)?),
    })
}
