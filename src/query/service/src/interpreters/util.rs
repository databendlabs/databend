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

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::TypeName;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::decimal::MAX_DECIMAL256_PRECISION;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_script::ir::ColumnAccess;
use databend_common_script::Client;
use databend_common_sql::Planner;
use futures_util::TryStreamExt;
use itertools::Itertools;
use serde_json::Value as JsonValue;

use crate::interpreters::InterpreterFactory;
use crate::sessions::QueryContext;

#[allow(clippy::type_complexity)]
pub fn generate_desc_schema(
    schema: TableSchemaRef,
) -> (
    Vec<String>,
    Vec<String>,
    Vec<String>,
    Vec<String>,
    Vec<String>,
) {
    let mut names: Vec<String> = vec![];
    let mut types: Vec<String> = vec![];
    let mut nulls: Vec<String> = vec![];
    let mut default_exprs: Vec<String> = vec![];
    let mut extras: Vec<String> = vec![];

    for field in schema.fields().iter() {
        names.push(field.name().to_string());

        let non_null_type = field.data_type().remove_recursive_nullable();
        types.push(non_null_type.sql_name());
        nulls.push(if field.is_nullable() {
            "YES".to_string()
        } else {
            "NO".to_string()
        });
        match field.default_expr() {
            Some(expr) => {
                default_exprs.push(expr.clone());
            }

            None => {
                let value = Scalar::default_value(&field.data_type().into());
                default_exprs.push(value.to_string());
            }
        }
        let extra = match field.computed_expr() {
            Some(ComputedExpr::Virtual(expr)) => format!("VIRTUAL COMPUTED COLUMN `{}`", expr),
            Some(ComputedExpr::Stored(expr)) => format!("STORED COMPUTED COLUMN `{}`", expr),
            _ => "".to_string(),
        };
        extras.push(extra);
    }
    (names, types, nulls, default_exprs, extras)
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub(crate) schema: DataSchemaRef,
    pub(crate) block: DataBlock,
}

pub struct ScriptClient {
    pub(crate) ctx: Arc<QueryContext>,
}

impl Client for ScriptClient {
    type Var = Scalar;
    type Set = QueryResult;

    async fn query(&self, query: &str) -> databend_common_exception::Result<Self::Set> {
        let ctx = self
            .ctx
            .get_current_session()
            .create_query_context()
            .await?;

        let mut planner = Planner::new(ctx.clone());
        let (plan, _) = planner.plan_sql(query).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = interpreter.execute(ctx.clone()).await?;
        let blocks = stream.try_collect::<Vec<_>>().await?;
        let schema = plan.schema();

        let block = match blocks.len() {
            0 => DataBlock::empty_with_schema(schema.clone()),
            1 => blocks[0].clone(),
            _ => DataBlock::concat(&blocks)?,
        };

        Ok(QueryResult { schema, block })
    }

    fn var_to_ast(&self, scalar: &Self::Var) -> databend_common_exception::Result<Expr> {
        let ast = match scalar {
            Scalar::Number(v) => with_integer_mapped_type!(|NUM_TYPE| match v {
                NumberScalar::NUM_TYPE(v) => Expr::Literal {
                    span: None,
                    value: Literal::Decimal256 {
                        value: (*v).into(),
                        precision: MAX_DECIMAL256_PRECISION,
                        scale: 0,
                    },
                },
                NumberScalar::Float32(v) => Expr::Literal {
                    span: None,
                    value: Literal::Float64(v.into_inner() as f64),
                },
                NumberScalar::Float64(v) => Expr::Literal {
                    span: None,
                    value: Literal::Float64(v.into_inner()),
                },
            }),
            Scalar::Boolean(v) => Expr::Literal {
                span: None,
                value: Literal::Boolean(*v),
            },
            Scalar::String(v) => Expr::Literal {
                span: None,
                value: Literal::String(v.clone()),
            },
            Scalar::Tuple(v) => Expr::FunctionCall {
                span: None,
                func: FunctionCall {
                    distinct: false,
                    name: Identifier::from_name(None, "tuple"),
                    args: v
                        .iter()
                        .map(|x| self.var_to_ast(&x.to_owned()))
                        .collect::<databend_common_exception::Result<Vec<_>>>()?,
                    params: vec![],
                    window: None,
                    lambda: None,
                },
            },
            Scalar::Array(v) => Expr::FunctionCall {
                span: None,
                func: FunctionCall {
                    distinct: false,
                    name: Identifier::from_name(None, "array"),
                    args: v
                        .iter()
                        .map(|x| self.var_to_ast(&x.to_owned()))
                        .collect::<databend_common_exception::Result<Vec<_>>>()?,
                    params: vec![],
                    window: None,
                    lambda: None,
                },
            },
            Scalar::Decimal(DecimalScalar::Decimal128(v, size)) => Expr::Literal {
                span: None,
                value: Literal::Decimal256 {
                    value: (*v).into(),
                    precision: size.precision,
                    scale: size.scale,
                },
            },
            Scalar::Decimal(DecimalScalar::Decimal256(v, size)) => Expr::Literal {
                span: None,
                value: Literal::Decimal256 {
                    value: *v,
                    precision: size.precision,
                    scale: size.scale,
                },
            },
            Scalar::Map(v) => {
                let col = v.as_tuple().unwrap();
                let keys = col[0]
                    .iter()
                    .map(|x| self.var_to_ast(&x.to_owned()))
                    .collect::<databend_common_exception::Result<Vec<_>>>()?;
                let vals = col[1]
                    .iter()
                    .map(|x| self.var_to_ast(&x.to_owned()))
                    .collect::<databend_common_exception::Result<Vec<_>>>()?;
                Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(None, "map"),
                        args: vec![
                            Expr::FunctionCall {
                                span: None,
                                func: FunctionCall {
                                    distinct: false,
                                    name: Identifier::from_name(None, "array"),
                                    args: keys,
                                    params: vec![],
                                    window: None,
                                    lambda: None,
                                },
                            },
                            Expr::FunctionCall {
                                span: None,
                                func: FunctionCall {
                                    distinct: false,
                                    name: Identifier::from_name(None, "array"),
                                    args: vals,
                                    params: vec![],
                                    window: None,
                                    lambda: None,
                                },
                            },
                        ],
                        params: vec![],
                        window: None,
                        lambda: None,
                    },
                }
            }
            Scalar::Variant(v) => {
                let value = jsonb::from_slice(v).unwrap();
                let json = JsonValue::from(value).to_string();
                Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(None, "parse_json"),
                        args: vec![Expr::Literal {
                            span: None,
                            value: Literal::String(json),
                        }],
                        params: vec![],
                        window: None,
                        lambda: None,
                    },
                }
            }
            Scalar::EmptyArray => Expr::FunctionCall {
                span: None,
                func: FunctionCall {
                    distinct: false,
                    name: Identifier::from_name(None, "array"),
                    args: vec![],
                    params: vec![],
                    window: None,
                    lambda: None,
                },
            },
            Scalar::EmptyMap => Expr::FunctionCall {
                span: None,
                func: FunctionCall {
                    distinct: false,
                    name: Identifier::from_name(None, "map"),
                    args: vec![],
                    params: vec![],
                    window: None,
                    lambda: None,
                },
            },
            Scalar::Date(v) => Expr::Cast {
                span: None,
                expr: Box::new(Expr::Literal {
                    span: None,
                    value: Literal::Decimal256 {
                        value: (*v).into(),
                        precision: MAX_DECIMAL256_PRECISION,
                        scale: 0,
                    },
                }),
                target_type: TypeName::Date,
                pg_style: false,
            },
            Scalar::Timestamp(v) => Expr::Cast {
                span: None,
                expr: Box::new(Expr::Literal {
                    span: None,
                    value: Literal::Decimal256 {
                        value: (*v).into(),
                        precision: MAX_DECIMAL256_PRECISION,
                        scale: 0,
                    },
                }),
                target_type: TypeName::Timestamp,
                pg_style: false,
            },
            Scalar::Null => Expr::Literal {
                span: None,
                value: Literal::Null,
            },
            Scalar::Bitmap(_) | Scalar::Binary(_) | Scalar::Geometry(_) | Scalar::Geography(_) => {
                return Err(ErrorCode::Unimplemented(format!(
                    "variable of type {} is not supported yet",
                    scalar.as_ref().infer_data_type()
                )));
            }
        };

        Ok(ast)
    }

    fn read_from_set(
        &self,
        set: &Self::Set,
        row: usize,
        col: &ColumnAccess,
    ) -> databend_common_exception::Result<Self::Var> {
        let offset = match col {
            ColumnAccess::Position(offset) => *offset,
            // TODO(andylokandy): name resolution
            ColumnAccess::Name(name) => set
                .schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| {
                ErrorCode::ScriptExecutionError(format!(
                    "cannot find column with name {} in block, available columns: {}",
                    name,
                    set.schema
                        .fields()
                        .iter()
                        .map(|f| format!("'{}'", f.name()))
                        .join(", ")
                ))
            })?,
        };
        let col = set.block.columns().get(offset).ok_or_else(|| {
            ErrorCode::ScriptExecutionError(format!(
                "cannot read column at offset {} from block with {} columns",
                offset,
                set.block.num_columns()
            ))
        })?;
        let cell = col
            .value
            .index(row)
            .ok_or_else(|| {
                ErrorCode::ScriptExecutionError(format!(
                    "cannot read value at row {} from column with {} rows",
                    row,
                    set.block.num_rows(),
                ))
            })?
            .to_owned();

        Ok(cell)
    }

    fn num_rows(&self, set: &Self::Set) -> usize {
        set.block.num_rows()
    }

    fn is_true(&self, scalar: &Self::Var) -> databend_common_exception::Result<bool> {
        match scalar {
            Scalar::Boolean(v) => Ok(*v),
            _ => Err(ErrorCode::ScriptExecutionError(format!(
                "`is_true` called on non-boolean value {scalar}",
            ))),
        }
    }
}
