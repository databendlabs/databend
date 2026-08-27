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

use databend_common_exception::ErrorCode;
use databend_common_expression::ColumnRef;
use databend_common_expression::Constant;
use databend_common_expression::Expr;
use databend_common_expression::LambdaFunctionCall;
use databend_common_expression::RemoteDefaultExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::NullAs;

use crate::read::cast::load_can_auto_cast_to;

/// 1. try auto cast
/// 2. fill missing value according to NullAs
///
/// used for orc and parquet now
pub fn project_columnar(
    input_schema: &TableSchemaRef,
    output_schema: &TableSchemaRef,
    missing_as: &NullAs,
    default_exprs: &Option<Vec<RemoteDefaultExpr>>,
    location: &str,
    case_sensitive: bool,
) -> databend_common_exception::Result<(Vec<Expr>, Vec<usize>)> {
    let mut pushdown_columns = vec![];
    let mut output_projection = vec![];

    for (i, to_field) in output_schema.fields().iter().enumerate() {
        let field_name = to_field.name();
        let positions = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                if case_sensitive {
                    f.name() == field_name
                } else {
                    f.name().to_lowercase() == field_name.to_lowercase()
                }
            })
            .map(|(pos, _)| pos)
            .collect::<Vec<_>>();

        let expr = match positions.len() {
            1 => {
                let pos = positions[0];
                pushdown_columns.push(pos);
                let from_field = input_schema.field(pos);
                let expr = Expr::ColumnRef(ColumnRef {
                    span: None,
                    id: pos,
                    data_type: from_field.data_type().into(),
                    display_name: to_field.name().clone(),
                });

                if from_field.data_type == to_field.data_type {
                    expr
                } else if load_can_auto_cast_to(
                    &from_field.data_type().into(),
                    &to_field.data_type().into(),
                ) {
                    check_cast(
                        None,
                        false,
                        expr,
                        &to_field.data_type().into(),
                        &BUILTIN_FUNCTIONS,
                    )?
                } else if !matches!(missing_as, NullAs::Error) {
                    // special cast for tuple type, fill in default values for the missing fields.
                    match (
                        from_field.data_type.remove_nullable(),
                        to_field.data_type.remove_nullable(),
                    ) {
                        (
                            TableDataType::Array(box TableDataType::Nullable(
                                box TableDataType::Tuple {
                                    fields_name: from_fields_name,
                                    fields_type: from_fields_type,
                                },
                            )),
                            TableDataType::Array(box TableDataType::Nullable(
                                box TableDataType::Tuple {
                                    fields_name: to_fields_name,
                                    fields_type: to_fields_type,
                                },
                            )),
                        ) => project_array_tuple(
                            expr,
                            to_field,
                            &from_fields_name,
                            &from_fields_type,
                            &to_fields_name,
                            &to_fields_type,
                        )?,
                        (
                            TableDataType::Tuple {
                                fields_name: from_fields_name,
                                fields_type: from_fields_type,
                            },
                            TableDataType::Tuple {
                                fields_name: to_fields_name,
                                fields_type: to_fields_type,
                            },
                        ) => project_tuple(
                            expr,
                            &from_field.data_type,
                            &to_field.data_type,
                            &from_fields_name,
                            &from_fields_type,
                            &to_fields_name,
                            &to_fields_type,
                        )?,
                        (_, _) => {
                            return Err(ErrorCode::BadDataValueType(format!(
                                "fail to load file {}: Cannot cast column {} from {:?} to {:?}",
                                location,
                                field_name,
                                from_field.data_type(),
                                to_field.data_type()
                            )));
                        }
                    }
                } else {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "fail to load file {}: Cannot cast column {} from {:?} to {:?}",
                        location,
                        field_name,
                        from_field.data_type(),
                        to_field.data_type()
                    )));
                }
            }
            0 => {
                match missing_as {
                    // default
                    NullAs::Error => {
                        let advice = build_columnar_missing_advice(
                            input_schema,
                            output_schema,
                            field_name,
                            case_sensitive,
                            "MISSING_FIELD_AS=ERROR",
                            None,
                        );
                        return Err(ErrorCode::BadDataValueType(format!(
                            "file {} missing column `{}`. {}",
                            location, field_name, advice,
                        )));
                    }
                    NullAs::Null => {
                        if to_field.is_nullable() {
                            Expr::Constant(Constant {
                                span: None,
                                data_type: to_field.data_type().into(),
                                scalar: Scalar::Null,
                            })
                        } else {
                            let advice = build_columnar_missing_advice(
                                input_schema,
                                output_schema,
                                field_name,
                                case_sensitive,
                                "MISSING_FIELD_AS=NULL",
                                Some("the column is not nullable"),
                            );
                            return Err(ErrorCode::BadDataValueType(format!(
                                "file {} missing column `{}`. {}",
                                location, field_name, advice,
                            )));
                        }
                    }
                    NullAs::FieldDefault => {
                        let default_exprs = &default_exprs.as_deref().expect(
                            "default_values should not be none when miss_field_as=FIELD_DEFAULT",
                        );
                        match &default_exprs[i] {
                            RemoteDefaultExpr::RemoteExpr(expr) => expr.as_expr(&BUILTIN_FUNCTIONS),
                            RemoteDefaultExpr::Sequence(_) => {
                                return Err(ErrorCode::BadDataValueType(
                                    "not supported yet: fill missing column with sequence as default",
                                ));
                            }
                        }
                    }
                }
            }
            _ => {
                return Err(ErrorCode::BadArguments(format!(
                    "multi field named {} in file {}",
                    field_name, location
                )));
            }
        };
        output_projection.push(expr);
    }
    if pushdown_columns.is_empty() {
        return Err(ErrorCode::BadBytes(format!(
            "not column name match in file {location}",
        )));
    }
    Ok((output_projection, pushdown_columns))
}

fn build_columnar_missing_advice(
    input_schema: &TableSchemaRef,
    output_schema: &TableSchemaRef,
    field_name: &str,
    case_sensitive: bool,
    current_setting: &str,
    extra_reason: Option<&str>,
) -> String {
    let mut hints = Vec::new();

    // Hint 1: current MISSING_FIELD_AS setting
    hints.push(format!("current FILE_FORMAT option: {current_setting}"));

    // Hint 2: if there's an extra reason (e.g., column not nullable)
    if let Some(reason) = extra_reason {
        hints.push(reason.to_string());
    }

    // Hint 3: if case_sensitive, check if there's a case-insensitive match in the file
    if case_sensitive {
        let lower_field = field_name.to_lowercase();
        let matched = input_schema
            .fields()
            .iter()
            .find(|f| f.name().to_lowercase() == lower_field && f.name() != field_name);
        if let Some(f) = matched {
            hints.push(format!(
                "found column '{}' with different case in file; consider using `COLUMN_MATCH_MODE=CASE_INSENSITIVE`",
                f.name()
            ));
        }
    }

    // Hint 4: if target table has a single Variant column, suggest select $1
    let fields = output_schema.fields();
    if fields.len() == 1
        && matches!(
            fields[0].data_type.remove_nullable(),
            TableDataType::Variant
        )
    {
        hints.push(
            "the target table has a single VARIANT column; consider using `COPY INTO <table> FROM (SELECT $1 FROM @<stage>)` to load raw data"
                .to_string(),
        );
    }

    hints.join(". ")
}

fn project_array_tuple(
    expr: Expr,
    to_field: &TableField,
    from_fields_name: &[String],
    from_fields_type: &[TableDataType],
    to_fields_name: &[String],
    to_fields_type: &[TableDataType],
) -> databend_common_exception::Result<Expr> {
    let from_tuple_type = TableDataType::Nullable(Box::new(TableDataType::Tuple {
        fields_name: from_fields_name.to_vec(),
        fields_type: from_fields_type.to_vec(),
    }));
    let to_tuple_type = TableDataType::Nullable(Box::new(TableDataType::Tuple {
        fields_name: to_fields_name.to_vec(),
        fields_type: to_fields_type.to_vec(),
    }));
    let lambda_arg = Expr::ColumnRef(ColumnRef {
        span: None,
        id: 0,
        data_type: (&from_tuple_type).into(),
        display_name: "x".to_string(),
    });
    let lambda_expr = project_tuple(
        lambda_arg,
        &from_tuple_type,
        &to_tuple_type,
        from_fields_name,
        from_fields_type,
        to_fields_name,
        to_fields_type,
    )?;

    let array_map_return_type = if expr.data_type().is_nullable_or_null() {
        DataType::Array(Box::new(lambda_expr.data_type().clone())).wrap_nullable()
    } else {
        DataType::Array(Box::new(lambda_expr.data_type().clone()))
    };
    let array_map = Expr::LambdaFunctionCall(LambdaFunctionCall {
        span: None,
        name: "array_map".to_string(),
        args: vec![expr],
        lambda_expr: Box::new(lambda_expr.as_remote_expr()),
        lambda_display: "x -> tuple projection".to_string(),
        return_type: array_map_return_type,
    });

    check_cast(
        None,
        false,
        array_map,
        &to_field.data_type().into(),
        &BUILTIN_FUNCTIONS,
    )
}

fn project_tuple(
    expr: Expr,
    from_data_type: &TableDataType,
    to_data_type: &TableDataType,
    from_fields_name: &[String],
    from_fields_type: &[TableDataType],
    to_fields_name: &[String],
    to_fields_type: &[TableDataType],
) -> databend_common_exception::Result<Expr> {
    let mut inner_columns = Vec::with_capacity(to_fields_name.len());

    for (to_field_name, to_field_type) in to_fields_name.iter().zip(to_fields_type.iter()) {
        let inner_column = match from_fields_name.iter().position(|k| k == to_field_name) {
            Some(idx) => {
                let from_field_type = from_fields_type.get(idx).unwrap();
                let tuple_idx = Scalar::Number(NumberScalar::Int64((idx + 1) as i64));
                let inner_column = check_function(
                    None,
                    "get",
                    &[tuple_idx],
                    &[expr.clone()],
                    &BUILTIN_FUNCTIONS,
                )?;
                if from_field_type != to_field_type {
                    check_cast(
                        None,
                        false,
                        inner_column,
                        &to_field_type.into(),
                        &BUILTIN_FUNCTIONS,
                    )?
                } else {
                    inner_column
                }
            }
            None => {
                // if inner field not exists, fill default value.
                let data_type: DataType = to_field_type.into();
                let scalar = Scalar::default_value(&data_type);
                Expr::Constant(Constant {
                    span: None,
                    scalar,
                    data_type,
                })
            }
        };
        inner_columns.push(inner_column);
    }
    let tuple_column = check_function(None, "tuple", &[], &inner_columns, &BUILTIN_FUNCTIONS)?;
    let tuple_column = if from_data_type != to_data_type {
        let dest_ty: DataType = to_data_type.into();
        check_cast(None, false, tuple_column, &dest_ty, &BUILTIN_FUNCTIONS)?
    } else {
        tuple_column
    };

    if from_data_type.is_nullable() && to_data_type.is_nullable() {
        // add if function to cast null value
        let is_not_null = check_function(
            None,
            "is_not_null",
            &[],
            &[expr.clone()],
            &BUILTIN_FUNCTIONS,
        )?;
        let null_scalar = Expr::Constant(Constant {
            span: None,
            scalar: Scalar::Null,
            data_type: DataType::Null,
        });
        check_function(
            None,
            "if",
            &[],
            &[is_not_null, tuple_column, null_scalar],
            &BUILTIN_FUNCTIONS,
        )
    } else {
        Ok(tuple_column)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;

    use super::*;

    #[test]
    fn test_project_array_nullable_tuple_with_lambda_for_parquet() {
        let input_schema = Arc::new(TableSchema::new(vec![TableField::new(
            "a",
            TableDataType::Array(Box::new(TableDataType::Nullable(Box::new(
                TableDataType::Tuple {
                    fields_name: vec!["x".to_string(), "y".to_string()],
                    fields_type: vec![
                        TableDataType::Nullable(Box::new(TableDataType::Number(
                            NumberDataType::Int32,
                        ))),
                        TableDataType::Nullable(Box::new(TableDataType::Number(
                            NumberDataType::Int32,
                        ))),
                    ],
                },
            )))),
        )]));
        let output_schema = Arc::new(TableSchema::new(vec![TableField::new(
            "a",
            TableDataType::Nullable(Box::new(TableDataType::Array(Box::new(
                TableDataType::Nullable(Box::new(TableDataType::Tuple {
                    fields_name: vec!["y".to_string(), "z".to_string(), "x".to_string()],
                    fields_type: vec![
                        TableDataType::Nullable(Box::new(TableDataType::Number(
                            NumberDataType::Int32,
                        ))),
                        TableDataType::Nullable(Box::new(TableDataType::Number(
                            NumberDataType::Int32,
                        ))),
                        TableDataType::Nullable(Box::new(TableDataType::Number(
                            NumberDataType::Int32,
                        ))),
                    ],
                })),
            )))),
        )]));

        let (projection, pushdowns) = project_columnar(
            &input_schema,
            &output_schema,
            &NullAs::Null,
            &None,
            "test.parquet",
            true,
        )
        .unwrap();

        assert_eq!(pushdowns, vec![0]);
        assert_eq!(projection.len(), 1);
        let Expr::Cast(cast) = &projection[0] else {
            panic!("expected array tuple projection to be cast to the target type");
        };
        assert_eq!(cast.dest_type, output_schema.field(0).data_type().into());
        let Expr::LambdaFunctionCall(lambda) = cast.expr.as_ref() else {
            panic!("expected array tuple projection to be a lambda expression");
        };
        assert_eq!(lambda.name, "array_map");
        assert_eq!(lambda.args.len(), 1);
        let Expr::ColumnRef(arg) = &lambda.args[0] else {
            panic!("expected array_map input to be the source column");
        };
        assert_eq!(arg.id, 0);
        assert_eq!(lambda.return_type, cast.dest_type.remove_nullable());
    }
}
