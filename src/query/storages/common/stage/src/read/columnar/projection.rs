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
use databend_common_expression::type_check::check_cast;
use databend_common_expression::Expr;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
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
    null_as: &NullAs,
    default_values: &Option<Vec<RemoteExpr>>,
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
                let expr = Expr::ColumnRef {
                    span: None,
                    id: pos,
                    data_type: from_field.data_type().into(),
                    display_name: to_field.name().clone(),
                };

                if from_field.data_type == to_field.data_type {
                    expr
                } else {
                    // note: tuple field name is dropped here, matched by pos here
                    if load_can_auto_cast_to(
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
            }
            0 => {
                match null_as {
                    // default
                    NullAs::Error => {
                        return Err(ErrorCode::BadDataValueType(format!(
                            "file {} missing column `{}`",
                            location, field_name,
                        )));
                    }
                    NullAs::Null => {
                        if to_field.is_nullable() {
                            Expr::Constant {
                                span: None,
                                data_type: to_field.data_type().into(),
                                scalar: Scalar::Null,
                            }
                        } else {
                            return Err(ErrorCode::BadDataValueType(format!(
                                "{} missing column `{}`",
                                location, field_name,
                            )));
                        }
                    }
                    NullAs::FieldDefault => {
                        let default_values = &default_values.as_deref().expect(
                            "default_values should not be none when miss_field_as=FIELD_DEFAULT",
                        );
                        default_values[i].as_expr(&BUILTIN_FUNCTIONS)
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
