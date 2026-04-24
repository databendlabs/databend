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

use databend_common_ast::ast::TypeName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::vector::VectorDataType;

pub fn resolve_type_name_by_str(name: &str, not_null: bool) -> Result<TableDataType> {
    let sql_tokens = databend_common_ast::parser::tokenize_sql(name)?;
    let ast = databend_common_ast::parser::run_parser(
        &sql_tokens,
        databend_common_ast::parser::Dialect::default(),
        databend_common_ast::parser::ParseMode::Default,
        false,
        databend_common_ast::parser::expr::type_name,
    )?;
    resolve_type_name(&ast, not_null)
}

pub fn resolve_type_name(type_name: &TypeName, not_null: bool) -> Result<TableDataType> {
    let data_type = match type_name {
        TypeName::Boolean => TableDataType::Boolean,
        TypeName::UInt8 => TableDataType::Number(NumberDataType::UInt8),
        TypeName::UInt16 => TableDataType::Number(NumberDataType::UInt16),
        TypeName::UInt32 => TableDataType::Number(NumberDataType::UInt32),
        TypeName::UInt64 => TableDataType::Number(NumberDataType::UInt64),
        TypeName::Int8 => TableDataType::Number(NumberDataType::Int8),
        TypeName::Int16 => TableDataType::Number(NumberDataType::Int16),
        TypeName::Int32 => TableDataType::Number(NumberDataType::Int32),
        TypeName::Int64 => TableDataType::Number(NumberDataType::Int64),
        TypeName::Float32 => TableDataType::Number(NumberDataType::Float32),
        TypeName::Float64 => TableDataType::Number(NumberDataType::Float64),
        TypeName::Decimal { precision, scale } => {
            TableDataType::Decimal(DecimalSize::new(*precision, *scale)?.into())
        }
        TypeName::Binary => TableDataType::Binary,
        TypeName::String => TableDataType::String,
        TypeName::Timestamp => TableDataType::Timestamp,
        TypeName::Date => TableDataType::Date,
        TypeName::Array(item_type) => {
            TableDataType::Array(Box::new(resolve_type_name(item_type, not_null)?))
        }
        TypeName::Map { key_type, val_type } => {
            let key_type = resolve_type_name(key_type, true)?;
            match key_type {
                TableDataType::Boolean
                | TableDataType::String
                | TableDataType::Number(_)
                | TableDataType::Decimal(_)
                | TableDataType::Timestamp
                | TableDataType::Date => {
                    let val_type = resolve_type_name(val_type, not_null)?;
                    let inner_type = TableDataType::Tuple {
                        fields_name: vec!["key".to_string(), "value".to_string()],
                        fields_type: vec![key_type, val_type],
                    };
                    TableDataType::Map(Box::new(inner_type))
                }
                _ => {
                    return Err(ErrorCode::BadArguments(format!(
                        "Invalid Map key type \'{:?}\'",
                        key_type
                    )));
                }
            }
        }
        TypeName::Bitmap => TableDataType::Bitmap,
        TypeName::Interval => TableDataType::Interval,
        TypeName::Tuple {
            fields_type,
            fields_name,
        } => TableDataType::Tuple {
            fields_name: match fields_name {
                None => (0..fields_type.len())
                    .map(|i| (i + 1).to_string())
                    .collect(),
                Some(names) => names
                    .iter()
                    .map(|i| {
                        if i.is_quoted() {
                            i.name.clone()
                        } else {
                            i.name.to_lowercase()
                        }
                    })
                    .collect(),
            },
            fields_type: fields_type
                .iter()
                .map(|item_type| resolve_type_name(item_type, not_null))
                .collect::<Result<Vec<_>>>()?,
        },
        TypeName::Nullable(inner_type) => {
            let data_type = resolve_type_name(inner_type, not_null)?;
            data_type.wrap_nullable()
        }
        TypeName::Variant => TableDataType::Variant,
        TypeName::Geometry => TableDataType::Geometry,
        TypeName::Geography => TableDataType::Geography,
        TypeName::Vector(dimension) => {
            if *dimension == 0 || *dimension > 4096 {
                return Err(ErrorCode::BadArguments(format!(
                    "Invalid vector dimension '{}'",
                    dimension
                )));
            }
            // Only support float32 type currently.
            TableDataType::Vector(VectorDataType::Float32(*dimension))
        }
        TypeName::NotNull(inner_type) => {
            let data_type = resolve_type_name(inner_type, not_null)?;
            data_type.remove_nullable()
        }
        TypeName::StageLocation => TableDataType::StageLocation,
        TypeName::TimestampTz => TableDataType::TimestampTz,
    };
    if !matches!(type_name, TypeName::Nullable(_) | TypeName::NotNull(_)) && !not_null {
        return Ok(data_type.wrap_nullable());
    }
    Ok(data_type)
}

pub fn resolve_type_name_udf(type_name: &TypeName) -> Result<TableDataType> {
    let type_name = match type_name {
        name @ TypeName::Nullable(_) | name @ TypeName::NotNull(_) => name,
        name => &name.clone().wrap_nullable(),
    };
    resolve_type_name(type_name, true)
}

pub fn validate_function_arg(
    name: &str,
    args_len: usize,
    variadic_arguments: Option<(usize, usize)>,
    num_arguments: usize,
) -> Result<()> {
    match variadic_arguments {
        Some((start, end)) => {
            if args_len < start || args_len > end {
                Err(ErrorCode::NumberArgumentsNotMatch(format!(
                    "Function `{}` expect to have [{}, {}] arguments, but got {}",
                    name, start, end, args_len
                )))
            } else {
                Ok(())
            }
        }
        None => {
            if num_arguments != args_len {
                Err(ErrorCode::NumberArgumentsNotMatch(format!(
                    "Function `{}` expect to have {} arguments, but got {}",
                    name, num_arguments, args_len
                )))
            } else {
                Ok(())
            }
        }
    }
}
