// Copyright 2023 Datafuse Labs.
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

use anyhow::{anyhow, Error, Result};
use databend_client::response::SchemaField;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NumberDataType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
}

// #[derive(Debug, Clone, PartialEq, Eq)]
// pub struct DecimalSize {
//     pub precision: u8,
//     pub scale: u8,
// }

// #[derive(Debug, Clone, PartialEq, Eq)]
// pub enum DecimalDataType {
//     Decimal128(DecimalSize),
//     Decimal256(DecimalSize),
// }

#[derive(Debug, Clone)]
pub enum DataType {
    Null,
    EmptyArray,
    EmptyMap,
    Boolean,
    String,
    Number(NumberDataType),
    Decimal,
    // TODO:(everpcpc) fix Decimal internal type
    // Decimal(DecimalDataType),
    Timestamp,
    Date,
    Nullable(Box<DataType>),
    Array(Box<DataType>),
    Map(Box<DataType>),
    Tuple(Vec<DataType>),
    Variant,
    // Generic(usize),
}

impl TryFrom<String> for DataType {
    type Error = Error;

    fn try_from(s: String) -> Result<Self> {
        let type_desc = parse_type_desc(&s)?;
        match type_desc.name {
            "Null" => Ok(Self::Null),
            "Boolean" => Ok(Self::Boolean),
            "String" => Ok(Self::String),
            "Int8" => Ok(Self::Number(NumberDataType::Int8)),
            "Int16" => Ok(Self::Number(NumberDataType::Int16)),
            "Int32" => Ok(Self::Number(NumberDataType::Int32)),
            "Int64" => Ok(Self::Number(NumberDataType::Int64)),
            "UInt8" => Ok(Self::Number(NumberDataType::UInt8)),
            "UInt16" => Ok(Self::Number(NumberDataType::UInt16)),
            "UInt32" => Ok(Self::Number(NumberDataType::UInt32)),
            "UInt64" => Ok(Self::Number(NumberDataType::UInt64)),
            "Float32" => Ok(Self::Number(NumberDataType::Float32)),
            "Float64" => Ok(Self::Number(NumberDataType::Float64)),
            "Decimal" => Ok(Self::Decimal),
            "Timestamp" => Ok(Self::Timestamp),
            "Date" => Ok(Self::Date),
            "Nullable" => {
                if type_desc.args.len() != 1 {
                    return Err(anyhow!("Nullable type must have one argument"));
                }
                let inner = Self::try_from(type_desc.args[0].name.to_string())?;
                Ok(Self::Nullable(Box::new(inner)))
            }
            "Array" => {
                if type_desc.args.len() != 1 {
                    return Err(anyhow!("Array type must have one argument"));
                }
                let inner = Self::try_from(type_desc.args[0].name.to_string())?;
                Ok(Self::Array(Box::new(inner)))
            }
            "Map" => {
                if type_desc.args.len() != 1 {
                    return Err(anyhow!("Map type must have one argument"));
                }
                let inner = Self::try_from(type_desc.args[0].name.to_string())?;
                Ok(Self::Map(Box::new(inner)))
            }
            "Tuple" => {
                let mut inner = vec![];
                for arg in type_desc.args {
                    inner.push(Self::try_from(arg.name.to_string())?);
                }
                Ok(Self::Tuple(inner))
            }
            "Variant" => Ok(Self::Variant),
            _ => Err(anyhow!("Unknown type: {}", s)),
        }
    }
}

impl TryFrom<SchemaField> for DataType {
    type Error = Error;

    fn try_from(field: SchemaField) -> Result<Self> {
        Self::try_from(field.r#type)
    }
}

pub(crate) struct SchemaFieldList(Vec<SchemaField>);

impl SchemaFieldList {
    pub(crate) fn new(fields: Vec<SchemaField>) -> Self {
        Self(fields)
    }
}

impl TryFrom<SchemaFieldList> for Vec<DataType> {
    type Error = Error;

    fn try_from(fields: SchemaFieldList) -> Result<Self> {
        fields.0.into_iter().map(DataType::try_from).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TypeDesc<'t> {
    name: &'t str,
    args: Vec<TypeDesc<'t>>,
}

fn parse_type_desc(s: &str) -> Result<TypeDesc> {
    let mut name = "";
    let mut args = vec![];
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in s.chars().enumerate() {
        match c {
            '(' => {
                if depth == 0 {
                    name = &s[start..i];
                    start = i + 1;
                }
                depth += 1;
            }
            ')' => {
                depth -= 1;
                if depth == 0 {
                    let s = &s[start..i];
                    if !s.is_empty() {
                        args.push(parse_type_desc(s)?);
                    }
                    start = i + 1;
                }
            }
            ',' => {
                if depth == 1 {
                    args.push(parse_type_desc(&s[start..i])?);
                    start = i + 1;
                }
            }
            ' ' => {
                if depth == 0 {
                    start = i + 1;
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(Error::msg(format!("Invalid type desc: {}", s)));
    }
    if start < s.len() {
        name = &s[start..];
    }
    Ok(TypeDesc { name, args })
}

#[cfg(test)]
mod test {
    use std::vec;

    use super::*;

    #[test]
    fn test_parse_type_desc() {
        struct TestCase<'t> {
            desc: &'t str,
            input: &'t str,
            output: TypeDesc<'t>,
        }

        let test_cases = vec![
            TestCase {
                desc: "plain type",
                input: "String",
                output: TypeDesc {
                    name: "String",
                    args: vec![],
                },
            },
            TestCase {
                desc: "nullable type",
                input: "Nullable(Nothing)",
                output: TypeDesc {
                    name: "Nullable",
                    args: vec![TypeDesc {
                        name: "Nothing",
                        args: vec![],
                    }],
                },
            },
            TestCase {
                desc: "empty arg",
                input: "DateTime()",
                output: TypeDesc {
                    name: "DateTime",
                    args: vec![],
                },
            },
            TestCase {
                desc: "numeric arg",
                input: "FixedString(42)",
                output: TypeDesc {
                    name: "FixedString",
                    args: vec![TypeDesc {
                        name: "42",
                        args: vec![],
                    }],
                },
            },
            TestCase {
                desc: "multiple args",
                input: "Array(Tuple(Tuple(String, String), Tuple(String, UInt64)))",
                output: TypeDesc {
                    name: "Array",
                    args: vec![TypeDesc {
                        name: "Tuple",
                        args: vec![
                            TypeDesc {
                                name: "Tuple",
                                args: vec![
                                    TypeDesc {
                                        name: "String",
                                        args: vec![],
                                    },
                                    TypeDesc {
                                        name: "String",
                                        args: vec![],
                                    },
                                ],
                            },
                            TypeDesc {
                                name: "Tuple",
                                args: vec![
                                    TypeDesc {
                                        name: "String",
                                        args: vec![],
                                    },
                                    TypeDesc {
                                        name: "UInt64",
                                        args: vec![],
                                    },
                                ],
                            },
                        ],
                    }],
                },
            },
            TestCase {
                desc: "map args",
                input: "Map(String, Array(Int64))",
                output: TypeDesc {
                    name: "Map",
                    args: vec![
                        TypeDesc {
                            name: "String",
                            args: vec![],
                        },
                        TypeDesc {
                            name: "Array",
                            args: vec![TypeDesc {
                                name: "Int64",
                                args: vec![],
                            }],
                        },
                    ],
                },
            },
        ];
        for case in test_cases {
            let output = parse_type_desc(case.input).unwrap();
            assert_eq!(output, case.output, "{}", case.desc);
        }
    }
}
