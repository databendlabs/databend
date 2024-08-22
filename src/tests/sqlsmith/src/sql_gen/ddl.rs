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

use std::collections::BTreeMap;

use databend_common_ast::ast::ColumnDefinition;
use databend_common_ast::ast::ColumnExpr;
use databend_common_ast::ast::CreateOption;
use databend_common_ast::ast::CreateTableSource;
use databend_common_ast::ast::CreateTableStmt;
use databend_common_ast::ast::DropTableStmt;
use databend_common_ast::ast::Engine;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::TypeName;
use rand::distributions::Alphanumeric;
use rand::Rng;

use crate::sql_gen::SqlGenerator;

const BASE_TABLE_NAMES: [&str; 4] = ["t1", "t2", "t3", "t4"];

const SIMPLE_COLUMN_TYPES: [TypeName; 21] = [
    TypeName::Boolean,
    TypeName::UInt8,
    TypeName::UInt16,
    TypeName::UInt32,
    TypeName::UInt64,
    TypeName::Int8,
    TypeName::Int16,
    TypeName::Int32,
    TypeName::Int64,
    TypeName::Float32,
    TypeName::Float64,
    TypeName::Decimal {
        precision: 15,
        scale: 2,
    },
    TypeName::Decimal {
        precision: 40,
        scale: 10,
    },
    TypeName::Date,
    TypeName::Timestamp,
    TypeName::String,
    TypeName::Bitmap,
    TypeName::Variant,
    TypeName::Binary,
    TypeName::Geometry,
    TypeName::Geography,
];

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_base_tables(&mut self) -> Vec<(DropTableStmt, CreateTableStmt)> {
        let mut tables = Vec::with_capacity(BASE_TABLE_NAMES.len());
        for table_name in BASE_TABLE_NAMES {
            let source = self.gen_table_source();

            let drop_table = DropTableStmt {
                if_exists: true,
                catalog: None,
                database: None,
                table: Identifier::from_name(None, table_name),
                all: false,
            };
            let create_table = CreateTableStmt {
                create_option: CreateOption::CreateIfNotExists,
                catalog: None,
                database: None,
                table: Identifier::from_name(None, table_name),
                source: Some(source),
                engine: Some(Engine::Fuse),
                uri_location: None,
                cluster_by: vec![],
                table_options: BTreeMap::new(),
                as_query: None,
                transient: false,
            };
            tables.push((drop_table, create_table));
        }
        tables
    }

    fn gen_nested_type(&mut self, depth: u8) -> TypeName {
        if depth == 0 {
            let i = self.rng.gen_range(0..=19);
            // replace bitmap with string, as generated bitmap value can't display
            if i == 16 {
                TypeName::Nullable(Box::new(TypeName::String))
            } else {
                // TODO: fix not null types as inner nested type
                TypeName::Nullable(Box::new(SIMPLE_COLUMN_TYPES[i].clone()))
            }
        } else {
            match self.rng.gen_range(0..=2) {
                0 => {
                    let inner_ty = self.gen_nested_type(depth - 1);
                    TypeName::Array(Box::new(inner_ty))
                }
                1 => {
                    let key_type = TypeName::String;
                    let val_type = self.gen_nested_type(depth - 1);

                    TypeName::Map {
                        key_type: Box::new(key_type),
                        val_type: Box::new(val_type),
                    }
                }
                2 => {
                    let len = self.rng.gen_range(1..=3);
                    let fields_name = if self.rng.gen_bool(0.7) {
                        None
                    } else {
                        let mut fields_name = Vec::with_capacity(len);
                        for i in 0..len {
                            let rand_name = self
                                .rng
                                .sample_iter(&Alphanumeric)
                                .take(5)
                                .map(char::from)
                                .collect::<String>();
                            let name = format!("t_{}_{}_{}", depth, i, rand_name);
                            let field_name = if self.rng.gen_bool(0.5) {
                                Identifier::from_name(None, name)
                            } else {
                                Identifier::from_name_with_quoted(None, name, Some('"'))
                            };
                            fields_name.push(field_name);
                        }
                        Some(fields_name)
                    };
                    let mut fields_type = Vec::with_capacity(len);
                    for _ in 0..len {
                        let field_type = self.gen_nested_type(depth - 1);
                        fields_type.push(field_type);
                    }
                    TypeName::Tuple {
                        fields_name,
                        fields_type,
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    pub(crate) fn gen_data_type_name(&mut self, idx: Option<usize>) -> TypeName {
        let i = match idx {
            Some(i) => i,
            None => self.rng.gen_range(0..42),
        };
        if i < 20 {
            TypeName::NotNull(Box::new(SIMPLE_COLUMN_TYPES[i].clone()))
        } else if i < 40 {
            TypeName::Nullable(Box::new(SIMPLE_COLUMN_TYPES[i - 20].clone()))
        } else {
            let depth = self.rng.gen_range(1..=3);
            self.gen_nested_type(depth)
        }
    }

    pub(crate) fn gen_random_name(&mut self) -> String {
        let name: String = (0..5)
            .map(|_| self.rng.sample(Alphanumeric) as char)
            .collect();
        name
    }

    pub(crate) fn gen_new_column(&mut self) -> ColumnDefinition {
        let name = self.gen_random_name();
        let new_column_name = Identifier::from_name(None, format!("cc{}", name));
        let data_type = self.gen_data_type_name(None);
        ColumnDefinition {
            name: new_column_name,
            data_type,
            expr: None,
            comment: None,
        }
    }

    fn gen_table_source(&mut self) -> CreateTableSource {
        let mut column_defs = Vec::with_capacity(38);

        for i in 0..42 {
            let name = format!("c{}", i);
            let data_type = self.gen_data_type_name(Some(i));

            // TODO: computed expr
            // TODO: fix binary default value
            let default_expr = if data_type != TypeName::NotNull(Box::new(TypeName::Binary)) {
                Some(ColumnExpr::Default(Box::new(gen_default_expr(&data_type))))
            } else {
                None
            };
            let column_def = ColumnDefinition {
                name: Identifier::from_name(None, name),
                data_type,
                expr: default_expr,
                comment: None,
            };
            column_defs.push(column_def);
        }
        CreateTableSource::Columns(column_defs, None)
    }
}

fn gen_default_expr(type_name: &TypeName) -> Expr {
    match type_name {
        TypeName::Boolean => Expr::Literal {
            span: None,
            value: Literal::Boolean(false),
        },
        TypeName::UInt8
        | TypeName::UInt16
        | TypeName::UInt32
        | TypeName::UInt64
        | TypeName::Int8
        | TypeName::Int16
        | TypeName::Int32
        | TypeName::Int64 => Expr::Literal {
            span: None,
            value: Literal::UInt64(0),
        },
        TypeName::Float32 | TypeName::Float64 => Expr::Literal {
            span: None,
            value: Literal::Float64(0.0),
        },
        TypeName::Decimal { precision, scale } => Expr::Literal {
            span: None,
            value: Literal::Decimal256 {
                value: 0.into(),
                precision: *precision,
                scale: *scale,
            },
        },
        TypeName::Date => Expr::Literal {
            span: None,
            value: Literal::String("1970-01-01".to_string()),
        },
        TypeName::Timestamp => Expr::Literal {
            span: None,
            value: Literal::String("1970-01-01 00:00:00".to_string()),
        },
        TypeName::Binary => Expr::Literal {
            span: None,
            value: Literal::String("".to_string()),
        },
        TypeName::String => Expr::Literal {
            span: None,
            value: Literal::String("".to_string()),
        },
        TypeName::Array(_) => Expr::Array {
            span: None,
            exprs: vec![],
        },
        TypeName::Map { .. } => Expr::Map {
            span: None,
            kvs: vec![],
        },
        TypeName::Bitmap => Expr::Literal {
            span: None,
            value: Literal::UInt64(0),
        },
        TypeName::Tuple { fields_type, .. } => Expr::Tuple {
            span: None,
            exprs: fields_type.iter().map(gen_default_expr).collect(),
        },
        TypeName::Variant => Expr::Literal {
            span: None,
            value: Literal::String("null".to_string()),
        },
        TypeName::Geometry => Expr::Literal {
            span: None,
            value: Literal::String("POINT(0 0)".to_string()),
        },
        TypeName::Geography => Expr::Literal {
            span: None,
            value: Literal::String("POINT(0 0)".to_string()),
        },
        TypeName::Nullable(_) => Expr::Literal {
            span: None,
            value: Literal::Null,
        },
        TypeName::NotNull(box ty) => gen_default_expr(ty),
    }
}
