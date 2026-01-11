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

use std::hash::Hasher;

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TableRef;
use databend_common_ast::ast::TableReference;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::types::NumberScalar;

use crate::BindContext;
use crate::ScalarBinder;
use crate::binder::Binder;
use crate::binder::table_args::bind_table_args;
use crate::binder::util::TableIdentifier;
use crate::optimizer::ir::SExpr;

impl Binder {
    pub(crate) fn bind_obfuscate(
        &mut self,
        bind_context: &mut BindContext,
        params: &[Expr],
        named_params: &[(Identifier, Expr)],
    ) -> Result<(SExpr, BindContext)> {
        let param = match params {
            [] => Err(None),
            [param @ Expr::ColumnRef { .. }] => Ok(param.clone()),
            _ => Err(params[0].span()),
        };

        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );
        let mut named_args = bind_table_args(&mut scalar_binder, &[], named_params, &None)?.named;
        let seed = match named_args.remove("seed") {
            Some(v) => u64_value(&v).ok_or(ErrorCode::BadArguments("invalid seed"))?,
            None => {
                let mut state = std::hash::DefaultHasher::new();
                state.write_u128(std::time::Instant::now().elapsed().as_nanos());
                state.finish()
            }
        };
        if !named_args.is_empty() {
            let invalid_names = named_args.into_keys().collect::<Vec<String>>().join(", ");
            return Err(ErrorCode::InvalidArgument(format!(
                "Invalid named parameters for 'obfuscate': {}, valid parameters are: [seed]",
                invalid_names,
            )));
        }

        match param {
            Ok(Expr::ColumnRef {
                column:
                    ColumnRef {
                        database: catalog,
                        table: database,
                        column: ColumnID::Name(table),
                    },
                ..
            }) => self.bind_obfuscate_subquery(bind_context, &catalog, &database, &table, seed),
            Err(span) => Err(ErrorCode::InvalidArgument(
                "The `OBFUSCATE` function expects a 'table_name' parameter.",
            )
            .set_span(span)),
            _ => unreachable!(),
        }
    }

    fn bind_obfuscate_subquery(
        &mut self,
        bind_context: &mut BindContext,
        catalog: &Option<Identifier>,
        database: &Option<Identifier>,
        table: &Identifier,
        seed: u64,
    ) -> Result<(SExpr, BindContext)> {
        let table_identifier = TableIdentifier::new(self, catalog, database, table, &None, &None);

        let catalog_name = table_identifier.catalog_name();
        let database_name = table_identifier.database_name();
        let table_name = table_identifier.table_name();

        let schema = self
            .resolve_data_source(
                &self.ctx,
                &catalog_name,
                &database_name,
                &table_name,
                None,
                None,
                None,
            )?
            .schema();

        let mut seed = seed;
        let mut next_seed = || {
            let x = seed;
            seed = seed.wrapping_add(1);
            x
        };

        let fields = schema
            .fields()
            .iter()
            .map(|f| match f.data_type.remove_nullable() {
                TableDataType::String => Field {
                    name: f.name.clone(),
                    params: Params::Markov {
                        seed: next_seed(),
                        gen_params: "{\"order\":5,\"sliding_window_size\":8}".to_string(),
                    },
                },
                TableDataType::Number(_) => Field {
                    name: f.name.clone(),
                    params: Params::Feistel { seed: next_seed() },
                },
                _ => Field {
                    name: f.name.clone(),
                    params: Params::None,
                },
            })
            .collect::<Vec<_>>();

        let model_table_name = if table_name.eq_ignore_ascii_case("model") {
            "tmp_model"
        } else {
            "model"
        };
        let subquery = build_subquery(
            table_identifier.database_name(),
            table_name,
            &fields,
            model_table_name,
        );

        self.bind_subquery(bind_context, false, &subquery, &None)
    }
}

struct Field {
    name: String,
    params: Params,
}

enum Params {
    None,
    Markov { seed: u64, gen_params: String },
    Feistel { seed: u64 },
}

fn build_subquery(
    database: String,
    table_name: String,
    fields: &[Field],
    model_table_name: &str,
) -> Box<Query> {
    let database = ident(database);
    let table_name = ident(table_name);

    let table = TableReference::Table {
        span: None,
        table: TableRef {
            catalog: None,
            database: Some(database.clone()),
            table: table_name.clone(),
            branch: None,
        },
        alias: None,
        temporal: None,
        with_options: None,
        pivot: None,
        unpivot: None,
        sample: None,
    };

    let train_list = fields
        .iter()
        .filter_map(|field| match &field.params {
            Params::Markov { .. } => Some(markov_train(
                ColumnRef {
                    database: Some(database.clone()),
                    table: Some(table_name.clone()),
                    column: ColumnID::Name(ident(field.name.clone())),
                },
                5,
                field.name.clone(),
            )),
            _ => None,
        })
        .collect::<Vec<_>>();

    let generate_list = fields
        .iter()
        .map(|field| {
            let input = ColumnRef {
                database: Some(database.clone()),
                table: Some(table_name.clone()),
                column: ColumnID::Name(ident(field.name.clone())),
            };
            match &field.params {
                Params::None => SelectTarget::AliasedExpr {
                    expr: Box::new(Expr::ColumnRef {
                        span: None,
                        column: input,
                    }),
                    alias: None,
                },
                Params::Markov { seed, gen_params } => markov_generate(
                    ColumnRef {
                        database: None,
                        table: Some(ident(model_table_name.to_string())),
                        column: ColumnID::Name(ident(field.name.clone())),
                    },
                    gen_params.clone(),
                    *seed,
                    input,
                    field.name.clone(),
                ),
                Params::Feistel { seed } => feistel_obfuscate(input, *seed, field.name.clone()),
            }
        })
        .collect::<Vec<_>>();

    let from = if train_list.is_empty() {
        vec![table.clone()]
    } else {
        vec![table.clone(), TableReference::Subquery {
            span: None,
            lateral: false,
            subquery: Box::new(Query {
                span: None,
                with: None,
                body: SetExpr::Select(Box::new(SelectStmt {
                    select_list: train_list,
                    from: vec![table],
                    ..zero_select_stmt()
                })),
                order_by: vec![],
                limit: vec![],
                offset: None,
                ignore_result: false,
            }),
            alias: Some(TableAlias {
                name: ident(model_table_name.to_string()),
                columns: vec![],
                keep_database_name: false,
            }),
            pivot: None,
            unpivot: None,
        }]
    };

    Box::new(Query {
        span: None,
        with: None,
        body: SetExpr::Select(Box::new(SelectStmt {
            select_list: generate_list,
            from,
            ..zero_select_stmt()
        })),
        order_by: vec![],
        limit: vec![],
        offset: None,
        ignore_result: false,
    })
}

fn markov_generate(
    model: ColumnRef,
    params: String,
    seed: u64,
    input: ColumnRef,
    alias: String,
) -> SelectTarget {
    SelectTarget::AliasedExpr {
        expr: Box::new(Expr::FunctionCall {
            span: None,
            func: FunctionCall {
                distinct: false,
                name: ident("markov_generate".to_string()),
                args: vec![
                    Expr::ColumnRef {
                        span: None,
                        column: model,
                    },
                    Expr::Literal {
                        span: None,
                        value: Literal::String(params),
                    },
                    Expr::Literal {
                        span: None,
                        value: Literal::UInt64(seed),
                    },
                    Expr::ColumnRef {
                        span: None,
                        column: input,
                    },
                ],
                params: vec![],
                order_by: vec![],
                window: None,
                lambda: None,
            },
        }),
        alias: Some(ident(alias)),
    }
}

fn markov_train(src: ColumnRef, order: u64, alias: String) -> SelectTarget {
    SelectTarget::AliasedExpr {
        expr: Box::new(Expr::FunctionCall {
            span: None,
            func: FunctionCall {
                distinct: false,
                name: ident("markov_train".to_string()),
                params: vec![Expr::Literal {
                    span: None,
                    value: Literal::UInt64(order),
                }],
                args: vec![Expr::ColumnRef {
                    span: None,
                    column: src,
                }],
                order_by: vec![],
                window: None,
                lambda: None,
            },
        }),
        alias: Some(ident(alias)),
    }
}

fn feistel_obfuscate(src: ColumnRef, seed: u64, alias: String) -> SelectTarget {
    SelectTarget::AliasedExpr {
        expr: Box::new(Expr::FunctionCall {
            span: None,
            func: FunctionCall {
                distinct: false,
                name: ident("feistel_obfuscate".to_string()),
                params: vec![],
                args: vec![
                    Expr::ColumnRef {
                        span: None,
                        column: src,
                    },
                    Expr::Literal {
                        span: None,
                        value: Literal::UInt64(seed),
                    },
                ],
                order_by: vec![],
                window: None,
                lambda: None,
            },
        }),
        alias: Some(ident(alias)),
    }
}

fn zero_select_stmt() -> SelectStmt {
    SelectStmt {
        span: None,
        hints: None,
        distinct: false,
        top_n: None,
        select_list: vec![],
        from: vec![],
        selection: None,
        group_by: None,
        having: None,
        window_list: None,
        qualify: None,
    }
}

fn ident(name: String) -> Identifier {
    Identifier {
        span: None,
        name,
        quote: None,
        ident_type: Default::default(),
    }
}

pub fn u64_value(val: &Scalar) -> Option<u64> {
    val.as_number().and_then(|x| {
        if !x.is_positive() {
            return None;
        }
        match x {
            NumberScalar::UInt8(n) => Some(*n as u64),
            NumberScalar::UInt16(n) => Some(*n as u64),
            NumberScalar::UInt32(n) => Some(*n as u64),
            NumberScalar::UInt64(n) => Some(*n),
            NumberScalar::Int8(n) => Some(*n as u64),
            NumberScalar::Int16(n) => Some(*n as u64),
            NumberScalar::Int32(n) => Some(*n as u64),
            NumberScalar::Int64(n) => Some(*n as u64),
            _ => None,
        }
    })
}
