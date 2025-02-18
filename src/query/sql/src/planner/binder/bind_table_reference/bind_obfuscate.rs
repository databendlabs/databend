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

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TableReference;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;

use crate::binder::util::TableIdentifier;
use crate::binder::Binder;
use crate::optimizer::SExpr;
use crate::BindContext;

impl Binder {
    pub(crate) fn bind_obfuscate(
        &mut self,
        bind_context: &mut BindContext,
        params: &[Expr],
        _named_params: &[(Identifier, Expr)],
    ) -> Result<(SExpr, BindContext)> {
        let param = match params {
            [] => Err(None),
            [param @ Expr::ColumnRef { .. }] => Ok(param.clone()),
            _ => Err(params[0].span()),
        };

        match param {
            Ok(Expr::ColumnRef {
                column:
                    ColumnRef {
                        database: catalog,
                        table: database,
                        column: ColumnID::Name(table),
                    },
                ..
            }) => self.bind_obfuscate_subquery(bind_context, &catalog, &database, &table),
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
    ) -> Result<(SExpr, BindContext)> {
        let table_identifier = TableIdentifier::new(self, catalog, database, table, &None);

        let schema = self
            .resolve_data_source(
                &table_identifier.catalog_name(),
                &table_identifier.database_name(),
                &table_identifier.table_name(),
                None,
                None,
                self.ctx.clone().get_abort_checker(),
            )?
            .schema();

        let columns = schema
            .fields()
            .iter()
            .map(|f| match f.data_type.remove_nullable() {
                TableDataType::String => StrColumn {
                    name: f.name.clone(),
                    params: "{\"order\":5,\"sliding_window_size\":8}".to_string(),
                    seed: 0,
                },
                _ => todo!(),
            })
            .collect::<Vec<_>>();

        let subquery = build_subquery(
            table_identifier.database_name(),
            table_identifier.table_name(),
            &columns,
            "a12345",
        );

        self.bind_subquery(bind_context, false, &subquery, &None)
    }
}

struct StrColumn {
    name: String,
    // alias: String,
    params: String,
    seed: u64,
}

fn build_subquery(
    database: String,
    table_name: String,
    strings: &[StrColumn],
    alias_prefix: &str,
) -> Box<Query> {
    let database = ident(database);
    let table_name = ident(table_name);

    let table = TableReference::Table {
        span: None,
        catalog: None,
        database: Some(database.clone()),
        table: table_name.clone(),
        alias: None,
        temporal: None,
        with_options: None,
        pivot: None,
        unpivot: None,
        sample: None,
    };

    let train_list = strings
        .iter()
        .map(|col| {
            markov_train(
                ColumnRef {
                    database: Some(database.clone()),
                    table: Some(table_name.clone()),
                    column: ColumnID::Name(ident(col.name.clone())),
                },
                col.name.clone(),
            )
        })
        .collect::<Vec<_>>();

    let model_name = format!("{alias_prefix}_model");

    let generate_list = strings
        .iter()
        .map(|col| {
            markov_generate(
                ColumnRef {
                    database: None,
                    table: Some(ident(model_name.clone())),
                    column: ColumnID::Name(ident(col.name.clone())),
                },
                col.params.clone(),
                col.seed,
                ColumnRef {
                    database: Some(database.clone()),
                    table: Some(table_name.clone()),
                    column: ColumnID::Name(ident(col.name.clone())),
                },
                col.name.clone(),
            )
        })
        .collect::<Vec<_>>();

    Box::new(Query {
        span: None,
        with: None,
        body: SetExpr::Select(Box::new(SelectStmt {
            select_list: generate_list,
            from: vec![table.clone(), TableReference::Subquery {
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
                    name: ident(model_name),
                    columns: vec![],
                }),
                pivot: None,
                unpivot: None,
            }],
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
            func: ASTFunctionCall {
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
                window: None,
                lambda: None,
            },
        }),
        alias: Some(ident(alias)),
    }
}

fn markov_train(src: ColumnRef, alias: String) -> SelectTarget {
    SelectTarget::AliasedExpr {
        expr: Box::new(Expr::FunctionCall {
            span: None,
            func: ASTFunctionCall {
                distinct: false,
                name: ident("markov_train".to_string()),
                params: vec![],
                args: vec![Expr::ColumnRef {
                    span: None,
                    column: src,
                }],
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
