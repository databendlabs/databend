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

use databend_common_ast::ast::Engine;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_ast::Range;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::CreateOption;
use databend_common_sql::optimizer::Matcher;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::optimizer::SubqueryRewriter;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::ConstantExpr;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOp;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::Binder;
use databend_common_sql::ColumnBinding;
use databend_common_sql::Metadata;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Visibility;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use parking_lot::RwLock;

struct TestSuite {
    name: &'static str,
    // Test cases
    query: &'static str,
    // Expected results
    explain: Matcher,
}

fn binding_a() -> ColumnBinding {
    ColumnBinding {
        database_name: None,
        table_name: Some(String::from("t3")),
        column_position: Some(1),
        table_index: Some(0),
        source_table_index: None,
        column_name: String::from("a"),
        index: 0,
        data_type: Box::new(DataType::Number(NumberDataType::Int32)),
        visibility: Visibility::Visible,
        virtual_expr: None,
    }
}

fn binding_b() -> ColumnBinding {
    ColumnBinding {
        database_name: None,
        table_name: Some(String::from("t3")),
        column_position: Some(2),
        table_index: Some(0),
        source_table_index: None,
        column_name: String::from("b"),
        index: 1,
        data_type: Box::new(DataType::Nullable(Box::new(DataType::Number(
            NumberDataType::Int32,
        )))),
        visibility: Visibility::Visible,
        virtual_expr: None,
    }
}

fn binding_c() -> ColumnBinding {
    ColumnBinding {
        database_name: None,
        table_name: Some(String::from("t3")),
        column_position: Some(3),
        table_index: Some(0),
        source_table_index: None,
        column_name: String::from("c"),
        index: 2,
        data_type: Box::new(DataType::String),
        visibility: Visibility::Visible,
        virtual_expr: None,
    }
}

fn build_matcher(predicates: Vec<ScalarExpr>) -> Matcher {
    Matcher::MatchFn {
        predicate: Box::new(|op| {
            if let RelOperator::EvalScalar(eval) = op {
                return eval.items.len() == 3
                    && eval.items[0].index == 0
                    && eval.items[1].index == 1
                    && eval.items[2].index == 2
                    && eval.items[0].scalar
                        == ScalarExpr::BoundColumnRef(BoundColumnRef {
                            column: binding_a(),
                            span: Some(Range::from(10..11)),
                        })
                    && eval.items[1].scalar
                        == ScalarExpr::BoundColumnRef(BoundColumnRef {
                            column: binding_b(),
                            span: Some(Range::from(10..11)),
                        })
                    && eval.items[2].scalar
                        == ScalarExpr::BoundColumnRef(BoundColumnRef {
                            column: binding_c(),
                            span: Some(Range::from(10..11)),
                        });
            }
            false
        }),
        children: vec![Matcher::MatchFn {
            predicate: Box::new(move |op| {
                if let RelOperator::Filter(filter) = op {
                    return filter
                        .predicates
                        .iter()
                        .zip(predicates.iter())
                        .all(|(left, right)| left == right);
                }
                false
            }),
            children: vec![Matcher::MatchOp {
                op_type: RelOp::Scan,
                children: vec![],
            }],
        }],
    }
}

fn get_test_suites() -> Vec<TestSuite> {
    vec![
        // Elimination Successful
        TestSuite {
            name: "base",
            query: "SELECT t3.* FROM t1 t3 WHERE t3.b IN (SELECT t4.b FROM t1 t4 WHERE t4.a = 5);",
            explain: build_matcher(vec![ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "and".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: Some(Range::from(72..73)),
                        func_name: "is_not_null".to_string(),
                        params: vec![],
                        arguments: vec![ScalarExpr::BoundColumnRef(
                            BoundColumnRef {
                                span: Some(Range::from(29..31)),
                                column: binding_b(),
                            },
                        )],
                    }),
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: Some(Range::from(72..73)),
                        func_name: "eq".to_string(),
                        params: vec![],
                        arguments: vec![
                            ScalarExpr::BoundColumnRef(BoundColumnRef {
                                span: Some(Range::from(67..69)),
                                column: binding_a(),
                            }),
                            ScalarExpr::ConstantExpr(ConstantExpr {
                                span: Some(Range::from(74..75)),
                                value: Scalar::Number(NumberScalar::UInt8(
                                    5,
                                )),
                            }),
                        ],
                    }),
                ],
            })])
        },
        TestSuite {
            name: "filter merge",
            query: "select t3.* from t1 as t3 where t3.c = 'D' and t3.b in (select t4.b from t1 as t4 where t4.a = 7);",
            explain: build_matcher(vec![
                ScalarExpr::FunctionCall(FunctionCall {
                    span: Some(Range::from(37..38)),
                    func_name: "eq".to_string(),
                    params: vec![],
                    arguments: vec![
                        ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: Some(Range::from(32..34)),
                            column: binding_c(),
                        }),
                        ScalarExpr::ConstantExpr(ConstantExpr {
                            span: Some(Range::from(39..42)),
                            value: Scalar::String("D".to_string()),
                        })
                    ],
                }),
                ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: "and".to_string(),
                    params: vec![],
                    arguments: vec![
                        ScalarExpr::FunctionCall(FunctionCall {
                            span: None,
                            func_name: "is_not_null".to_string(),
                            params: vec![],
                            arguments: vec![ScalarExpr::BoundColumnRef(
                                BoundColumnRef {
                                    span: Some(Range::from(47..49)),
                                    column: binding_b(),
                                },
                            )],
                        }),
                        ScalarExpr::FunctionCall(FunctionCall {
                            span: Some(Range::from(93..94)),
                            func_name: "eq".to_string(),
                            params: vec![],
                            arguments: vec![
                                ScalarExpr::BoundColumnRef(BoundColumnRef {
                                    span: Some(Range::from(88..90)),
                                    column: binding_a(),
                                }),
                                ScalarExpr::ConstantExpr(ConstantExpr {
                                    span: Some(Range::from(95..96)),
                                    value: Scalar::Number(NumberScalar::UInt8(
                                        7,
                                    )),
                                }),
                            ],
                        }),
                    ],
                })
            ])
        }
    ]
}

fn create_table_plan(fixture: &TestFixture, format: &str) -> Vec<CreateTablePlan> {
    vec![
        CreateTablePlan {
            create_option: CreateOption::Create,
            tenant: fixture.default_tenant(),
            catalog: fixture.default_catalog_name(),
            database: "default".to_string(),
            table: "t1".to_string(),
            schema: TableSchemaRefExt::create(vec![
                TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
                TableField::new(
                    "b",
                    TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int32))),
                ),
                TableField::new("c", TableDataType::String),
            ]),
            engine: Engine::Fuse,
            engine_options: Default::default(),
            storage_params: None,
            options: [
                // database id is required for FUSE
                (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
                (OPT_KEY_STORAGE_FORMAT.to_owned(), format.to_owned()),
            ]
            .into(),
            field_comments: vec![],
            as_select: None,
            cluster_key: None,
            inverted_indexes: None,
            attached_columns: None,
        },
        CreateTablePlan {
            create_option: CreateOption::Create,
            tenant: fixture.default_tenant(),
            catalog: fixture.default_catalog_name(),
            database: "default".to_string(),
            table: "t2".to_string(),
            schema: TableSchemaRefExt::create(vec![
                TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
                TableField::new(
                    "b",
                    TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int32))),
                ),
                TableField::new("c", TableDataType::String),
            ]),
            engine: Engine::Fuse,
            engine_options: Default::default(),
            storage_params: None,
            options: [
                // database id is required for FUSE
                (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
                (OPT_KEY_STORAGE_FORMAT.to_owned(), format.to_owned()),
            ]
            .into(),
            field_comments: vec![],
            as_select: None,
            cluster_key: None,
            inverted_indexes: None,
            attached_columns: None,
        },
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn test_query_rewrite() -> Result<()> {
    test_query_rewrite_impl("parquet").await?;
    test_query_rewrite_impl("native").await
}

async fn test_query_rewrite_impl(format: &str) -> Result<()> {
    let fixture = TestFixture::setup().await?;

    let ctx = fixture.new_query_ctx().await?;
    for create_table in create_table_plan(&fixture, format) {
        let interpreter = CreateTableInterpreter::try_create(ctx.clone(), create_table)?;
        let _ = interpreter.execute(ctx.clone()).await?;
    }
    let test_suites = get_test_suites();
    for suite in test_suites.into_iter() {
        let query = plan_sql(ctx.clone(), suite.query).await?;
        assert!(
            suite.explain.matches(&query),
            "\n==== Case: {} ====",
            suite.name
        );
    }

    Ok(())
}

async fn plan_sql(ctx: Arc<dyn TableContext>, sql: &str) -> Result<SExpr> {
    let settings = ctx.get_settings();
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let binder = Binder::new(
        ctx.clone(),
        CatalogManager::instance(),
        name_resolution_ctx,
        metadata,
    );
    let tokens = tokenize_sql(sql)?;
    let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL)?;
    let plan = binder.bind(&stmt).await?;
    if let Plan::Query {
        s_expr, metadata, ..
    } = plan
    {
        return SubqueryRewriter::new(ctx.clone(), metadata.clone(), None).rewrite(&s_expr);
    }
    unreachable!()
}
