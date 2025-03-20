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
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::CreateOption;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::optimizer::SubqueryRewriter;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_sql::plans::Plan;
use databend_common_sql::Binder;
use databend_common_sql::Metadata;
use databend_common_sql::MetadataRef;
use databend_common_sql::NameResolutionContext;
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
    explain: &'static str,
}

fn get_test_suites() -> Vec<TestSuite> {
    vec![
        // Elimination Successful
        TestSuite {
            name: "base",
            query: "SELECT t3.* FROM t1 t3 WHERE t3.b IN (SELECT t4.b FROM t1 t4 WHERE t4.a = 5);",
            explain: "EvalScalar
├── scalars: [t3.a (#0) AS (#0), t3.b (#1) AS (#1), t3.c (#2) AS (#2)]
└── Filter
    ├── filters: [and(true, eq(t1.a (#0), 5))]
    └── Scan
        ├── table: default.t1
        ├── filters: []
        ├── order by: []
        └── limit: NONE\n",
        },
        TestSuite {
            name: "filter merge",
            query: "select t3.* from t1 as t3 where t3.c = 'D' and t3.b in (select t4.b from t1 as t4 where t4.a = 7);",
            explain: "EvalScalar
├── scalars: [t3.a (#0) AS (#0), t3.b (#1) AS (#1), t3.c (#2) AS (#2)]
└── Filter
    ├── filters: [eq(t3.c (#2), 'D'), and(true, eq(t1.a (#0), 7))]
    └── Scan
        ├── table: default.t1
        ├── filters: []
        ├── order by: []
        └── limit: NONE\n",
        },
        TestSuite {
            name: "order by exists in subquery",
            query: "SELECT t3.* FROM t1 t3 WHERE t3.b IN (SELECT t4.b FROM t1 t4 WHERE t4.a > 10 ORDER BY t4.c);",
            explain: "EvalScalar
├── scalars: [t3.a (#0) AS (#0), t3.b (#1) AS (#1), t3.c (#2) AS (#2)]
└── Filter
    ├── filters: [and(true, gt(t1.a (#0), 10))]
    └── Scan
        ├── table: default.t1
        ├── filters: []
        ├── order by: []
        └── limit: NONE\n",
        },
        TestSuite {
            name: "complex predicate(and & or)",
            query: "SELECT t3.* FROM t1 t3 WHERE t3.c = 'X' AND t3.b IN (SELECT t4.b FROM t1 t4 WHERE t4.a = 3 OR t4.c = 'Y');",
            explain: "EvalScalar
├── scalars: [t3.a (#0) AS (#0), t3.b (#1) AS (#1), t3.c (#2) AS (#2)]
└── Filter
    ├── filters: [eq(t3.c (#2), 'X'), and(true, or(eq(t1.a (#0), 3), eq(t1.c (#2), 'Y')))]
    └── Scan
        ├── table: default.t1
        ├── filters: []
        ├── order by: []
        └── limit: NONE\n",
        },
        TestSuite {
            name: "complex predicate(between & is not null)",
            query: "SELECT t3.* FROM t1 t3 WHERE t3.b IN (SELECT t4.b FROM t1 t4 WHERE t4.a BETWEEN 5 AND 10 AND t4.c IS NOT NULL);",
            explain: "EvalScalar
├── scalars: [t3.a (#0) AS (#0), t3.b (#1) AS (#1), t3.c (#2) AS (#2)]
└── Filter
    ├── filters: [and(and(and(true, gte(t1.a (#0), 5)), lte(t1.a (#0), 10)), true)]
    └── Scan
        ├── table: default.t1
        ├── filters: []
        ├── order by: []
        └── limit: NONE\n",
        },

        // Eliminate Failure
        TestSuite {
            name: "join in the main query",
            query: "SELECT t3.* FROM t1 t3 JOIN t2 ON t3.a = t2.a WHERE t3.b IN (SELECT t4.b FROM t1 t4 WHERE t4.a = 5);",
            explain: "EvalScalar
├── scalars: [t3.a (#0) AS (#0), t3.b (#1) AS (#1), t3.c (#2) AS (#2)]
└── Filter
    ├── filters: [9 (#9)]
    └── Join(RightMark)
        ├── build keys: [subquery_7 (#7)]
        ├── probe keys: [t3.b (#1)]
        ├── other filters: []
        ├── Join(Inner)
        │   ├── build keys: [t2.a (#3)]
        │   ├── probe keys: [t3.a (#0)]
        │   ├── other filters: []
        │   ├── Scan
        │   │   ├── table: default.t1
        │   │   ├── filters: []
        │   │   ├── order by: []
        │   │   └── limit: NONE
        │   └── Scan
        │       ├── table: default.t2
        │       ├── filters: []
        │       ├── order by: []
        │       └── limit: NONE
        └── EvalScalar
            ├── scalars: [t4.b (#7) AS (#7)]
            └── Filter
                ├── filters: [eq(t4.a (#6), 5)]
                └── Scan
                    ├── table: default.t1
                    ├── filters: []
                    ├── order by: []
                    └── limit: NONE\n",
        },
        TestSuite {
            name: "group by in the subquery",
            query: "SELECT t3.* FROM t1 t3 WHERE t3.b IN (SELECT t4.b FROM t1 t4 GROUP BY t4.b HAVING COUNT(*) > 1);",
            explain: "EvalScalar
├── scalars: [t3.a (#0) AS (#0), t3.b (#1) AS (#1), t3.c (#2) AS (#2)]
└── Filter
    ├── filters: [7 (#7)]
    └── Join(RightMark)
        ├── build keys: [subquery_4 (#4)]
        ├── probe keys: [t3.b (#1)]
        ├── other filters: []
        ├── Scan
        │   ├── table: default.t1
        │   ├── filters: []
        │   ├── order by: []
        │   └── limit: NONE
        └── EvalScalar
            ├── scalars: [t4.b (#4) AS (#4)]
            └── Filter
                ├── filters: [gt(COUNT(*) (#6), 1)]
                └── Aggregate(Initial)
                    ├── group items: [t4.b (#4)]
                    ├── aggregate functions: [COUNT(*) (#6)]
                    └── EvalScalar
                        ├── scalars: [t4.b (#4) AS (#4)]
                        └── Scan
                            ├── table: default.t1
                            ├── filters: []
                            ├── order by: []
                            └── limit: NONE\n",
        },
        TestSuite {
            name: "different tables",
            query: "SELECT t3.* FROM t1 t3 WHERE t3.b IN (SELECT t2.b FROM t2 WHERE t2.a = 3);",
            explain: "EvalScalar
├── scalars: [t3.a (#0) AS (#0), t3.b (#1) AS (#1), t3.c (#2) AS (#2)]
└── Filter
    ├── filters: [6 (#6)]
    └── Join(RightMark)
        ├── build keys: [subquery_4 (#4)]
        ├── probe keys: [t3.b (#1)]
        ├── other filters: []
        ├── Scan
        │   ├── table: default.t1
        │   ├── filters: []
        │   ├── order by: []
        │   └── limit: NONE
        └── EvalScalar
            ├── scalars: [t2.b (#4) AS (#4)]
            └── Filter
                ├── filters: [eq(t2.a (#3), 3)]
                └── Scan
                    ├── table: default.t2
                    ├── filters: []
                    ├── order by: []
                    └── limit: NONE\n",
        },
        TestSuite {
            name: "limit in the subquery",
            query: "SELECT t3.* FROM t1 t3 WHERE t3.b IN (SELECT t4.b FROM t1 t4 WHERE t4.a = 5 LIMIT 1);",
            explain: "EvalScalar
├── scalars: [t3.a (#0) AS (#0), t3.b (#1) AS (#1), t3.c (#2) AS (#2)]
└── Filter
    ├── filters: [6 (#6)]
    └── Join(RightMark)
        ├── build keys: [subquery_4 (#4)]
        ├── probe keys: [t3.b (#1)]
        ├── other filters: []
        ├── Scan
        │   ├── table: default.t1
        │   ├── filters: []
        │   ├── order by: []
        │   └── limit: NONE
        └── Limit
            ├── limit: [1]
            ├── offset: [0]
            └── EvalScalar
                ├── scalars: [t4.b (#4) AS (#4)]
                └── Filter
                    ├── filters: [eq(t4.a (#3), 5)]
                    └── Scan
                        ├── table: default.t1
                        ├── filters: []
                        ├── order by: []
                        └── limit: NONE\n",
        },
        TestSuite {
            name: "join in the subquery",
            query: "SELECT t3.* FROM t1 t3 WHERE t3.b IN (SELECT t4.b FROM t1 t4 JOIN t2 ON t4.a = t2.a);",
            explain: "EvalScalar
├── scalars: [t3.a (#0) AS (#0), t3.b (#1) AS (#1), t3.c (#2) AS (#2)]
└── Filter
    ├── filters: [9 (#9)]
    └── Join(RightMark)
        ├── build keys: [subquery_4 (#4)]
        ├── probe keys: [t3.b (#1)]
        ├── other filters: []
        ├── Scan
        │   ├── table: default.t1
        │   ├── filters: []
        │   ├── order by: []
        │   └── limit: NONE
        └── EvalScalar
            ├── scalars: [t4.b (#4) AS (#4)]
            └── Join(Inner)
                ├── build keys: [t2.a (#6)]
                ├── probe keys: [t4.a (#3)]
                ├── other filters: []
                ├── Scan
                │   ├── table: default.t1
                │   ├── filters: []
                │   ├── order by: []
                │   └── limit: NONE
                └── Scan
                    ├── table: default.t2
                    ├── filters: []
                    ├── order by: []
                    └── limit: NONE\n",
        },
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
                TableField::new(
                    "a",
                    TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int32))),
                ),
                TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
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
                TableField::new(
                    "a",
                    TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int32))),
                ),
                TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
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
        assert_eq!(
            query, suite.explain,
            "\n==== Case: {} ====\n\nQuery: \n{} \nExpected: \n{}",
            suite.name, query, suite.explain
        );
    }

    Ok(())
}

fn format_pretty(query: &SExpr, query_meta: &MetadataRef) -> Result<String> {
    let metadata = &*query_meta.read();

    Ok(query.to_format_tree(metadata, false)?.format_pretty()?)
}

async fn plan_sql(ctx: Arc<dyn TableContext>, sql: &str) -> Result<String> {
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
        let s_expr = SubqueryRewriter::new(ctx.clone(), metadata.clone(), None).rewrite(&s_expr)?;

        return format_pretty(&s_expr, &metadata);
    }
    unreachable!()
}
