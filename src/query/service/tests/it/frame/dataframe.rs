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

use std::io::Write;
use std::sync::Arc;

use common_ast::ast::BinaryOperator;
use common_ast::ast::ColumnID;
use common_ast::ast::Expr;
use common_ast::ast::GroupBy;
use common_ast::ast::Identifier;
use common_ast::ast::JoinCondition;
use common_ast::ast::JoinOperator;
use common_ast::ast::Literal;
use common_base::base::tokio;
use common_exception::Result;
use common_expression::block_debug::box_render;
use common_sql::dataframe::Dataframe;
use common_sql::Planner;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::TestFixture;
use goldenfile::Mint;
use tokio_stream::StreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_dataframe() -> Result<()> {
    let fixture = TestFixture::new().await;
    let query_ctx = fixture.ctx();
    fixture.create_default_table().await.unwrap();

    let db = fixture.default_db_name();
    let table = fixture.default_table_name();

    // Count
    {
        let sql = "select count(*) from system.engines";
        let df = Dataframe::scan(query_ctx.clone(), Some("system"), "engines")
            .await
            .unwrap()
            .count()
            .await
            .unwrap();

        test_case(sql, df, fixture.ctx()).await?;
    }

    // select single table
    // scan
    {
        let sql = format!("select id from {}.{}", db, table);
        let df = Dataframe::scan(query_ctx.clone(), Some(&db), &table)
            .await
            .unwrap()
            .select_columns(&["id"])
            .await
            .unwrap();

        test_case(&sql, df, fixture.ctx()).await?;
    }

    // scan_one
    {
        let sql = "select 3 from system.one";
        let df = Dataframe::scan_one(query_ctx.clone())
            .await
            .unwrap()
            .select(vec![Expr::Literal {
                span: None,
                lit: Literal::UInt64(3),
            }])
            .await
            .unwrap();

        test_case(sql, df, fixture.ctx()).await?;
    }

    // limit
    {
        let sql = "select * from system.tables limit 3 offset 1";
        let df = Dataframe::scan(query_ctx.clone(), Some("system"), "tables")
            .await
            .unwrap()
            .limit(Some(3), 1)
            .await
            .unwrap();

        test_case(sql, df, fixture.ctx()).await?;
    }

    // sort
    {
        let sql = "select `Engine` from system.engines order by `Engine`";
        let df = Dataframe::scan(query_ctx.clone(), Some("system"), "engines")
            .await
            .unwrap()
            .sort_column(
                &["Engine"],
                vec![(
                    Expr::ColumnRef {
                        span: None,
                        database: None,
                        table: None,
                        column: ColumnID::Name(Identifier::from_name_with_quoted(
                            "Engine",
                            Some('`'),
                        )),
                    },
                    Some(true),
                    Some(false),
                )],
                false,
            )
            .await
            .unwrap();

        let df2 = Dataframe::scan(query_ctx.clone(), Some("system"), "engines")
            .await
            .unwrap()
            .sort(
                vec![Expr::ColumnRef {
                    span: None,
                    database: Some(Identifier::from_name("system")),
                    table: Some(Identifier::from_name("engines")),
                    column: ColumnID::Name(Identifier {
                        name: "Engine".to_string(),
                        quote: Some('`'),
                        span: None,
                    }),
                }],
                vec![(
                    Expr::ColumnRef {
                        span: None,
                        database: None,
                        table: None,
                        column: ColumnID::Name(Identifier::from_name_with_quoted(
                            "Engine",
                            Some('`'),
                        )),
                    },
                    Some(true),
                    Some(false),
                )],
                false,
            )
            .await
            .unwrap();
        test_case(sql, df, fixture.ctx()).await?;
        test_case(sql, df2, fixture.ctx()).await?;
    }

    // filter
    {
        let sql = "select * from system.tables where name='tables'";
        let df = Dataframe::scan(query_ctx.clone(), Some("system"), "tables")
            .await
            .unwrap()
            .filter(Expr::BinaryOp {
                span: None,
                op: BinaryOperator::Eq,
                left: Box::new(Expr::ColumnRef {
                    span: None,
                    database: Some(Identifier::from_name("system")),
                    table: Some(Identifier::from_name("tables")),
                    column: ColumnID::Name(Identifier::from_name("name")),
                }),
                right: Box::new(Expr::Literal {
                    span: None,
                    lit: Literal::String("tables".to_string()),
                }),
            })
            .await
            .unwrap();

        test_case(sql, df, fixture.ctx()).await?;
    }

    // aggregate
    {
        let sql = "select `Engine` from system.engines group by `Engine` having `Engine`='VIEW'";
        let df = Dataframe::scan(query_ctx.clone(), Some("system"), "engines")
            .await
            .unwrap()
            .aggregate(
                GroupBy::Normal(vec![Expr::ColumnRef {
                    span: None,
                    database: Some(Identifier::from_name("system")),
                    table: Some(Identifier::from_name("engines")),
                    column: ColumnID::Name(Identifier::from_name_with_quoted("Engine", Some('`'))),
                }]),
                vec![Expr::ColumnRef {
                    span: None,
                    database: Some(Identifier::from_name("system")),
                    table: Some(Identifier::from_name("engines")),
                    column: ColumnID::Name(Identifier::from_name_with_quoted("Engine", Some('`'))),
                }],
                Some(Expr::BinaryOp {
                    span: None,
                    op: BinaryOperator::Eq,
                    left: Box::new(Expr::ColumnRef {
                        span: None,
                        database: Some(Identifier::from_name("system")),
                        table: Some(Identifier::from_name("engines")),
                        column: ColumnID::Name(Identifier::from_name_with_quoted(
                            "Engine",
                            Some('`'),
                        )),
                    }),
                    right: Box::new(Expr::Literal {
                        span: None,
                        lit: Literal::String("VIEW".to_string()),
                    }),
                }),
            )
            .await
            .unwrap();

        test_case(sql, df, fixture.ctx()).await?;
    }

    // distinct
    {
        let sql = "select distinct `Engine` from system.engines";
        let df = Dataframe::scan(query_ctx.clone(), Some("system"), "engines")
            .await
            .unwrap()
            .distinct_col(&["Engine"])
            .await
            .unwrap();

        let df2 = Dataframe::scan(query_ctx.clone(), Some("system"), "engines")
            .await
            .unwrap()
            .distinct(vec![Expr::ColumnRef {
                span: None,
                database: Some(Identifier::from_name("system")),
                table: Some(Identifier::from_name("engines")),
                column: ColumnID::Name(Identifier {
                    name: "Engine".to_string(),
                    quote: Some('`'),
                    span: None,
                }),
            }])
            .await
            .unwrap();
        test_case(sql, df, fixture.ctx()).await?;
        test_case(sql, df2, fixture.ctx()).await?;
    }

    // union
    {
        let sql = format!(
            "select * from {}.{} union all select * from {}.{}",
            db, table, db, table
        );
        let df = Dataframe::scan(query_ctx.clone(), Some(&db), &table)
            .await
            .unwrap()
            .union(Dataframe::scan(query_ctx.clone(), Some(&db), &table).await?)
            .await
            .unwrap();

        test_case(&sql, df, fixture.ctx()).await?;
    }

    // union_distinct
    {
        let sql = format!(
            "select * from {}.{} union select * from {}.{}",
            db, table, db, table
        );
        let df = Dataframe::scan(query_ctx.clone(), Some(&db), &table)
            .await
            .unwrap()
            .union_distinct(Dataframe::scan(query_ctx.clone(), Some(&db), &table).await?)
            .await
            .unwrap();

        test_case(&sql, df, fixture.ctx()).await?;
    }

    // intersect
    {
        let sql = "select * from system.engines intersect select * from system.engines";
        let df = Dataframe::scan(query_ctx.clone(), Some("system"), "engines")
            .await
            .unwrap()
            .intersect(Dataframe::scan(query_ctx.clone(), Some("system"), "engines").await?)
            .await
            .unwrap();

        test_case(sql, df, fixture.ctx()).await?;
    }

    // except
    {
        let sql = "select * from system.engines except select * from system.engines";
        let df = Dataframe::scan(query_ctx.clone(), Some("system"), "engines")
            .await
            .unwrap()
            .except(Dataframe::scan(query_ctx.clone(), Some("system"), "engines").await?)
            .await
            .unwrap();

        test_case(sql, df, fixture.ctx()).await?;
    }

    // join
    {
        let sql = "select tables.database, tables.name from system.tables left join system.databases on tables.database=databases.name where name='tables'";
        let df = Dataframe::scan(query_ctx.clone(), Some("system"), "tables")
            .await
            .unwrap()
            .join(
                vec![(Some("system"), "tables"), (Some("system"), "databases")],
                JoinOperator::LeftSemi,
                JoinCondition::On(Box::new(Expr::BinaryOp {
                    span: None,
                    op: BinaryOperator::Eq,
                    left: Box::new(Expr::ColumnRef {
                        span: None,
                        database: None,
                        table: Some(Identifier::from_name("tables")),
                        column: ColumnID::Name(Identifier::from_name("database")),
                    }),
                    right: Box::new(Expr::ColumnRef {
                        span: None,
                        database: None,
                        table: Some(Identifier::from_name("databases")),
                        column: ColumnID::Name(Identifier::from_name("name")),
                    }),
                })),
            )
            .await
            .unwrap()
            .filter(Expr::BinaryOp {
                span: None,
                op: BinaryOperator::Eq,
                left: Box::new(Expr::ColumnRef {
                    span: None,
                    database: Some(Identifier::from_name("system")),
                    table: Some(Identifier::from_name("tables")),
                    column: ColumnID::Name(Identifier::from_name("name")),
                }),
                right: Box::new(Expr::Literal {
                    span: None,
                    lit: Literal::String("tables".to_string()),
                }),
            })
            .await
            .unwrap()
            .select_columns(&["database", "name"])
            .await?;

        test_case(sql, df, fixture.ctx()).await?;
    }

    Ok(())
}

async fn test_case(sql: &str, df: Dataframe, ctx: Arc<QueryContext>) -> Result<()> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await.unwrap();

    let df_plan = df.into_plan(false)?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await.unwrap();
    let stream = interpreter.execute(ctx.clone()).await.unwrap();
    let blocks = stream.map(|v| v).collect::<Vec<_>>().await;
    let interpreter = InterpreterFactory::get(ctx.clone(), &df_plan)
        .await
        .unwrap();
    let stream = interpreter.execute(ctx.clone()).await.unwrap();
    let blocks2 = stream.map(|v| v).collect::<Vec<_>>().await;

    assert_eq!(blocks.len(), blocks2.len());

    let schema = plan.schema();

    for (a, b) in blocks.iter().zip(blocks2.iter()) {
        let a = box_render(&schema, &[a.clone().unwrap()], 40, 0, 0).unwrap();
        let b = box_render(&schema, &[b.clone().unwrap()], 40, 0, 0).unwrap();

        assert_eq!(a, b);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_box_display() {
    let mut mint = Mint::new("tests/it/frame/testdata");
    let mut file = &mint.new_goldenfile("box_display.txt").unwrap();

    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    let cases = vec![
        "select number*10000 as n, concat('a',n::String,'b'),concat('testcc', n::String,'vabc') as c1 from numbers(100)",
    ];

    for sql in cases {
        let mut planner = Planner::new(ctx.clone());
        let (plan, _) = planner.plan_sql(sql).await.unwrap();

        let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await.unwrap();
        let stream = interpreter.execute(ctx.clone()).await.unwrap();
        let blocks = stream.map(|v| v).collect::<Vec<_>>().await;
        let schema = plan.schema();

        writeln!(file, "------------ SQL INFO --------------").unwrap();
        writeln!(file, "{}", sql).unwrap();
        writeln!(file, "---------- DISPLAY INFO ------------").unwrap();
        writeln!(
            file,
            "max_rows {:?}, max_width {:?}, max_col_width {:?}",
            40, 33, 13
        )
        .unwrap();
        for a in blocks.iter() {
            let a = box_render(&schema, &[a.clone().unwrap()], 40, 33, 13).unwrap();
            write!(file, "{}", a).unwrap();
        }

        writeln!(file, "\n---------- DISPLAY INFO ------------").unwrap();
        writeln!(
            file,
            "max_rows {:?}, max_width {:?}, max_col_width {:?}",
            40, 40, 13
        )
        .unwrap();
        for a in blocks.iter() {
            let a = box_render(&schema, &[a.clone().unwrap()], 40, 40, 13).unwrap();
            write!(file, "{}", a).unwrap();
        }
        write!(file, "\n\n").unwrap();
    }
}
