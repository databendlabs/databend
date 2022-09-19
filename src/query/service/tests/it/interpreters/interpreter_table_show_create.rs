// Copyright 2021 Datafuse Labs.
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

use common_base::base::tokio;
use common_exception::Result;
use databend_query::interpreters::*;
use databend_query::sql::planner::Planner;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn interpreter_show_create_table_test() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let mut planner = Planner::new(ctx.clone());

    struct Case<'a> {
        create_stmt: Vec<&'a str>,
        show_stmt: &'a str,
        expects: Vec<&'a str>,
        name: &'a str,
    }

    let normal_case = Case {
        create_stmt: vec![
            "
            CREATE TABLE default.a(\
                a bigint, b int, c varchar(255), d smallint, e Date\
            ) Engine = Null COMMENT = 'test create'\
        ",
        ],
        show_stmt: "SHOW CREATE TABLE a",
        expects: vec![
            "+-------+-------------------------------------+",
            "| Table | Create Table                        |",
            "+-------+-------------------------------------+",
            "| a     | CREATE TABLE `a` (                  |",
            "|       |   `a` BIGINT,                       |",
            "|       |   `b` INT,                          |",
            "|       |   `c` VARCHAR,                      |",
            "|       |   `d` SMALLINT,                     |",
            "|       |   `e` DATE                          |",
            "|       | ) ENGINE=NULL COMMENT='test create' |",
            "+-------+-------------------------------------+",
        ],
        name: "normal case",
    };

    let internal_opt = Case {
        create_stmt: vec!["CREATE TABLE t( a int) Engine = fuse COMMENT = 'test create'"],
        show_stmt: "SHOW CREATE TABLE t",
        expects: vec![
            "+-------+-------------------------------------+",
            "| Table | Create Table                        |",
            "+-------+-------------------------------------+",
            "| t     | CREATE TABLE `t` (                  |",
            "|       |   `a` INT                           |",
            "|       | ) ENGINE=FUSE COMMENT='test create' |",
            "+-------+-------------------------------------+",
        ],
        name: "internal options should not be shown in fuse engine",
    };

    let cases = vec![normal_case, internal_opt];

    for case in cases {
        for stmt in case.create_stmt {
            let (plan, _, _) = planner.plan_sql(stmt).await?;
            let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
            let _ = executor.execute(ctx.clone()).await?;
        }
        let (plan, _, _) = planner.plan_sql(case.show_stmt).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "ShowCreateTableInterpreter");
        let result = executor
            .execute(ctx.clone())
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        common_datablocks::assert_blocks_eq_with_name(case.name, case.expects, result.as_slice());
    }

    Ok(())
}

#[tokio::test]
async fn interpreter_show_create_table_with_comments_test() -> Result<()> {
    let (_guard, ctx) = crate::tests::create_query_context().await?;
    let _planner = Planner::new(ctx.clone());

    struct Case<'a> {
        create_stmt: Vec<&'a str>,
        show_stmt: &'a str,
        expects: Vec<&'a str>,
        name: &'a str,
    }

    let normal_case = Case {
        create_stmt: vec![
            "
            CREATE TABLE default.a(\
                a bigint comment 'a', b int comment 'b\\'b\\'b',\
                c varchar(255) comment 'c', d smallint comment '', e Date\
            ) Engine = Null COMMENT = 'test create'\
        ",
        ],
        show_stmt: "SHOW CREATE TABLE a",
        expects: vec![
            "+-------+-------------------------------------+",
            "| Table | Create Table                        |",
            "+-------+-------------------------------------+",
            "| a     | CREATE TABLE `a` (                  |",
            "|       |   `a` BIGINT COMMENT 'a',           |",
            "|       |   `b` INT COMMENT 'b\\'b\\'b',        |",
            "|       |   `c` VARCHAR COMMENT 'c',          |",
            "|       |   `d` SMALLINT,                     |",
            "|       |   `e` DATE                          |",
            "|       | ) ENGINE=NULL COMMENT='test create' |",
            "+-------+-------------------------------------+",
        ],
        name: "normal case",
    };

    let internal_opt = Case {
        create_stmt: vec![
            "CREATE TABLE t( a int comment 'column comment') Engine = fuse COMMENT = 'test create'",
        ],
        show_stmt: "SHOW CREATE TABLE t",
        expects: vec![
            "+-------+-------------------------------------+",
            "| Table | Create Table                        |",
            "+-------+-------------------------------------+",
            "| t     | CREATE TABLE `t` (                  |",
            "|       |   `a` INT COMMENT 'column comment'  |",
            "|       | ) ENGINE=FUSE COMMENT='test create' |",
            "+-------+-------------------------------------+",
        ],
        name: "internal options should not be shown in fuse engine",
    };

    let cases = vec![normal_case, internal_opt];

    let mut planner = Planner::new(ctx.clone());

    for case in cases {
        for stmt in case.create_stmt {
            // create table in the new planner to support column comment.
            let (plan, _, _) = planner.plan_sql(stmt).await?;
            let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
            let _ = executor.execute(ctx.clone()).await?;
        }
        let (plan, _, _) = planner.plan_sql(case.show_stmt).await?;
        let executor = InterpreterFactoryV2::get(ctx.clone(), &plan).await?;
        assert_eq!(executor.name(), "ShowCreateTableInterpreter");
        let result = executor
            .execute(ctx.clone())
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        common_datablocks::assert_blocks_eq_with_name(case.name, case.expects, result.as_slice());
    }

    Ok(())
}
