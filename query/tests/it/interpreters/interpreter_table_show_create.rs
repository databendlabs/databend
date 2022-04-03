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

use common_base::tokio;
use common_exception::Result;
use databend_query::interpreters::*;
use databend_query::sql::PlanParser;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn interpreter_show_create_table_test() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;

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
            "|       |   `a` Int64,                        |",
            "|       |   `b` Int32,                        |",
            "|       |   `c` String,                       |",
            "|       |   `d` Int16,                        |",
            "|       |   `e` Date16,                       |",
            "|       | ) ENGINE=Null COMMENT='test create' |",
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
            "|       |   `a` Int32,                        |",
            "|       | ) ENGINE=fuse COMMENT='test create' |",
            "+-------+-------------------------------------+",
        ],
        name: "internal options should not be shown in fuse engine",
    };

    //  after insertion, the table snapshot will be created
    //  with the corresponding table options which should not be shown
    let internal_opts_after_insert = Case {
        create_stmt: vec![
            "CREATE TABLE s( a int) Engine = fuse COMMENT = 'test create'",
            "insert into s values(1)",
        ],
        show_stmt: "SHOW CREATE TABLE s",
        expects: vec![
            "+-------+-------------------------------------+",
            "| Table | Create Table                        |",
            "+-------+-------------------------------------+",
            "| s     | CREATE TABLE `s` (                  |",
            "|       |   `a` Int32,                        |",
            "|       | ) ENGINE=fuse COMMENT='test create' |",
            "+-------+-------------------------------------+",
        ],
        name: "internal options should not be shown in fuse engine",
    };

    let cases = vec![normal_case, internal_opt, internal_opts_after_insert];

    for case in cases {
        for stmt in case.create_stmt {
            let plan = PlanParser::parse(ctx.clone(), stmt).await?;
            let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
            let _ = executor.execute(None).await?;
        }
        let plan = PlanParser::parse(ctx.clone(), case.show_stmt).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ShowCreateTableInterpreter");
        let result = executor
            .execute(None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        common_datablocks::assert_blocks_sorted_eq_with_name(
            case.name,
            case.expects,
            result.as_slice(),
        );
    }

    Ok(())
}
