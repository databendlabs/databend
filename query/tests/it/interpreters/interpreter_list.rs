// Copyright 2022 Datafuse Labs.
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
use common_meta_types::StageFile;
use common_meta_types::UserIdentity;
use databend_query::interpreters::*;
use databend_query::sql::PlanParser;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_list_stage_interpreter() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;
    let tenant = ctx.get_tenant();
    let user_mgr = ctx.get_user_manager();

    // create stage
    {
        let query = "CREATE stage test_stage";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let _ = executor.execute(None).await?;
    }

    // list stage
    {
        let query = "LIST @test_stage";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        assert_eq!(executor.name(), "ListInterpreter");
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+------+------+-----+---------------+---------+",
            "| name | size | md5 | last_modified | creator |",
            "+------+------+-----+---------------+---------+",
            "+------+------+-----+---------------+---------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // add file
    {
        let stage_file = StageFile {
            path: "books.csv".to_string(),
            size: 100,
            ..Default::default()
        };
        user_mgr.add_file(&tenant, "test_stage", stage_file).await?;
    }

    // list stage
    {
        let query = "LIST @test_stage";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+-----------+------+------+-------------------------------+---------+",
            "| name      | size | md5  | last_modified                 | creator |",
            "+-----------+------+------+-------------------------------+---------+",
            "| books.csv | 100  | NULL | 1970-01-01 00:00:00.000 +0000 | NULL    |",
            "+-----------+------+------+-------------------------------+---------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // add file with dir
    {
        let stage_file = StageFile {
            path: "test/books.csv".to_string(),
            size: 100,
            creator: Some(UserIdentity {
                username: "test_user".to_string(),
                hostname: "%".to_string(),
            }),
            ..Default::default()
        };
        user_mgr.add_file(&tenant, "test_stage", stage_file).await?;
    }

    // list stage
    {
        let query = "LIST @test_stage";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+----------------+------+------+-------------------------------+-----------------+",
            "| name           | size | md5  | last_modified                 | creator         |",
            "+----------------+------+------+-------------------------------+-----------------+",
            "| books.csv      | 100  | NULL | 1970-01-01 00:00:00.000 +0000 | NULL            |",
            "| test/books.csv | 100  | NULL | 1970-01-01 00:00:00.000 +0000 | 'test_user'@'%' |",
            "+----------------+------+------+-------------------------------+-----------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // list stage with file path
    {
        let query = "LIST @test_stage/test";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+------+------+-----+---------------+---------+",
            "| name | size | md5 | last_modified | creator |",
            "+------+------+-----+---------------+---------+",
            "+------+------+-----+---------------+---------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // list stage with dir path
    {
        let query = "LIST @test_stage/test/";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+----------------+------+------+-------------------------------+-----------------+",
            "| name           | size | md5  | last_modified                 | creator         |",
            "+----------------+------+------+-------------------------------+-----------------+",
            "| test/books.csv | 100  | NULL | 1970-01-01 00:00:00.000 +0000 | 'test_user'@'%' |",
            "+----------------+------+------+-------------------------------+-----------------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // list stage with pattern
    {
        let query = "LIST @test_stage pattern = '^books.*'";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+-----------+------+------+-------------------------------+---------+",
            "| name      | size | md5  | last_modified                 | creator |",
            "+-----------+------+------+-------------------------------+---------+",
            "| books.csv | 100  | NULL | 1970-01-01 00:00:00.000 +0000 | NULL    |",
            "+-----------+------+------+-------------------------------+---------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    // list stage with pattern
    {
        let query = "LIST @test_stage/test/ pattern = '^books.*'";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let expected = vec![
            "+------+------+-----+---------------+---------+",
            "| name | size | md5 | last_modified | creator |",
            "+------+------+-----+---------------+---------+",
            "+------+------+-----+---------------+---------+",
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }
    Ok(())
}
