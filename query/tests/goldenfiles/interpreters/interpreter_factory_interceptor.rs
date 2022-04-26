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
use databend_query::sql::*;
use futures::TryStreamExt;
use goldenfile::Mint;
use pretty_assertions::assert_eq;

use crate::interpreters::interpreter_goldenfiles;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_interpreter_interceptor() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context().await?;
    {
        let query = "select number from numbers_mt(100) where number > 90";
        ctx.attach_query_str(query);
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;
        interpreter.start().await?;
        let stream = interpreter.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let block = &result[0];
        assert_eq!(block.num_columns(), 1);
        interpreter.finish().await?;
    }

    // Check.
    {
        let mut mint = Mint::new("tests/goldenfiles/data");
        let mut file = mint.new_goldenfile("factory_interceptor.txt").unwrap();

        interpreter_goldenfiles(&mut file, ctx.clone(), "SelectInterpreter", r#"select log_type, handler_type, cpu_usage, scan_rows, scan_bytes, scan_partitions, written_rows, written_bytes, result_rows, result_bytes, query_kind, query_text, sql_user, sql_user_quota from system.query_log"#).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_interpreter_interceptor_for_insert() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context().await?;
    {
        let query = "create table t as select number from numbers_mt(1)";
        ctx.attach_query_str(query);
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;
        interpreter.start().await?;
        let stream = interpreter.execute(None).await?;
        stream.try_collect::<Vec<_>>().await?;
        interpreter.finish().await?;
    }

    // Check.
    {
        let mut mint = Mint::new("tests/goldenfiles/data");
        let mut file = mint
            .new_goldenfile("factory_interceptor_for_insert.txt")
            .unwrap();

        interpreter_goldenfiles(&mut file, ctx.clone(), "SelectInterpreter", r#"select log_type, handler_type, cpu_usage, scan_rows, scan_bytes, scan_partitions, written_rows, written_bytes, result_rows, result_bytes, query_kind, query_text, sql_user, sql_user_quota from system.query_log"#).await?;
    }

    Ok(())
}
