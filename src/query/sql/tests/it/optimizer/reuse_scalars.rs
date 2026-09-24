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

use std::io::Write;

use databend_common_exception::Result;
use databend_common_sql::optimizer::ir::StatContext;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_window_input_reuse_preserves_nondeterministic_expressions() -> Result<()> {
    let case = SqlTestCase {
        name: "window_input_reuse_preserves_nondeterministic_expressions",
        description: "Only deterministic window inputs may replace SELECT expressions.",
        setup_sqls: &["CREATE TABLE t(number UInt64)"],
        sql: "SELECT number + 1 AS n, rand() AS r, \
              row_number() OVER (PARTITION BY number + 1 ORDER BY rand()) FROM t",
    };
    let ctx = setup_context(&case).await?;
    let raw = ctx.bind_sql(case.sql).await?;
    let optimized = ctx.optimize_plan(raw.clone()).await?;
    let mut file = open_golden_file("optimizer", "reuse_scalars_volatile.txt")?;
    write_case_header(&mut file, &case)?;
    writeln!(
        file,
        "raw_plan:\n{}",
        raw.format_indent(Default::default(), &StatContext::default())?
    )?;
    writeln!(
        file,
        "optimized_plan:\n{}",
        optimized.format_indent(Default::default(), &StatContext::default())?
    )?;
    Ok(())
}
