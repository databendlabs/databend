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

mod interpreter_database_drop;
mod interpreter_database_show_create;
mod interpreter_empty;
mod interpreter_factory_interceptor;
mod interpreter_insert;
mod interpreter_select;
mod interpreter_show_databases;
mod interpreter_show_engines;
mod interpreter_show_grant;
mod interpreter_show_roles;
mod interpreter_show_tables;
mod interpreter_show_users;
mod interpreter_table_describe;
mod interpreter_table_drop;
mod interpreter_table_rename;
mod interpreter_table_truncate;

use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use common_datablocks::pretty_format_blocks;
use common_exception::Result;
use databend_query::interpreters::*;
use databend_query::sessions::QueryContext;
use databend_query::sql::PlanParser;
use futures::TryStreamExt;

pub async fn interpreter_goldenfiles(
    file: &mut File,
    ctx: Arc<QueryContext>,
    interpreter: &str,
    query: &str,
) -> Result<()> {
    let plan = PlanParser::parse(ctx.clone(), query).await?;
    let executor = InterpreterFactory::get(ctx.clone(), plan.clone())?;
    assert_eq!(executor.name(), interpreter);
    let stream = executor.execute(None).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let formatted = pretty_format_blocks(&result)?;
    let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

    // sort except for header + footer
    let num_lines = actual_lines.len();
    if num_lines > 3 {
        actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
    }

    writeln!(file, "---------- Input ----------").unwrap();
    writeln!(file, "{}", query).unwrap();
    writeln!(file, "---------- Output ---------").unwrap();
    for line in actual_lines {
        writeln!(file, "{}", line).unwrap();
    }
    writeln!(file, "\n").unwrap();
    Ok(())
}
