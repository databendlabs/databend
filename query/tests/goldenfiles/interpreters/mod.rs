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

mod interpreter_show_tables;

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
    writeln!(file, "---------- Input ----------").unwrap();
    writeln!(file, "{}", query).unwrap();
    writeln!(file, "---------- Output ---------").unwrap();
    writeln!(file, "{}", formatted).unwrap();
    writeln!(file, "\n").unwrap();
    Ok(())
}
