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
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Arc;

use databend_common_exception::Result;
use goldenfile::Mint;

use crate::framework::LiteTableContext;

pub(crate) struct SqlTestCase {
    pub name: &'static str,
    pub description: &'static str,
    pub setup_sqls: &'static [&'static str],
    pub sql: &'static str,
}

pub(crate) enum SqlTestOutcome {
    Plan(String),
    Error { code: u16, message: String },
}

pub(crate) struct GoldenFile {
    _mint: Mint,
    file: std::fs::File,
}

impl Deref for GoldenFile {
    type Target = std::fs::File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl DerefMut for GoldenFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl Write for GoldenFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

pub(crate) fn open_golden_file(group: &str, file_name: &str) -> Result<GoldenFile> {
    let mut mint = Mint::new("tests/it");
    let path = format!("{group}/{file_name}");
    Ok(GoldenFile {
        file: mint.new_goldenfile(&path)?,
        _mint: mint,
    })
}

pub(crate) async fn setup_context(case: &SqlTestCase) -> Result<Arc<LiteTableContext>> {
    let ctx = LiteTableContext::create().await?;
    for setup_sql in case.setup_sqls {
        ctx.register_table_sql(setup_sql).await?;
    }
    Ok(ctx)
}

pub(crate) fn write_case_title(file: &mut impl Write, name: &str, description: &str) -> Result<()> {
    writeln!(file, "=== {name} ===")?;
    if !description.is_empty() {
        writeln!(file, "description: {description}")?;
    }
    Ok(())
}

pub(crate) fn write_case_header(file: &mut impl Write, case: &SqlTestCase) -> Result<()> {
    write_case_title(file, case.name, case.description)?;
    writeln!(file, "sql: {}", case.sql)?;
    Ok(())
}

pub(crate) fn write_case_outcome(file: &mut impl Write, outcome: &SqlTestOutcome) -> Result<()> {
    match outcome {
        SqlTestOutcome::Plan(plan) => {
            writeln!(file, "status: ok")?;
            writeln!(file, "{plan}")?;
        }
        SqlTestOutcome::Error { code, message } => {
            writeln!(file, "status: error")?;
            writeln!(file, "code: {code}")?;
            writeln!(file, "message: {message}")?;
        }
    }

    writeln!(file)?;
    Ok(())
}
