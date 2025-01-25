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

use std::fs::read_to_string;
use std::path::Path;
use std::path::PathBuf;

use databend_common_exception::Result;

// The fuzz test type
#[derive(Default)]
enum TestType {
    #[default]
    Databend,
    Duckdb,
    Sqlite,
}

// Walk through the sqllogic tests folder and read the test SQL in it as seed to generate fuzzed SQL.
pub(crate) fn read_sql_from_test_dirs(fuzz_path: &str) -> Result<Vec<String>> {
    let test_type = if fuzz_path.contains("sqlite") {
        TestType::Sqlite
    } else if fuzz_path.contains("duckdb") {
        TestType::Duckdb
    } else {
        TestType::Databend
    };

    let path = Path::new(fuzz_path);
    let mut paths = vec![];
    collect_paths(path, &mut paths)?;

    let mut sqls = vec![];
    for path in paths.into_iter() {
        let content = read_to_string(path)?;
        let lines: Vec<&str> = content.lines().collect();
        match test_type {
            TestType::Databend | TestType::Duckdb => {
                read_sqllogict_test_sqls(lines, &mut sqls);
            }
            TestType::Sqlite => {
                read_sqlite_test_sqls(lines, &mut sqls);
            }
        }
    }
    Ok(sqls)
}

fn collect_paths(path: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_dir() {
        for entry in (path.read_dir()?).flatten() {
            let sub_path = entry.path();
            collect_paths(&sub_path, paths)?;
        }
    } else if path.is_file() && path.extension().is_some_and(|ext| ext == "test") {
        paths.push(path.to_path_buf());
    }
    Ok(())
}

// Read and parse Sqllogic test files, include Databend and Duckdb.
fn read_sqllogict_test_sqls(lines: Vec<&str>, sqls: &mut Vec<String>) {
    let mut in_sql = false;
    let mut current_sql = String::new();

    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("statement") || line.starts_with("query") {
            if !current_sql.is_empty() {
                sqls.push(current_sql.clone());
                current_sql.clear();
            }
            in_sql = !line.starts_with("statement error");
        } else if line.eq("----") {
            // ignore result values
            in_sql = false;
        } else if !line.is_empty()
            && !line.starts_with("--")
            && !line.starts_with("#")
            && !line.starts_with("include")
            && !line.starts_with("onlyif")
            && in_sql
        {
            if current_sql.is_empty() {
                current_sql.push_str(line);
            } else {
                current_sql.push(' ');
                current_sql.push_str(line);
            }
        }
    }
    if !current_sql.is_empty() {
        sqls.push(current_sql);
    }
}

// Read and parse Sqlite test files
fn read_sqlite_test_sqls(lines: Vec<&str>, sqls: &mut Vec<String>) {
    let mut in_sql = false;
    let mut current_sql = String::new();

    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("do_execsql_test") {
            in_sql = true;
        } else if line.starts_with("}") {
            if !current_sql.is_empty() {
                sqls.push(current_sql.clone());
                current_sql.clear();
            }
            in_sql = false;
        } else if in_sql {
            let line = line.replace("PRIMARY KEY", "");
            let line = line.replace("WITHOUT ROWID", "");
            current_sql.push(' ');
            current_sql.push_str(&line);
            if line.ends_with(";") {
                sqls.push(current_sql.clone());
                current_sql.clear();
            }
        }
    }
}
