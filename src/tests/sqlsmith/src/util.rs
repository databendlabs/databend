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
    Tidb,
    Postgres,
    Cockroach,
    Doris,
    Starrocks,
}

// Walk through the sqllogic tests folder and read the test SQL in it as seed to generate fuzzed SQL.
pub(crate) fn read_sql_from_test_dirs(fuzz_path: &str) -> Result<Vec<String>> {
    let test_type = if fuzz_path.contains("sqlite") {
        TestType::Sqlite
    } else if fuzz_path.contains("duckdb") {
        TestType::Duckdb
    } else if fuzz_path.contains("tidb") {
        TestType::Tidb
    } else if fuzz_path.contains("postgres") {
        TestType::Postgres
    } else if fuzz_path.contains("cockroach") {
        TestType::Cockroach
    } else if fuzz_path.contains("doris") {
        TestType::Doris
    } else if fuzz_path.contains("starrocks") {
        TestType::Starrocks
    } else {
        TestType::Databend
    };

    let path = Path::new(fuzz_path);
    let mut paths = vec![];
    collect_paths(&test_type, path, &mut paths)?;

    let mut sqls = vec![];
    for path in paths.into_iter() {
        let content = read_to_string(path)?;
        let lines: Vec<&str> = content.lines().collect();
        match test_type {
            TestType::Databend | TestType::Duckdb | TestType::Cockroach => {
                read_sqllogict_test_sqls(lines, &mut sqls);
            }
            TestType::Sqlite => {
                read_sqlite_test_sqls(lines, &mut sqls);
            }
            TestType::Tidb | TestType::Postgres | TestType::Doris | TestType::Starrocks => {
                read_test_sqls(lines, &mut sqls);
            }
        }
    }
    Ok(sqls)
}

fn collect_paths(test_type: &TestType, path: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    let file_ext = match test_type {
        TestType::Postgres | TestType::Doris => "sql",
        TestType::Cockroach | TestType::Starrocks => "",
        _ => "test",
    };
    if path.is_dir() {
        for entry in (path.read_dir()?).flatten() {
            let sub_path = entry.path();
            collect_paths(test_type, &sub_path, paths)?;
        }
    } else if path.is_file() {
        let is_test_file = match test_type {
            TestType::Cockroach | TestType::Starrocks => path.extension().is_none(),
            _ => path.extension().is_some_and(|ext| ext == file_ext),
        };
        if is_test_file {
            paths.push(path.to_path_buf());
        }
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

// Read and parse Postgres/Tidb/Doris/Starrocks test files.
fn read_test_sqls(lines: Vec<&str>, sqls: &mut Vec<String>) {
    let mut current_sql = String::new();
    let mut in_properties = false;

    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("#") || line.starts_with("--") || line.starts_with("set") {
            in_properties = false;
            if !current_sql.is_empty() {
                sqls.push(current_sql.clone());
                current_sql.clear();
            }
            continue;
        } else {
            let line = line.replace("primary key", "");
            let line = line.replace("ENGINE=OLAP", "");
            if line.starts_with("PROPERTIES") {
                in_properties = true;
            }
            if !line.starts_with("DUPLICATE KEY")
                && !line.starts_with("AGGREGATE KEY")
                && !line.starts_with("PARTITION BY")
                && !line.starts_with("DISTRIBUTED BY")
                && !in_properties
            {
                if !current_sql.is_empty() {
                    current_sql.push(' ');
                }
                current_sql.push_str(&line);
            }
            if line.ends_with(";") {
                in_properties = false;
                sqls.push(current_sql.clone());
                current_sql.clear();
            }
        }
    }
    if !current_sql.is_empty() {
        sqls.push(current_sql);
    }
}
