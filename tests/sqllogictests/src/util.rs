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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use bollard::Docker;
use bollard::container::RemoveContainerOptions;
use glob::glob;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use testcontainers::ContainerAsync;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::core::client::docker_client_instance;
use testcontainers::core::logs::consumer::logging_consumer::LoggingConsumer;
use testcontainers::runners::AsyncRunner;
use walkdir::WalkDir;

use crate::arg::SqlLogicTestArgs;
use crate::client::QueryResultFormat;
use crate::error::DSqlLogicTestError;
use crate::error::Result;

const CONTAINER_RETRY_TIMES: usize = 3;
const CONTAINER_STARTUP_TIMEOUT_SECONDS: u64 = 60;
const CONTAINER_TIMEOUT_SECONDS: u64 = 300;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HttpSessionConf {
    pub catalog: Option<String>,
    pub database: Option<String>,
    pub role: Option<String>,
    pub secondary_roles: Option<Vec<String>>,
    pub settings: Option<BTreeMap<String, String>>,
    pub txn_state: Option<String>,
    pub internal: String,
}

pub fn parser_rows(rows: &Value) -> Result<Vec<Vec<String>>> {
    let mut parsed_rows = Vec::new();
    for row in rows.as_array().unwrap() {
        let mut parsed_row = Vec::new();
        for col in row.as_array().unwrap() {
            match col {
                Value::Null => {
                    parsed_row.push("NULL".to_string());
                }
                Value::String(cell) => {
                    // If the result is empty, we'll use `(empty)` to mark it explicitly to avoid confusion
                    if cell.is_empty() {
                        parsed_row.push("(empty)".to_string());
                    } else {
                        parsed_row.push(cell.to_string());
                    }
                }
                _ => unreachable!(),
            }
        }
        parsed_rows.push(parsed_row);
    }
    Ok(parsed_rows)
}

fn find_specific_dir(dir: &str, suit: PathBuf) -> Result<PathBuf> {
    for entry in WalkDir::new(suit)
        .min_depth(0)
        .max_depth(100)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter()
    {
        let entry = entry?;
        if entry.file_type().is_dir() && entry.file_name().to_str().unwrap() == dir {
            return Ok(entry.into_path());
        }
    }
    Err(DSqlLogicTestError::SelfError(
        "Didn't find specific dir".to_string(),
    ))
}

fn get_legacy_files(suit: PathBuf, args: &SqlLogicTestArgs) -> Result<Vec<PathBuf>> {
    let mut files = vec![];
    let dirs = match args.dir {
        Some(ref dir) => find_specific_dir(dir, suit).ok(),
        None => Some(suit),
    };
    let target = match dirs {
        Some(dir) => dir,
        None => return Ok(vec![]),
    };
    for entry in WalkDir::new(target)
        .min_depth(0)
        .max_depth(100)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter()
        .filter_entry(|entry| {
            if let Some(skipped_dir) = args.skipped_dir.as_ref() {
                let dirs = skipped_dir.split(',').collect::<Vec<_>>();
                return !dirs.contains(&entry.file_name().to_str().unwrap());
            }
            true
        })
    {
        let entry = entry?;
        if !entry.file_type().is_dir() {
            files.push(entry.into_path());
        }
    }
    Ok(files)
}

fn expand_path(path: &Path, files: &mut BTreeSet<PathBuf>) -> Result<()> {
    if path.is_file() {
        files.insert(path.to_path_buf());
        return Ok(());
    }
    if path.is_dir() {
        for entry in WalkDir::new(path)
            .min_depth(0)
            .max_depth(100)
            .sort_by(|a, b| a.file_name().cmp(b.file_name()))
            .into_iter()
        {
            let entry = entry?;
            if !entry.file_type().is_dir() {
                files.insert(entry.into_path());
            }
        }
    }
    Ok(())
}

fn expand_glob_patterns(patterns: &[String]) -> Result<BTreeSet<PathBuf>> {
    let mut files = BTreeSet::new();
    for pattern in patterns {
        let entries = glob(pattern).map_err(|e| {
            DSqlLogicTestError::SelfError(format!("Invalid glob pattern '{pattern}': {e}"))
        })?;
        for entry in entries {
            let path = entry.map_err(|e| {
                DSqlLogicTestError::SelfError(format!(
                    "Failed to resolve glob entry for pattern '{pattern}': {e}"
                ))
            })?;
            expand_path(&path, &mut files)?;
        }
    }
    Ok(files)
}

fn get_glob_files(args: &SqlLogicTestArgs) -> Result<Vec<PathBuf>> {
    let Some(patterns) = args.run.as_ref() else {
        return Ok(vec![]);
    };

    let mut selected = expand_glob_patterns(patterns)?;
    if let Some(skip_patterns) = args.skip.as_ref() {
        let skipped = expand_glob_patterns(skip_patterns)?;
        selected.retain(|path| !skipped.contains(path));
    }

    Ok(selected.into_iter().collect())
}

fn get_suite_files(args: &SqlLogicTestArgs, suites: &[String]) -> Result<Vec<PathBuf>> {
    let suites_root = Path::new(&args.suites).canonicalize()?;
    let mut files = BTreeSet::new();
    for suite in suites {
        let relative = Path::new(suite);
        if relative.is_absolute() {
            return Err(DSqlLogicTestError::SelfError(format!(
                "Suite '{suite}' must be relative to {}",
                suites_root.display()
            )));
        }
        let suite_path = suites_root.join(relative).canonicalize().map_err(|error| {
            DSqlLogicTestError::SelfError(format!(
                "Failed to resolve suite '{suite}' under {}: {error}",
                suites_root.display()
            ))
        })?;
        if !suite_path.starts_with(&suites_root) || !suite_path.is_dir() {
            return Err(DSqlLogicTestError::SelfError(format!(
                "Suite '{suite}' must be a directory under {}",
                suites_root.display()
            )));
        }
        expand_path(&suite_path, &mut files)?;
    }
    Ok(files.into_iter().collect())
}

pub fn collect_files(args: &SqlLogicTestArgs) -> Result<Vec<PathBuf>> {
    if args.run.is_some() {
        return get_glob_files(args);
    }
    if let Some(suites) = args.run_suite.as_ref() {
        return get_suite_files(args, suites);
    }

    let mut files = vec![];
    let suits = std::fs::read_dir(&args.suites)?;
    for suit in suits {
        files.extend(get_legacy_files(suit?.path(), args)?);
    }
    Ok(files)
}

pub fn collect_test_files(args: &SqlLogicTestArgs) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for file in collect_files(args)? {
        let file_name = file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if !file_name.ends_with(".test") {
            continue;
        }
        if let Some(specific_file) = &args.file
            && !specific_file.split(',').any(|name| name == file_name)
        {
            continue;
        }
        if let Some(skipped_file) = &args.skipped_file
            && skipped_file.split(',').any(|name| name == file_name)
        {
            continue;
        }
        files.push(file);
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    fn make_args(suites: String) -> SqlLogicTestArgs {
        SqlLogicTestArgs {
            run: None,
            run_suite: None,
            skip: None,
            dir: None,
            file: None,
            skipped_dir: None,
            skipped_file: None,
            handlers: None,
            suites,
            complete: false,
            no_fail_fast: false,
            parallel: 1,
            enable_sandbox: false,
            debug: false,
            bench: false,
            force_load: false,
            database: "default".to_string(),
            port: 8000,
        }
    }

    fn write_test_file(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, "statement ok\nselect 1;\n").unwrap();
    }

    #[test]
    fn collect_files_supports_run_dir_and_skip_dir() {
        let temp = tempdir().unwrap();
        let suites = temp.path().join("suites");
        let selected = suites.join("base/selected/selected.test");
        let skipped = suites.join("base/selected/skipped/skipped.test");
        let other = suites.join("base/other/other.test");
        write_test_file(&selected);
        write_test_file(&skipped);
        write_test_file(&other);

        let mut args = make_args(suites.to_string_lossy().into_owned());
        args.dir = Some("selected".to_string());
        args.skipped_dir = Some("skipped".to_string());

        assert_eq!(collect_test_files(&args).unwrap(), vec![selected]);
    }

    #[test]
    fn collect_test_files_applies_run_file_and_skip_file() {
        let temp = tempdir().unwrap();
        let suites = temp.path().join("suites");
        let first = suites.join("base/first.test");
        let second = suites.join("base/second.test");
        write_test_file(&first);
        write_test_file(&second);

        let mut args = make_args(suites.to_string_lossy().into_owned());
        args.file = Some("first.test,second.test".to_string());
        args.skipped_file = Some("first.test".to_string());

        assert_eq!(collect_test_files(&args).unwrap(), vec![second]);
    }

    #[test]
    fn collect_files_supports_multiple_top_level_suites() {
        let temp = tempdir().unwrap();
        let suites = temp.path().join("suites");
        let base = suites.join("base/base.test");
        let query = suites.join("query/query.test");
        write_test_file(&base);
        write_test_file(&query);

        let mut args = make_args(suites.to_string_lossy().into_owned());
        args.run_suite = Some(vec![
            "base".to_string(),
            "query".to_string(),
            "base".to_string(),
        ]);

        assert_eq!(collect_files(&args).unwrap(), vec![base, query]);
    }

    #[test]
    fn collect_files_rejects_suite_outside_root() {
        let temp = tempdir().unwrap();
        let suites = temp.path().join("suites");
        let external = temp.path().join("external/external.test");
        fs::create_dir_all(&suites).unwrap();
        write_test_file(&external);

        let mut args = make_args(suites.to_string_lossy().into_owned());
        args.run_suite = Some(vec!["../external".to_string()]);

        assert!(collect_files(&args).is_err());
    }

    #[test]
    fn collect_files_rejects_absolute_suite_path() {
        let temp = tempdir().unwrap();
        let suites = temp.path().join("suites");
        let external = temp.path().join("external");
        fs::create_dir_all(&suites).unwrap();
        fs::create_dir_all(&external).unwrap();

        let mut args = make_args(suites.to_string_lossy().into_owned());
        args.run_suite = Some(vec![external.to_string_lossy().into_owned()]);

        let error = collect_files(&args).unwrap_err().to_string();
        assert!(error.contains("must be relative to"), "{error}");
    }

    #[test]
    fn collect_files_rejects_missing_suite() {
        let temp = tempdir().unwrap();
        let suites = temp.path().join("suites");
        fs::create_dir_all(&suites).unwrap();

        let mut args = make_args(suites.to_string_lossy().into_owned());
        args.run_suite = Some(vec!["missing".to_string()]);

        let error = collect_files(&args).unwrap_err().to_string();
        assert!(
            error.contains("Failed to resolve suite 'missing'"),
            "{error}"
        );
    }

    #[test]
    fn collect_files_supports_glob_file_and_directory_patterns() {
        let temp = tempdir().unwrap();
        let suites = temp.path().join("ignored-suites");
        let dir_file = temp.path().join("cases/base/00_dummy/00_0000_dummy.test");
        let extra_file = temp.path().join("cases/extra/10_0000_extra.test");
        write_test_file(&dir_file);
        write_test_file(&extra_file);

        let mut args = make_args(suites.to_string_lossy().into_owned());
        args.run = Some(vec![
            temp.path()
                .join("cases/base/00_dummy")
                .to_string_lossy()
                .into_owned(),
            temp.path()
                .join("cases/extra/*.test")
                .to_string_lossy()
                .into_owned(),
        ]);

        let files = collect_files(&args).unwrap();

        assert_eq!(files, vec![dir_file, extra_file]);
    }

    #[test]
    fn collect_files_applies_glob_skip_and_deduplicates_matches() {
        let temp = tempdir().unwrap();
        let suites = temp.path().join("ignored-suites");
        let keep_file = temp.path().join("cases/base/00_dummy/00_keep.test");
        let skipped_glob = temp.path().join("cases/base/00_dummy/00_skip.test");
        write_test_file(&keep_file);
        write_test_file(&skipped_glob);

        let mut args = make_args(suites.to_string_lossy().into_owned());
        args.run = Some(vec![
            temp.path()
                .join("cases/base/00_dummy")
                .to_string_lossy()
                .into_owned(),
            keep_file.to_string_lossy().into_owned(),
        ]);
        args.skip = Some(vec![skipped_glob.to_string_lossy().into_owned()]);

        let files = collect_files(&args).unwrap();

        assert_eq!(files, vec![keep_file]);
    }
}

pub async fn run_ttc_container(
    image: &str,
    port: u16,
    http_server_port: u16,
    cs: &mut Vec<ContainerAsync<GenericImage>>,
    query_result_format: QueryResultFormat,
) -> Result<()> {
    let docker = &docker_client_instance().await?;
    let mut images = image.split(":");
    let image = images.next().unwrap();
    let tag = images.next().unwrap_or("latest");

    use rand::Rng;
    use rand::distributions::Alphanumeric;
    let rng = rand::thread_rng();
    let x: String = rng
        .sample_iter(&Alphanumeric)
        .take(5)
        .map(char::from)
        .collect();
    let container_name = format!("databend-ttc-{}-{}", port, x);
    let start = Instant::now();
    println!("Starting container {container_name}");
    let mut dsn = format!(
        "databend://root:@127.0.0.1:{}?sslmode=disable",
        http_server_port
    );
    if matches!(query_result_format, QueryResultFormat::Arrow) {
        dsn = format!("{dsn}&query_result_format=arrow");
    }

    let mut i = 1;
    loop {
        let log_consumer = LoggingConsumer::new();

        let container_res = GenericImage::new(image, tag)
            .with_exposed_port(port.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .with_startup_timeout(Duration::from_secs(CONTAINER_STARTUP_TIMEOUT_SECONDS))
            .with_network("host")
            .with_env_var("DATABEND_DSN", &dsn)
            .with_env_var("TTC_PORT", format!("{port}"))
            .with_container_name(&container_name)
            .with_log_consumer(log_consumer)
            .start()
            .await;
        let duration = start.elapsed().as_secs();
        match container_res {
            Ok(container) => {
                println!(
                    "Started container {container_name} {} using {duration} secs",
                    container.id(),
                );
                cs.push(container);
                return Ok(());
            }
            Err(err) => {
                eprintln!(
                    "Failed to start container {container_name} using {duration} secs: {err}"
                );
                stop_container(docker, &container_name).await;
                if i == CONTAINER_RETRY_TIMES || duration >= CONTAINER_TIMEOUT_SECONDS {
                    break;
                } else {
                    println!(
                        "Retrying to start container {container_name} {i} after {duration} secs",
                    );
                    i += 1;
                }
            }
        }
    }
    Err(format!("Start {container_name} failed").into())
}

async fn stop_container(docker: &Docker, container_name: &str) {
    if docker
        .inspect_container(container_name, None)
        .await
        .is_err()
    {
        return;
    }
    let _ = docker.stop_container(container_name, None).await;
    let _ = docker
        .remove_container(
            container_name,
            Some(RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        )
        .await;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ColumnType {
    Bool,
    Text,
    Integer,
    FloatingPoint,
    Any,
}

impl sqllogictest::ColumnType for ColumnType {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'B' => Some(Self::Bool),
            'T' => Some(Self::Text),
            'I' => Some(Self::Integer),
            'R' => Some(Self::FloatingPoint),
            _ => Some(Self::Any),
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Bool => 'B',
            Self::Text => 'T',
            Self::Integer => 'I',
            Self::FloatingPoint => 'R',
            Self::Any => '?',
        }
    }
}
