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
use std::path::Path;
use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use walkdir::DirEntry;
use walkdir::WalkDir;

use crate::arg::SqlLogicTestArgs;
use crate::error::DSqlLogicTestError;
use crate::error::Result;
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ServerInfo {
    pub id: String,
    pub start_time: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HttpSessionConf {
    pub database: Option<String>,
    pub role: Option<String>,
    pub secondary_roles: Option<Vec<String>>,
    pub settings: Option<BTreeMap<String, String>>,
    pub txn_state: Option<String>,
    pub last_server_info: Option<ServerInfo>,
    #[serde(default)]
    pub last_query_ids: Vec<String>,
}

pub fn parser_rows(rows: &Value) -> Result<Vec<Vec<String>>> {
    let mut parsed_rows = Vec::new();
    for row in rows.as_array().unwrap() {
        let mut parsed_row = Vec::new();
        for col in row.as_array().unwrap() {
            let mut cell = col.as_str().unwrap();
            if cell == "inf" {
                cell = "Infinity";
            }
            if cell == "nan" {
                cell = "NaN";
            }
            // If the result is empty, we'll use `(empty)` to mark it explicitly to avoid confusion
            if cell.is_empty() {
                parsed_row.push("(empty)".to_string());
            } else {
                parsed_row.push(cell.to_string());
            }
        }
        parsed_rows.push(parsed_row);
    }
    Ok(parsed_rows)
}

fn find_specific_dir(dir: &str, suit: PathBuf) -> Result<DirEntry> {
    for entry in WalkDir::new(suit)
        .min_depth(0)
        .max_depth(100)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter()
    {
        let e = entry.as_ref().unwrap();
        if e.file_type().is_dir() && e.file_name().to_str().unwrap() == dir {
            return Ok(entry?);
        }
    }
    Err(DSqlLogicTestError::SelfError(
        "Didn't find specific dir".to_string(),
    ))
}

pub fn get_files(suit: PathBuf) -> Result<Vec<walkdir::Result<DirEntry>>> {
    let args = SqlLogicTestArgs::parse();

    let target = if let Some(ref dir) = args.dir {
        match find_specific_dir(dir, suit.clone()) {
            Ok(dir_entry) => dir_entry.into_path(),
            Err(_) => {
                log::warn!("Specific directory not found: {:?}", dir);
                return Ok(vec![]);
            }
        }
    } else {
        suit
    };

    let skipped_dirs: Vec<&str> = args
        .skipped_dir
        .as_ref()
        .map(|dirs| dirs.split(',').collect())
        .unwrap_or_default();
    let run_files: Vec<&str> = args
        .file
        .as_ref()
        .map(|files| files.split(',').collect())
        .unwrap_or_default();

    log::info!(
        "get_files, skipped_dirs: {:?}, run_files: {:?}",
        skipped_dirs,
        run_files
    );

    let files = WalkDir::new(target)
        .min_depth(0)
        .max_depth(100)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter()
        .filter_entry(|e| {
            let should_skip = skipped_dirs.contains(&e.file_name().to_str().unwrap());
            log::info!(
                "get_files, skipped_dirs filter: {:?}, should_skip: {}",
                e.path(),
                should_skip
            );
            !should_skip
        })
        .filter(|e| {
            if let Ok(entry) = e {
                if entry.file_type().is_file() {
                    if !run_files.is_empty() {
                        let file_name = entry.file_name().to_str().unwrap();
                        let should_include = run_files.iter().any(|&file| file == file_name);
                        log::info!(
                            "get_files, run_files filter: {:?}, should_include: {}",
                            entry.path(),
                            should_include
                        );
                        should_include
                    } else {
                        true
                    }
                } else {
                    false
                }
            } else {
                false
            }
        })
        .collect::<Vec<_>>();

    log::info!("files: {:?}", files);

    Ok(files)
}

static PREPARE_TPCH: std::sync::Once = std::sync::Once::new();
static PREPARE_TPCDS: std::sync::Once = std::sync::Once::new();
static PREPARE_STAGE: std::sync::Once = std::sync::Once::new();
static PREPARE_SPILL: std::sync::Once = std::sync::Once::new();
static PREPARE_WASM: std::sync::Once = std::sync::Once::new();

pub fn lazy_prepare_data(file_path: &Path) -> Result<()> {
    let file_path = file_path.to_str().unwrap_or_default();
    if file_path.contains("tpch/") {
        PREPARE_TPCH.call_once(|| {
            println!("Calling the script prepare_tpch_data.sh ...");
            run_script("prepare_tpch_data.sh").unwrap();
        });
    } else if file_path.contains("tpcds/") {
        PREPARE_TPCDS.call_once(|| {
            println!("Calling the script prepare_tpcds_data.sh ...");
            run_script("prepare_tpcds_data.sh").unwrap();
        });
    } else if file_path.contains("stage/") || file_path.contains("stage_parquet/") {
        PREPARE_STAGE.call_once(|| {
            println!("Calling the script prepare_stage.sh ...");
            run_script("prepare_stage.sh").unwrap();
        });
    } else if file_path.contains("udf_native/") {
        println!("wasm context Calling the script prepare_stage.sh ...");
        PREPARE_WASM.call_once(|| run_script("prepare_stage.sh").unwrap())
    } else if file_path.contains("spill/") {
        println!("Calling the script prepare_spill_data.sh ...");
        PREPARE_SPILL.call_once(|| run_script("prepare_spill_data.sh").unwrap())
    }
    Ok(())
}

fn run_script(name: &str) -> Result<()> {
    let path = format!("tests/sqllogictests/scripts/{}", name);
    let output = std::process::Command::new("bash")
        .arg(path)
        .output()
        .expect("failed to execute process");
    if !output.status.success() {
        return Err(DSqlLogicTestError::SelfError(format!(
            "Failed to run {}: {}",
            name,
            String::from_utf8(output.stderr).unwrap()
        )));
    }
    Ok(())
}
