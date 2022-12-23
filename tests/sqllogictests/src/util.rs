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

use std::collections::BTreeMap;
use std::path::PathBuf;

use lazy_static::lazy_static;
use regex::Regex;
use regex::RegexBuilder;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use walkdir::DirEntry;
use walkdir::WalkDir;

use crate::error::DSqlLogicTestError;
use crate::error::Result;

lazy_static! {
    pub static ref SET_SQL_RE: Regex =
        RegexBuilder::new(r"^SET\s+(?P<key>\w+)\s*=\s*[']?(?P<value>[^;[']]+)[']?\s*;?")
            .case_insensitive(true)
            .build()
            .unwrap();
    pub static ref UNSET_SQL_RE: Regex = RegexBuilder::new(r"^UNSET\s+(?P<key>\w+)\s*;?")
        .case_insensitive(true)
        .build()
        .unwrap();
    pub static ref USE_SQL_RE: Regex = RegexBuilder::new(r"^use\s+(?P<db>\w+)\s*;?")
        .case_insensitive(true)
        .build()
        .unwrap();
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HttpSessionConf {
    pub database: Option<String>,
    pub keep_server_session_secs: Option<u64>,
    pub settings: Option<BTreeMap<String, String>>,
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

pub fn find_specific_dir(dir: &str, suit: PathBuf) -> Result<DirEntry> {
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
