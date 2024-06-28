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

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::OnErrorMode;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct CopyStatus {
    /// Key is file path.
    pub files: DashMap<String, FileStatus>,
}

impl CopyStatus {
    pub fn add_chunk(&self, file_path: &str, file_status: FileStatus) {
        match self.files.entry(file_path.to_string()) {
            Entry::Occupied(mut e) => {
                e.get_mut().merge(file_status);
            }
            Entry::Vacant(e) => {
                e.insert(file_status);
            }
        };
    }

    pub fn merge(&self, other: CopyStatus) {
        for (k, v) in other.files.into_iter() {
            self.add_chunk(&k, v);
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct FileStatus {
    pub num_rows_loaded: usize,
    pub error: Option<FileErrorsInfo>,
}

impl FileStatus {
    pub fn add_error(&mut self, error: FileParseError, line: usize) {
        match &mut self.error {
            None => {
                self.error = Some(FileErrorsInfo {
                    num_errors: 1,
                    first_error: FileErrorInfo { error, line },
                });
            }
            Some(info) => {
                info.num_errors += 1;
                if info.first_error.line > line {
                    info.first_error = FileErrorInfo { error, line };
                }
            }
        };
    }

    fn merge(&mut self, other: FileStatus) {
        self.num_rows_loaded += other.num_rows_loaded;
        match (&mut self.error, other.error) {
            (None, Some(e)) => self.error = Some(e),
            (Some(e1), Some(e2)) => e1.merge(e2),
            _ => {}
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FileErrorsInfo {
    pub num_errors: usize,
    pub first_error: FileErrorInfo,
}

impl FileErrorsInfo {
    fn merge(&mut self, other: FileErrorsInfo) {
        self.num_errors += other.num_errors;
        if self.first_error.line > other.first_error.line {
            self.first_error = other.first_error;
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FileErrorInfo {
    pub error: FileParseError,
    pub line: usize,
}

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum FileParseError {
    #[error(
        "Number of columns in file ({file}) does not match that of the corresponding table ({table})"
    )]
    NumberOfColumnsMismatch { table: usize, file: usize },
    #[error("Invalid JSON row: {message}")]
    InvalidNDJsonRow { message: String },
    #[error(
        "Invalid value '{column_data}' for column {column_index} ({column_name} {column_type}): {decode_error}"
    )]
    ColumnDecodeError {
        column_index: usize,
        column_name: String,
        column_type: String,
        decode_error: String,
        column_data: String,
    },
    #[error("Missing value for column {column_index} ({column_name} {column_type})")]
    ColumnMissingError {
        column_index: usize,
        column_name: String,
        column_type: String,
    },
    #[error(
        "Encountered an empty value for column {column_index} (`{column_name}` of type {column_type}), with the FILE_FORMAT option `EMPTY_FIELD_AS={empty_field_as}`. To resolve this, please consider {remedy}"
    )]
    ColumnEmptyError {
        column_index: usize,
        column_name: String,
        column_type: String,
        empty_field_as: String,
        remedy: String,
    },
    #[error(
        "Invalid value '{column_data}' for column {column_index} ({column_name} {column_type}): {size_remained} bytes remained, next_char at {error_pos} is {next_char}"
    )]
    ColumnDataNotDrained {
        column_index: u32,
        error_pos: u32,
        size_remained: u32,
        column_name: String,
        column_type: String,
        next_char: String,
        column_data: String,
    },
    #[error("Unexpected: {message}")]
    Unexpected { message: String },
}

impl FileParseError {
    pub fn to_error_code(&self, mode: &OnErrorMode, file_path: &str, line: usize) -> ErrorCode {
        let pos: String = format!("at file '{}', line {}", file_path, line);
        let message = match mode {
            OnErrorMode::AbortNum(n) if *n > 1u64 => {
                format!("abort after {n} errors! the last error: {self}",)
            }
            _ => format!("{self}"),
        };
        ErrorCode::BadBytes(message).add_detail_back(pos)
    }
}
