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

use common_exception::ErrorCode;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;

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
    pub fn add_error(&mut self, e: ErrorCode, line: usize) {
        match &mut self.error {
            None => {
                self.error = Some(FileErrorsInfo {
                    num_errors: 1,
                    first_error: FileErrorInfo {
                        code: e.code(),
                        message: e.message(),
                        line,
                    },
                });
            }
            Some(info) => {
                info.num_errors += 1;
                if info.first_error.line > line {
                    info.first_error = FileErrorInfo {
                        code: e.code(),
                        message: e.message(),
                        line,
                    };
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

#[derive(Default, Clone, Serialize, Deserialize)]
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

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct FileErrorInfo {
    pub code: u16,
    pub message: String,
    pub line: usize,
}
