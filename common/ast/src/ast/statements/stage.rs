// Copyright 2022 Datafuse Labs.
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
use std::fmt::Display;
use std::fmt::Formatter;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateStageStmt {
    pub if_not_exists: bool,
    pub stage_name: String,

    pub location: String,
    pub credential_options: BTreeMap<String, String>,
    pub encryption_options: BTreeMap<String, String>,

    pub file_format_options: BTreeMap<String, String>,
    pub on_error: String,
    pub size_limit: usize,
    pub validation_mode: String,
    pub comments: String,
}

impl Display for CreateStageStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE STAGE")?;
        if self.if_not_exists {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {}", self.stage_name)?;

        if !self.location.is_empty() {
            write!(f, " URL = '{}'", self.location)?;

            if !self.credential_options.is_empty() {
                write!(f, " CREDENTIALS = (")?;
                for (k, v) in self.credential_options.iter() {
                    write!(f, " {} = '{}'", k, v)?;
                }
                write!(f, " )")?;
            }

            if !self.encryption_options.is_empty() {
                write!(f, " ENCRYPTION = (")?;
                for (k, v) in self.encryption_options.iter() {
                    write!(f, " {} = '{}'", k, v)?;
                }
                write!(f, " )")?;
            }
        }

        if !self.file_format_options.is_empty() {
            write!(f, " FILE_FORMAT = (")?;
            for (k, v) in self.file_format_options.iter() {
                write!(f, " {} = '{}'", k, v)?;
            }
            write!(f, " )")?;
        }

        if !self.on_error.is_empty() {
            write!(f, " ON_ERROR = {}", self.on_error)?;
        }

        if self.size_limit != 0 {
            write!(f, " SIZE_LIMIT = {}", self.size_limit)?;
        }

        if !self.validation_mode.is_empty() {
            write!(f, " VALIDATION_MODE = {}", self.validation_mode)?;
        }

        if !self.comments.is_empty() {
            write!(f, " COMMENTS = '{}'", self.comments)?;
        }

        Ok(())
    }
}
