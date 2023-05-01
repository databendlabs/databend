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
use std::default::Default;
use std::fmt::Display;
use std::fmt::Formatter;

use crate::ast::UriLocation;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStageStmt {
    pub if_not_exists: bool,
    pub stage_name: String,

    pub location: Option<UriLocation>,

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

        if let Some(ul) = &self.location {
            write!(f, " URL = ")?;
            write!(f, "{ul}")?;
        }

        if !self.file_format_options.is_empty() {
            write!(f, " FILE_FORMAT = (")?;
            for (k, v) in self.file_format_options.iter() {
                write!(f, " {} = '{}'", k, v)?;
            }
            write!(f, " )")?;
        }

        if !self.on_error.is_empty() {
            write!(f, " ON_ERROR = '{}'", self.on_error)?;
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

#[derive(Debug, Clone, PartialEq)]
pub enum SelectStageOption {
    Files(Vec<String>),
    Pattern(String),
    FileFormat(String),
    Connection((String, String)),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SelectStageOptions {
    pub files: Option<Vec<String>>,
    pub pattern: Option<String>,
    pub file_format: Option<String>,
    pub connection: BTreeMap<String, String>,
}

impl SelectStageOptions {
    pub fn is_empty(&self) -> bool {
        self.files.is_none()
            && self.pattern.is_none()
            && self.file_format.is_none()
            && self.connection.is_empty()
    }
}

// SELECT <columns> FROM
// {@<stage_name>[/<path>] | '<uri>'} [(
// [ PATTERN => '<regex_pattern>']
// [ FILE_FORMAT => '<format_name>']
// [ FILES => ( 'file_name' [ , 'file_name' ... ] ) ]
// [ ENDPOINT_URL => <'url'> ]
// [ AWS_KEY_ID => <'aws_key_id'> ]
// [ AWS_KEY_SECRET => <'aws_key_secret'> ]
// [ ACCESS_KEY_ID => <'access_key_id'> ]
// [ ACCESS_KEY_SECRET => <'access_key_secret'> ]
// [ SECRET_ACCESS_KEY => <'secret_access_key'> ]
// [ SESSION_TOKEN => <'session_token'> ]
// [ REGION => <'region'> ]
// [ ENABLE_VIRTUAL_HOST_STYLE => true|false ]
// )]
impl Display for SelectStageOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, " (")?;

        let mut output: Vec<String> = vec![];
        if let Some(files) = self.files.clone() {
            let files = files
                .iter()
                .map(|x| format!("'{}'", x))
                .collect::<Vec<String>>();
            let files = files.join(",");
            let files = format!("FILES => ({})", files);
            output.push(files);
        }

        if let Some(file_format) = self.file_format.clone() {
            let file_format = format!("FILE_FORMAT => '{}'", file_format);
            output.push(file_format);
        }

        if let Some(pattern) = self.pattern.clone() {
            let pattern = format!("PATTERN => '{}'", pattern);
            output.push(pattern);
        }

        if !self.connection.is_empty() {
            for (k, v) in self.connection.iter() {
                output.push(format!(" {} => '{}'", k, v));
            }
        }

        let output = output.join(",");
        write!(f, "{output})")?;
        Ok(())
    }
}

impl SelectStageOptions {
    pub fn from(opts: Vec<SelectStageOption>) -> Self {
        let mut options: SelectStageOptions = Default::default();
        for opt in opts.into_iter() {
            match opt {
                SelectStageOption::Files(v) => options.files = Some(v),
                SelectStageOption::Pattern(v) => options.pattern = Some(v),
                SelectStageOption::FileFormat(v) => options.file_format = Some(v),
                SelectStageOption::Connection((k, v)) => {
                    options.connection.insert(k, v);
                }
            }
        }
        options
    }
}
