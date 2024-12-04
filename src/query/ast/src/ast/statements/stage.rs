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

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_comma_separated_string_list;
use crate::ast::write_comma_separated_string_map;
use crate::ast::CreateOption;
use crate::ast::FileFormatOptions;
use crate::ast::LiteralStringOrVariable;
use crate::ast::UriLocation;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CreateStageStmt {
    pub create_option: CreateOption,
    pub stage_name: String,

    pub location: Option<UriLocation>,

    pub file_format_options: FileFormatOptions,
    pub comments: String,
}

impl Display for CreateStageStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, " OR REPLACE")?;
        }
        write!(f, " STAGE")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, " IF NOT EXISTS")?;
        }
        write!(f, " {}", self.stage_name)?;

        if let Some(ul) = &self.location {
            write!(f, " {ul}")?;
        }

        if !self.file_format_options.is_empty() {
            write!(f, " FILE_FORMAT = ({})", self.file_format_options)?;
        }

        if !self.comments.is_empty() {
            write!(f, " COMMENTS = '{}'", self.comments)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum SelectStageOption {
    Files(Vec<String>),
    Pattern(LiteralStringOrVariable),
    FileFormat(String),
    Connection(BTreeMap<String, String>),
    CaseSensitive(bool),
}

impl SelectStageOptions {
    pub fn from(opts: Vec<SelectStageOption>) -> Self {
        let mut options: SelectStageOptions = Default::default();
        for opt in opts.into_iter() {
            match opt {
                SelectStageOption::Files(v) => options.files = Some(v),
                SelectStageOption::Pattern(v) => options.pattern = Some(v),
                SelectStageOption::FileFormat(v) => options.file_format = Some(v),
                SelectStageOption::Connection(v) => options.connection = v,
                SelectStageOption::CaseSensitive(v) => options.case_sensitive = Some(v),
            }
        }
        options
    }
}

#[derive(Debug, Clone, PartialEq, Default, Drive, DriveMut)]
pub struct SelectStageOptions {
    pub files: Option<Vec<String>>,
    pub pattern: Option<LiteralStringOrVariable>,
    pub file_format: Option<String>,
    pub connection: BTreeMap<String, String>,
    pub case_sensitive: Option<bool>,
}

impl SelectStageOptions {
    pub fn is_empty(&self) -> bool {
        self.files.is_none()
            && self.pattern.is_none()
            && self.file_format.is_none()
            && self.connection.is_empty()
            && self.case_sensitive.is_none()
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, " (")?;

        if let Some(files) = self.files.as_ref() {
            write!(f, " FILES => (")?;
            write_comma_separated_string_list(f, files)?;
            write!(f, "),")?;
        }

        if let Some(file_format) = self.file_format.as_ref() {
            write!(f, " FILE_FORMAT => '{}',", file_format)?;
        }

        if let Some(pattern) = self.pattern.as_ref() {
            write!(f, " PATTERN => {},", pattern)?;
        }

        if let Some(case_sensitive) = self.case_sensitive {
            write!(f, " CASE_SENSITIVE => {},", case_sensitive)?;
        }

        if !self.connection.is_empty() {
            write!(f, " CONNECTION => (")?;
            write_comma_separated_string_map(f, &self.connection)?;
            write!(f, " )")?;
        }

        write!(f, " )")?;

        Ok(())
    }
}
