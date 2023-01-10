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
use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;

use crate::ast::write_quoted_comma_separated_list;
use crate::ast::write_space_seperated_map;
use crate::ast::Identifier;
use crate::ast::Query;

/// CopyStmt is the parsed statement of `COPY`.
///
/// ## Examples
///
/// ```sql
/// COPY INTO table from s3://bucket/path/to/x.csv
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct CopyStmt<'a> {
    pub src: CopyUnit<'a>,
    pub dst: CopyUnit<'a>,
    pub files: Vec<String>,
    pub pattern: String,
    pub file_format: BTreeMap<String, String>,
    /// TODO(xuanwo): parse into validation_mode directly.
    pub validation_mode: String,
    pub size_limit: usize,
    pub max_file_size: usize,
    pub split_size: usize,
    pub single: bool,
    pub purge: bool,
    pub force: bool,
    pub on_error: String,
}

impl<'a> CopyStmt<'a> {
    pub fn apply_option(&mut self, opt: CopyOption) {
        match opt {
            CopyOption::Files(v) => self.files = v,
            CopyOption::Pattern(v) => self.pattern = v,
            CopyOption::FileFormat(v) => self.file_format = v,
            CopyOption::ValidationMode(v) => self.validation_mode = v,
            CopyOption::SizeLimit(v) => self.size_limit = v,
            CopyOption::MaxFileSize(v) => self.max_file_size = v,
            CopyOption::SplitSize(v) => self.split_size = v,
            CopyOption::Single(v) => self.single = v,
            CopyOption::Purge(v) => self.purge = v,
            CopyOption::Force(v) => self.force = v,
            CopyOption::OnError(v) => self.on_error = v,
        }
    }
}

impl Display for CopyStmt<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "COPY")?;
        write!(f, " INTO {}", self.dst)?;
        write!(f, " FROM {}", self.src)?;

        if !self.files.is_empty() {
            write!(f, " FILES = (")?;
            write_quoted_comma_separated_list(f, &self.files)?;
            write!(f, " )")?;
        }

        if !self.pattern.is_empty() {
            write!(f, " PATTERN = '{}'", self.pattern)?;
        }

        if !self.file_format.is_empty() {
            write!(f, " FILE_FORMAT = (")?;
            for (k, v) in self.file_format.iter() {
                write!(f, " {} = '{}'", k, v)?;
            }
            write!(f, " )")?;
        }

        if !self.validation_mode.is_empty() {
            write!(f, "VALIDATION_MODE = {}", self.validation_mode)?;
        }

        if self.size_limit != 0 {
            write!(f, " SIZE_LIMIT = {}", self.size_limit)?;
        }

        if self.max_file_size != 0 {
            write!(f, " MAX_FILE_SIZE = {}", self.max_file_size)?;
        }

        if self.split_size != 0 {
            write!(f, " SPLIT_SIZE = {}", self.split_size)?;
        }

        write!(f, " SINGLE = {}", self.single)?;
        write!(f, " PURGE = {}", self.purge)?;
        write!(f, " FORCE = {}", self.force)?;
        write!(f, " ON_ERROR = {}", self.on_error)?;
        Ok(())
    }
}

/// CopyUnit is the unit that can be used in `COPY`.
#[derive(Debug, Clone, PartialEq)]
pub enum CopyUnit<'a> {
    /// Table can be used in `INTO` or `FROM`.
    ///
    /// While table used as `FROM`, it will be rewrite as `(SELECT * FROM table)`
    Table {
        catalog: Option<Identifier<'a>>,
        database: Option<Identifier<'a>>,
        table: Identifier<'a>,
    },
    /// StageLocation (a.k.a internal and external stage) can be used
    /// in `INTO` or `FROM`.
    ///
    /// For examples:
    ///
    /// - internal stage: `@internal_stage/path/to/dir/`
    /// - external stage: `@s3_external_stage/path/to/dir/`
    StageLocation(StageLocation),
    /// UriLocation (a.k.a external location) can be used in `INTO` or `FROM`.
    ///
    /// For examples: `'s3://example/path/to/dir' CONNECTION = (AWS_ACCESS_ID="admin" AWS_SECRET_KEY="admin")`
    UriLocation(UriLocation),
    /// Query can only be used as `FROM`.
    ///
    /// For example:`(SELECT field_a,field_b FROM table)`
    Query(Box<Query<'a>>),
}

impl CopyUnit<'_> {
    pub fn target(&self) -> &'static str {
        match self {
            CopyUnit::Table { .. } => "Table",
            CopyUnit::StageLocation { .. } => "StageLocation",
            CopyUnit::UriLocation { .. } => "UriLocation",
            CopyUnit::Query(_) => "Query",
        }
    }
}

impl Display for CopyUnit<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyUnit::Table {
                catalog,
                database,
                table,
            } => {
                if let Some(catalog) = catalog {
                    write!(
                        f,
                        "{catalog}.{}.{table}",
                        database.as_ref().expect("database must be valid")
                    )
                } else if let Some(database) = database {
                    write!(f, "{database}.{table}")
                } else {
                    write!(f, "{table}")
                }
            }
            CopyUnit::StageLocation(v) => v.fmt(f),
            CopyUnit::UriLocation(v) => v.fmt(f),
            CopyUnit::Query(query) => {
                write!(f, "({query})")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    visited_keys: HashSet<String>,
    conns: BTreeMap<String, String>,
}

impl Connection {
    fn new(conns: BTreeMap<String, String>) -> Self {
        Self {
            visited_keys: HashSet::new(),
            conns,
        }
    }

    pub fn get(&mut self, key: &str) -> Option<&String> {
        self.visited_keys.insert(key.to_string());
        self.conns.get(key)
    }

    pub fn check(&self) -> Result<()> {
        let conn_keys = HashSet::from_iter(self.conns.keys().cloned());
        let diffs: Vec<String> = conn_keys
            .difference(&self.visited_keys)
            .map(|x| x.to_string())
            .collect();

        if !diffs.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Unknown params [{}], please check the documents for supported params",
                    diffs.join(","),
                ),
            ));
        }
        Ok(())
    }
}

impl Display for Connection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if !self.conns.is_empty() {
            write!(f, " CONNECTION = ( ")?;
            write_space_seperated_map(f, &self.conns)?;
            write!(f, " )")?;
        }
        Ok(())
    }
}

/// UriLocation (a.k.a external location) can be used in `INTO` or `FROM`.
///
/// For examples: `'s3://example/path/to/dir' CONNECTION = (AWS_ACCESS_ID="admin" AWS_SECRET_KEY="admin")`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UriLocation {
    pub protocol: String,
    pub name: String,
    pub path: String,
    pub part_prefix: String,
    pub connection: Connection,
}

impl UriLocation {
    pub fn new(
        protocol: String,
        name: String,
        path: String,
        part_prefix: String,
        conns: BTreeMap<String, String>,
    ) -> Self {
        Self {
            protocol,
            name,
            path,
            part_prefix,
            connection: Connection::new(conns),
        }
    }
}

impl Display for UriLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}://{}{}'", self.protocol, self.name, self.path)?;
        if !self.part_prefix.is_empty() {
            write!(f, " LOCATION_PREFIX = '{}'", self.part_prefix)?;
        }
        write!(f, "{}", self.connection)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageLocation {
    pub name: String,
    pub path: String,
}

impl Display for StageLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "@{}{}", self.name, self.path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileLocation {
    Stage(StageLocation),
    Uri(UriLocation),
}

impl Display for FileLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileLocation::Uri(loc) => {
                write!(f, "{}", loc)
            }
            FileLocation::Stage(loc) => {
                write!(f, "{}", loc)
            }
        }
    }
}

pub enum CopyOption {
    Files(Vec<String>),
    Pattern(String),
    FileFormat(BTreeMap<String, String>),
    ValidationMode(String),
    SizeLimit(usize),
    MaxFileSize(usize),
    SplitSize(usize),
    Single(bool),
    Purge(bool),
    Force(bool),
    OnError(String),
}
