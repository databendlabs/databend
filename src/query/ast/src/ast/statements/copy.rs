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
use std::collections::HashSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;

use databend_common_base::base::mask_string;
use databend_common_exception::ErrorCode;
use databend_common_io::escape_string_with_quote;
use derive_visitor::Drive;
use derive_visitor::DriveMut;
use itertools::Itertools;
use url::Url;

use crate::ast::write_comma_separated_map;
use crate::ast::write_comma_separated_string_list;
use crate::ast::write_comma_separated_string_map;
use crate::ast::Hint;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::TableRef;
use crate::ast::With;

/// CopyIntoTableStmt is the parsed statement of `COPY into <table> from <location>`.
///
/// ## Examples
///
/// ```sql
/// COPY INTO table from s3://bucket/path/to/x.csv
/// ```
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CopyIntoTableStmt {
    pub with: Option<With>,
    pub src: CopyIntoTableSource,
    pub dst: TableRef,
    pub dst_columns: Option<Vec<Identifier>>,

    pub hints: Option<Hint>,

    pub file_format: FileFormatOptions,

    // files to load
    #[drive(skip)]
    pub files: Option<Vec<String>>,
    #[drive(skip)]
    pub pattern: Option<String>,
    #[drive(skip)]
    pub force: bool,

    // copy options
    /// TODO(xuanwo): parse into validation_mode directly.
    #[drive(skip)]
    pub validation_mode: String,
    #[drive(skip)]
    pub size_limit: usize,
    #[drive(skip)]
    pub max_files: usize,
    #[drive(skip)]
    pub split_size: usize,
    #[drive(skip)]
    pub purge: bool,
    #[drive(skip)]
    pub disable_variant_check: bool,
    #[drive(skip)]
    pub return_failed_only: bool,
    #[drive(skip)]
    pub on_error: String,
}

impl CopyIntoTableStmt {
    pub fn apply_option(&mut self, opt: CopyIntoTableOption) {
        match opt {
            CopyIntoTableOption::Files(v) => self.files = Some(v),
            CopyIntoTableOption::Pattern(v) => self.pattern = Some(v),
            CopyIntoTableOption::FileFormat(v) => self.file_format = v,
            CopyIntoTableOption::ValidationMode(v) => self.validation_mode = v,
            CopyIntoTableOption::SizeLimit(v) => self.size_limit = v,
            CopyIntoTableOption::MaxFiles(v) => self.max_files = v,
            CopyIntoTableOption::SplitSize(v) => self.split_size = v,
            CopyIntoTableOption::Purge(v) => self.purge = v,
            CopyIntoTableOption::Force(v) => self.force = v,
            CopyIntoTableOption::DisableVariantCheck(v) => self.disable_variant_check = v,
            CopyIntoTableOption::ReturnFailedOnly(v) => self.return_failed_only = v,
            CopyIntoTableOption::OnError(v) => self.on_error = v,
        }
    }
}

impl Display for CopyIntoTableStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(cte) = &self.with {
            write!(f, "WITH {} ", cte)?;
        }
        write!(f, "COPY")?;
        if let Some(hints) = &self.hints {
            write!(f, "{} ", hints)?;
        }
        write!(f, " INTO {}", self.dst)?;
        if let Some(columns) = &self.dst_columns {
            write!(f, "({})", columns.iter().map(|c| c.to_string()).join(","))?;
        }
        write!(f, " FROM {}", self.src)?;

        if let Some(files) = &self.files {
            write!(f, " FILES = (")?;
            write_comma_separated_string_list(f, files)?;
            write!(f, " )")?;
        }

        if let Some(pattern) = &self.pattern {
            write!(f, " PATTERN = '{}'", pattern)?;
        }

        if !self.file_format.is_empty() {
            write!(f, " FILE_FORMAT = ({})", self.file_format)?;
        }

        if !self.validation_mode.is_empty() {
            write!(f, "VALIDATION_MODE = {}", self.validation_mode)?;
        }

        if self.size_limit != 0 {
            write!(f, " SIZE_LIMIT = {}", self.size_limit)?;
        }

        if self.max_files != 0 {
            write!(f, " MAX_FILES = {}", self.max_files)?;
        }

        if self.split_size != 0 {
            write!(f, " SPLIT_SIZE = {}", self.split_size)?;
        }

        write!(f, " PURGE = {}", self.purge)?;
        write!(f, " FORCE = {}", self.force)?;
        write!(f, " DISABLE_VARIANT_CHECK = {}", self.disable_variant_check)?;
        write!(f, " ON_ERROR = {}", self.on_error)?;

        Ok(())
    }
}

/// CopyIntoLocationStmt is the parsed statement of `COPY into <location>  from <table> ...`
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CopyIntoLocationStmt {
    pub with: Option<With>,
    pub hints: Option<Hint>,
    pub src: CopyIntoLocationSource,
    pub dst: FileLocation,
    #[drive(skip)]
    pub file_format: FileFormatOptions,
    #[drive(skip)]
    pub single: bool,
    #[drive(skip)]
    pub max_file_size: usize,
    #[drive(skip)]
    pub detailed_output: bool,
}

impl Display for CopyIntoLocationStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(cte) = &self.with {
            write!(f, "WITH {} ", cte)?;
        }
        write!(f, "COPY")?;
        if let Some(hints) = &self.hints {
            write!(f, "{} ", hints)?;
        }
        write!(f, " INTO {}", self.dst)?;
        write!(f, " FROM {}", self.src)?;

        if !self.file_format.is_empty() {
            write!(f, " FILE_FORMAT = ({})", self.file_format)?;
        }
        write!(f, " SINGLE = {}", self.single)?;
        write!(f, " MAX_FILE_SIZE = {}", self.max_file_size)?;
        write!(f, " DETAILED_OUTPUT = {}", self.detailed_output)?;

        Ok(())
    }
}

impl CopyIntoLocationStmt {
    pub fn apply_option(&mut self, opt: CopyIntoLocationOption) {
        match opt {
            CopyIntoLocationOption::FileFormat(v) => self.file_format = v,
            CopyIntoLocationOption::Single(v) => self.single = v,
            CopyIntoLocationOption::MaxFileSize(v) => self.max_file_size = v,
            CopyIntoLocationOption::DetailedOutput(v) => self.detailed_output = v,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum CopyIntoTableSource {
    Location(FileLocation),
    /// Load with Transform
    /// limited to `(SELECT ... FROM <location>)`
    Query(Box<Query>),
}

impl Display for CopyIntoTableSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyIntoTableSource::Location(location) => write!(f, "{location}"),
            CopyIntoTableSource::Query(query) => {
                write!(f, "({query})")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum CopyIntoLocationSource {
    Query(Box<Query>),
    /// it will be rewrite as `(SELECT * FROM table)`
    Table(TableRef),
}

impl Display for CopyIntoLocationSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CopyIntoLocationSource::Query(query) => {
                write!(f, "({query})")
            }
            CopyIntoLocationSource::Table(table) => {
                write!(f, "{}", table)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct Connection {
    #[drive(skip)]
    visited_keys: HashSet<String>,
    #[drive(skip)]
    pub conns: BTreeMap<String, String>,
}

impl Connection {
    pub fn new(conns: BTreeMap<String, String>) -> Self {
        Self {
            visited_keys: HashSet::new(),
            conns,
        }
    }

    pub fn mask(&self) -> Self {
        let mut conns = BTreeMap::new();
        for (k, v) in &self.conns {
            conns.insert(k.to_string(), mask_string(v, 3));
        }
        Self {
            visited_keys: self.visited_keys.clone(),
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
                    "connection params invalid: expected [{}], got [{}]",
                    self.visited_keys
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(","),
                    diffs.join(",")
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
            write_comma_separated_string_map(f, &self.conns)?;
            write!(f, " )")?;
        }
        Ok(())
    }
}

/// UriLocation (a.k.a external location) can be used in `INTO` or `FROM`.
///
/// For examples: `'s3://example/path/to/dir' CONNECTION = (AWS_ACCESS_ID="admin" AWS_SECRET_KEY="admin")`
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct UriLocation {
    #[drive(skip)]
    pub protocol: String,
    #[drive(skip)]
    pub name: String,
    #[drive(skip)]
    pub path: String,
    #[drive(skip)]
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

    pub fn from_uri(
        uri: String,
        part_prefix: String,
        conns: BTreeMap<String, String>,
    ) -> databend_common_exception::Result<Self> {
        // fs location is not a valid url, let's check it in advance.
        if let Some(path) = uri.strip_prefix("fs://") {
            if !path.starts_with('/') {
                return Err(ErrorCode::BadArguments(format!(
                    "Invalid uri: {}. fs location must start with 'fs:///'",
                    uri
                )));
            }
            return Ok(UriLocation::new(
                "fs".to_string(),
                "".to_string(),
                path.to_string(),
                part_prefix,
                BTreeMap::default(),
            ));
        }

        let parsed = Url::parse(&uri).map_err(|e| {
            databend_common_exception::ErrorCode::BadArguments(format!("invalid uri {}", e))
        })?;

        let protocol = parsed.scheme().to_string();

        let name = parsed
            .host_str()
            .map(|hostname| {
                if let Some(port) = parsed.port() {
                    format!("{}:{}", hostname, port)
                } else {
                    hostname.to_string()
                }
            })
            .ok_or_else(|| databend_common_exception::ErrorCode::BadArguments("invalid uri"))?;

        let path = if parsed.path().is_empty() {
            "/".to_string()
        } else {
            parsed.path().to_string()
        };

        Ok(Self {
            protocol,
            name,
            path,
            part_prefix,
            connection: Connection::new(conns),
        })
    }
}

impl Display for UriLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}://{}{}'", self.protocol, self.name, self.path)?;
        if f.alternate() {
            write!(f, "{}", self.connection.mask())?;
        } else {
            write!(f, "{}", self.connection)?;
        }
        if !self.part_prefix.is_empty() {
            write!(f, " LOCATION_PREFIX = '{}'", self.part_prefix)?;
        }
        Ok(())
    }
}

/// StageLocation (a.k.a internal and external stage) can be used
/// in `INTO` or `FROM`.
///
/// For examples:
///
/// - internal stage: `@internal_stage/path/to/dir/`
/// - external stage: `@s3_external_stage/path/to/dir/`
/// UriLocation (a.k.a external location) can be used in `INTO` or `FROM`.
///
/// For examples: `'s3://example/path/to/dir' CONNECTION = (AWS_ACCESS_ID="admin" AWS_SECRET_KEY="admin")`
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum FileLocation {
    Stage(#[drive(skip)] String),
    Uri(UriLocation),
}

impl Display for FileLocation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileLocation::Uri(loc) => {
                write!(f, "{}", loc)
            }
            FileLocation::Stage(loc) => {
                write!(f, "'@{}'", loc)
            }
        }
    }
}

pub enum CopyIntoTableOption {
    Files(Vec<String>),
    Pattern(String),
    FileFormat(FileFormatOptions),
    ValidationMode(String),
    SizeLimit(usize),
    MaxFiles(usize),
    SplitSize(usize),
    Purge(bool),
    Force(bool),
    DisableVariantCheck(bool),
    ReturnFailedOnly(bool),
    OnError(String),
}

pub enum CopyIntoLocationOption {
    FileFormat(FileFormatOptions),
    MaxFileSize(usize),
    Single(bool),
    DetailedOutput(bool),
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Drive, DriveMut)]
pub struct FileFormatOptions {
    #[drive(skip)]
    pub options: BTreeMap<String, FileFormatValue>,
}

impl FileFormatOptions {
    pub fn is_empty(&self) -> bool {
        self.options.is_empty()
    }
}

impl Display for FileFormatOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write_comma_separated_map(f, &self.options)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileFormatValue {
    Keyword(String),
    Bool(bool),
    U64(u64),
    String(String),
    StringList(Vec<String>),
}

impl FileFormatValue {
    pub fn to_meta_value(&self) -> String {
        match self {
            FileFormatValue::Keyword(v) => v.clone(),
            FileFormatValue::Bool(v) => v.to_string(),
            FileFormatValue::U64(v) => v.to_string(),
            FileFormatValue::String(v) => v.clone(),
            FileFormatValue::StringList(v) => serde_json::to_string(v).unwrap(),
        }
    }
}

impl Display for FileFormatValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileFormatValue::Keyword(v) => write!(f, "{v}"),
            FileFormatValue::Bool(v) => write!(f, "{v}"),
            FileFormatValue::U64(v) => write!(f, "{v}"),
            FileFormatValue::String(v) => {
                write!(f, "'{}'", escape_string_with_quote(v, Some('\'')))
            }
            FileFormatValue::StringList(v) => {
                write!(f, "(")?;
                for (i, s) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "'{}'", escape_string_with_quote(s, Some('\'')))?;
                }
                write!(f, ")")
            }
        }
    }
}
