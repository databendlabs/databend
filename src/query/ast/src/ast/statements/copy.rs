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
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

use derive_visitor::Drive;
use derive_visitor::DriveMut;
use itertools::Itertools;
use percent_encoding::percent_decode_str;
use url::Url;

use crate::ast::quote::QuotedString;
use crate::ast::write_comma_separated_map;
use crate::ast::write_comma_separated_string_list;
use crate::ast::write_comma_separated_string_map;
use crate::ast::Hint;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::ast::TableRef;
use crate::ast::With;
use crate::ParseError;
use crate::Result;

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
    pub files: Option<Vec<String>>,
    pub pattern: Option<LiteralStringOrVariable>,

    pub options: CopyIntoTableOptions,
}

impl CopyIntoTableStmt {
    pub fn apply_option(
        &mut self,
        opt: CopyIntoTableOption,
    ) -> std::result::Result<(), &'static str> {
        match opt {
            CopyIntoTableOption::Files(v) => self.files = Some(v),
            CopyIntoTableOption::Pattern(v) => self.pattern = Some(v),
            CopyIntoTableOption::FileFormat(v) => self.file_format = v,
            CopyIntoTableOption::SizeLimit(v) => self.options.size_limit = v,
            CopyIntoTableOption::MaxFiles(v) => self.options.max_files = v,
            CopyIntoTableOption::SplitSize(v) => self.options.split_size = v,
            CopyIntoTableOption::Purge(v) => self.options.purge = v,
            CopyIntoTableOption::Force(v) => self.options.force = v,
            CopyIntoTableOption::DisableVariantCheck(v) => self.options.disable_variant_check = v,
            CopyIntoTableOption::ReturnFailedOnly(v) => self.options.return_failed_only = v,
            CopyIntoTableOption::OnError(v) => self.options.on_error = OnErrorMode::from_str(&v)?,
            CopyIntoTableOption::ColumnMatchMode(v) => {
                self.options.column_match_mode = Some(ColumnMatchMode::from_str(&v)?)
            }
        }
        Ok(())
    }
}

impl Display for CopyIntoTableStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
            write!(f, " PATTERN = {}", pattern)?;
        }

        if !self.file_format.is_empty() {
            write!(f, " FILE_FORMAT = ({})", self.file_format)?;
        }
        write!(f, " {}", self.options)?;
        Ok(())
    }
}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, Default, PartialEq, Drive, DriveMut, Eq,
)]
pub struct CopyIntoTableOptions {
    pub on_error: OnErrorMode,
    pub size_limit: usize,
    pub max_files: usize,
    pub split_size: usize,
    pub force: bool,
    pub purge: bool,
    pub disable_variant_check: bool,
    pub return_failed_only: bool,
    pub validation_mode: String,
    pub column_match_mode: Option<ColumnMatchMode>,
}

impl CopyIntoTableOptions {
    fn parse_uint(k: &str, v: &String) -> std::result::Result<usize, String> {
        usize::from_str(v).map_err(|e| format!("can not parse {}={} as uint: {}", k, v, e))
    }
    fn parse_bool(k: &str, v: &String) -> std::result::Result<bool, String> {
        bool::from_str(v).map_err(|e| format!("can not parse {}={} as bool: {}", k, v, e))
    }

    pub fn set_column_match_mode(&mut self, mode: ColumnMatchMode) {
        self.column_match_mode = Some(mode);
    }

    pub fn apply(
        &mut self,
        opts: &BTreeMap<String, String>,
        ignore_unknown: bool,
    ) -> std::result::Result<(), String> {
        if opts.is_empty() {
            return Ok(());
        }
        for (k, v) in opts.iter() {
            match k.as_str() {
                "on_error" => {
                    let on_error = OnErrorMode::from_str(v)?;
                    self.on_error = on_error;
                }
                "column_match_mode" => {
                    let column_match_mode = ColumnMatchMode::from_str(v)?;
                    self.column_match_mode = Some(column_match_mode);
                }
                "size_limit" => {
                    self.size_limit = Self::parse_uint(k, v)?;
                }
                "max_files" => {
                    self.max_files = Self::parse_uint(k, v)?;
                }
                "split_size" => {
                    self.split_size = Self::parse_uint(k, v)?;
                }
                "purge" => {
                    self.purge = Self::parse_bool(k, v)?;
                }
                "disable_variant_check" => {
                    self.disable_variant_check = Self::parse_bool(k, v)?;
                }
                "return_failed_only" => {
                    self.return_failed_only = Self::parse_bool(k, v)?;
                }
                _ => {
                    if !ignore_unknown {
                        return Err(format!("Unknown stage copy option {}", k));
                    }
                }
            }
        }
        Ok(())
    }
}

impl Display for CopyIntoTableOptions {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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
        write!(f, " RETURN_FAILED_ONLY = {}", self.return_failed_only)?;
        if let Some(mode) = &self.column_match_mode {
            write!(f, " COLUMN_MATCH_MODE = {}", mode)?;
        }
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Drive, DriveMut, Eq)]
pub struct CopyIntoLocationOptions {
    pub single: bool,
    pub max_file_size: usize,
    pub detailed_output: bool,
    pub use_raw_path: bool,
    pub include_query_id: bool,
    pub overwrite: bool,
}

impl Default for CopyIntoLocationOptions {
    fn default() -> Self {
        Self {
            single: Default::default(),
            max_file_size: Default::default(),
            detailed_output: false,
            use_raw_path: false,
            include_query_id: true,
            overwrite: false,
        }
    }
}

/// CopyIntoLocationStmt is the parsed statement of `COPY into <location>  from <table> ...`
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CopyIntoLocationStmt {
    pub with: Option<With>,
    pub hints: Option<Hint>,
    pub src: CopyIntoLocationSource,
    pub dst: FileLocation,
    pub file_format: FileFormatOptions,
    pub options: CopyIntoLocationOptions,
}

impl Display for CopyIntoLocationStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
        write!(f, " SINGLE = {}", self.options.single)?;
        write!(f, " MAX_FILE_SIZE = {}", self.options.max_file_size)?;
        write!(f, " DETAILED_OUTPUT = {}", self.options.detailed_output)?;
        write!(f, " INCLUDE_QUERY_ID = {}", self.options.include_query_id)?;
        write!(f, " USE_RAW_PATH = {}", self.options.use_raw_path)?;
        write!(f, " OVERWRITE = {}", self.options.overwrite)?;

        Ok(())
    }
}

impl CopyIntoLocationStmt {
    pub fn apply_option(&mut self, opt: CopyIntoLocationOption) {
        match opt {
            CopyIntoLocationOption::FileFormat(v) => self.file_format = v,
            CopyIntoLocationOption::Single(v) => self.options.single = v,
            CopyIntoLocationOption::MaxFileSize(v) => self.options.max_file_size = v,
            CopyIntoLocationOption::DetailedOutput(v) => self.options.detailed_output = v,
            CopyIntoLocationOption::IncludeQueryID(v) => self.options.include_query_id = v,
            CopyIntoLocationOption::UseRawPath(v) => self.options.use_raw_path = v,
            CopyIntoLocationOption::OverWrite(v) => self.options.overwrite = v,
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
    /// it will be rewritten as `(SELECT * FROM table)`
    Table(TableRef),
}

impl Display for CopyIntoLocationSource {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
            return Err(ParseError(
                None,
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if !self.conns.is_empty() {
            write!(f, " CONNECTION = ( ")?;
            write_comma_separated_string_map(f, &self.conns)?;
            write!(f, " )")?;
        }
        Ok(())
    }
}

/// Mask a string by "******", but keep `unmask_len` of suffix.
fn mask_string(s: &str, unmask_len: usize) -> String {
    if s.len() <= unmask_len {
        s.to_string()
    } else {
        let mut ret = "******".to_string();
        ret.push_str(&s[(s.len() - unmask_len)..]);
        ret
    }
}

/// UriLocation (a.k.a external location) can be used in `INTO` or `FROM`.
///
/// For examples: `'s3://example/path/to/dir' CONNECTION = (AWS_ACCESS_ID="admin" AWS_SECRET_KEY="admin")`
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct UriLocation {
    pub protocol: String,
    pub name: String,
    pub path: String,
    pub connection: Connection,
}

impl UriLocation {
    pub fn new(
        protocol: String,
        name: String,
        path: String,
        conns: BTreeMap<String, String>,
    ) -> Self {
        Self {
            protocol,
            name,
            path,
            connection: Connection::new(conns),
        }
    }

    pub fn from_uri(uri: String, conns: BTreeMap<String, String>) -> Result<Self> {
        // fs location is not a valid url, let's check it in advance.
        if let Some(path) = uri.strip_prefix("fs://") {
            if !path.starts_with('/') {
                return Err(ParseError(
                    None,
                    format!("Invalid uri: {}. fs location must start with 'fs:///'", uri),
                ));
            }
            return Ok(UriLocation::new(
                "fs".to_string(),
                "".to_string(),
                path.to_string(),
                BTreeMap::default(),
            ));
        }

        let parsed =
            Url::parse(&uri).map_err(|e| ParseError(None, format!("invalid uri {}", e)))?;

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
            .ok_or_else(|| ParseError(None, "invalid uri".to_string()))?;

        let path = if parsed.path().is_empty() {
            "/".to_string()
        } else {
            percent_decode_str(parsed.path())
                .decode_utf8_lossy()
                .to_string()
        };

        Ok(Self {
            protocol,
            name,
            path,
            connection: Connection::new(conns),
        })
    }

    pub fn mask(&self) -> Self {
        Self {
            connection: self.connection.mask(),
            ..self.clone()
        }
    }
}

impl Display for UriLocation {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "'{}://{}{}'", self.protocol, self.name, self.path)?;
        write!(f, "{}", self.connection)?;
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
///
/// UriLocation (a.k.a external location) can be used in `INTO` or `FROM`.
///
/// For examples: `'s3://example/path/to/dir' CONNECTION = (AWS_ACCESS_ID="admin" AWS_SECRET_KEY="admin")`
#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum FileLocation {
    Stage(String),
    Uri(UriLocation),
}

impl Display for FileLocation {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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

/// Used when we want to allow use variable for options etc.
/// Other expr is not necessary, because
/// 1. we can always create a variable that can be used directly.
/// 2. columns can not be referred.
///
/// Can extend to all type of Literals if needed later.
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum LiteralStringOrVariable {
    Literal(String),
    Variable(String),
}

impl Display for LiteralStringOrVariable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LiteralStringOrVariable::Literal(s) => {
                write!(f, "'{s}'")
            }
            LiteralStringOrVariable::Variable(s) => {
                write!(f, "${s}")
            }
        }
    }
}

pub enum CopyIntoTableOption {
    Files(Vec<String>),
    Pattern(LiteralStringOrVariable),
    FileFormat(FileFormatOptions),
    SizeLimit(usize),
    MaxFiles(usize),
    SplitSize(usize),
    Purge(bool),
    Force(bool),
    DisableVariantCheck(bool),
    ReturnFailedOnly(bool),
    OnError(String),
    ColumnMatchMode(String),
}

pub enum CopyIntoLocationOption {
    FileFormat(FileFormatOptions),
    MaxFileSize(usize),
    Single(bool),
    IncludeQueryID(bool),
    UseRawPath(bool),
    DetailedOutput(bool),
    OverWrite(bool),
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Drive, DriveMut)]
pub struct FileFormatOptions {
    pub options: BTreeMap<String, FileFormatValue>,
}

impl FileFormatOptions {
    pub fn is_empty(&self) -> bool {
        self.options.is_empty()
    }
}

impl Display for FileFormatOptions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write_comma_separated_map(f, &self.options)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Drive, DriveMut)]
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            FileFormatValue::Keyword(v) => write!(f, "{v}"),
            FileFormatValue::Bool(v) => write!(f, "{v}"),
            FileFormatValue::U64(v) => write!(f, "{v}"),
            FileFormatValue::String(v) => {
                write!(f, "{}", QuotedString(v, '\''))
            }
            FileFormatValue::StringList(v) => {
                write!(f, "(")?;
                for (i, s) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", QuotedString(s, '\''))?;
                }
                write!(f, ")")
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Drive, DriveMut, Eq)]
pub enum OnErrorMode {
    Continue,
    SkipFileNum(u64),
    AbortNum(u64),
}

impl Default for OnErrorMode {
    fn default() -> Self {
        Self::AbortNum(1)
    }
}

impl Display for OnErrorMode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            OnErrorMode::Continue => {
                write!(f, "continue")
            }
            OnErrorMode::SkipFileNum(n) => {
                if *n <= 1 {
                    write!(f, "skipfile")
                } else {
                    write!(f, "skipfile_{}", n)
                }
            }
            OnErrorMode::AbortNum(n) => {
                if *n <= 1 {
                    write!(f, "abort")
                } else {
                    write!(f, "abort_{}", n)
                }
            }
        }
    }
}

const ERROR_MODE_MSG: &str =
    "OnError must one of {{ CONTINUE | SKIP_FILE | SKIP_FILE_<num> | ABORT | ABORT_<num> }}";
impl FromStr for OnErrorMode {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, &'static str> {
        match s.to_uppercase().as_str() {
            "" | "ABORT" => Ok(OnErrorMode::AbortNum(1)),
            "CONTINUE" => Ok(OnErrorMode::Continue),
            "SKIP_FILE" => Ok(OnErrorMode::SkipFileNum(1)),
            v => {
                if v.starts_with("ABORT_") {
                    let num_str = v.replace("ABORT_", "");
                    let nums = num_str.parse::<u64>();
                    match nums {
                        Ok(n) if n < 1 => Err(ERROR_MODE_MSG),
                        Ok(n) => Ok(OnErrorMode::AbortNum(n)),
                        Err(_) => Err(ERROR_MODE_MSG),
                    }
                } else {
                    let num_str = v.replace("SKIP_FILE_", "");
                    let nums = num_str.parse::<u64>();
                    match nums {
                        Ok(n) if n < 1 => Err(ERROR_MODE_MSG),
                        Ok(n) => Ok(OnErrorMode::SkipFileNum(n)),
                        Err(_) => Err(ERROR_MODE_MSG),
                    }
                }
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Drive, DriveMut, Eq)]
pub enum ColumnMatchMode {
    CaseSensitive,
    CaseInsensitive,
    Position,
}

impl Display for ColumnMatchMode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            ColumnMatchMode::CaseSensitive => write!(f, "CASE_SENSITIVE"),
            ColumnMatchMode::CaseInsensitive => write!(f, "CASE_INSENSITIVE"),
            ColumnMatchMode::Position => write!(f, "POSITION"),
        }
    }
}

const COLUMN_MATCH_MODE_MSG: &str =
    "ColumnMatchMode must be one of {{ CASE_SENSITIVE | CASE_INSENSITIVE | POSITION }}";
impl FromStr for ColumnMatchMode {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, &'static str> {
        match s.to_uppercase().as_str() {
            "CASE_SENSITIVE" => Ok(Self::CaseSensitive),
            "CASE_INSENSITIVE" => Ok(Self::CaseInsensitive),
            "POSITION" => Ok(Self::Position),
            _ => Err(COLUMN_MATCH_MODE_MSG),
        }
    }
}
