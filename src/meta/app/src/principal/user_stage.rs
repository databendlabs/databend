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
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_io::constants::NAN_BYTES_SNAKE;
use databend_common_io::escape_string;

use crate::principal::FileFormatParams;
use crate::principal::UserIdentity;
use crate::storage::StorageParams;

// -- Internal stage
// CREATE [ OR REPLACE ] [ TEMPORARY ] STAGE [ IF NOT EXISTS ] <internal_stage_name>
// internalStageParams
// directoryTableParams
// [ FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] ) } ]
// [ COPY_OPTIONS = ( copyOptions ) ]
// [ COMMENT = '<string_literal>' ]
//
// -- External stage
// CREATE [ OR REPLACE ] [ TEMPORARY ] STAGE [ IF NOT EXISTS ] <external_stage_name>
// externalStageParams
// directoryTableParams
// [ FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] ) } ]
// [ COPY_OPTIONS = ( copyOptions ) ]
// [ COMMENT = '<string_literal>' ]
//
//
// WHERE
//
// externalStageParams (for Amazon S3) ::=
// 's3://<bucket>[/<path>/]'
// [ { CREDENTIALS = ( {  { AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>' [ AWS_TOKEN = '<string>' ] } | AWS_ROLE = '<string>'  } ) ) } ]
//
// copyOptions ::=
// ON_ERROR = { CONTINUE | SKIP_FILE | SKIP_FILE_<num> | SKIP_FILE_<num>% | ABORT_STATEMENT }
// SIZE_LIMIT = <num>

/// Maximum files per 'copy into table' commit.
pub const COPY_MAX_FILES_PER_COMMIT: usize = 15000;

/// Instruction for exceeding 'copy into table' file limit.
pub const COPY_MAX_FILES_COMMIT_MSG: &str = "Commit limit reached: 15,000 files for 'copy into table'. To handle more files, adjust 'CopyOption' with 'max_files=<num>'(e.g., 'max_files=10000') and perform several operations until all files are processed.";

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum StageType {
    /// LegacyInternal will be deprecated.
    ///
    /// Please never use this variant except in `proto_conv`. We keep this
    /// stage type for backward compatible.
    ///
    /// TODO(xuanwo): remove this when we are releasing v0.9.
    LegacyInternal,
    External,
    Internal,
    /// User Stage is the stage for every sql user.
    ///
    /// This is a stage that just in memory. We will not persist in metasrv
    User,
}

impl fmt::Display for StageType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let name = match self {
            // LegacyInternal will print the same name as Internal, this is by design.
            StageType::LegacyInternal => "Internal",
            StageType::External => "External",
            StageType::Internal => "Internal",
            StageType::User => "User",
        };
        write!(f, "{}", name)
    }
}

impl Default for StageType {
    fn default() -> Self {
        Self::External
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
pub enum StageFileCompression {
    Auto,
    Gzip,
    Bz2,
    Brotli,
    Zstd,
    Deflate,
    RawDeflate,
    Lzo,
    Snappy,
    Xz,
    None,
}

impl StageFileFormatType {
    pub fn has_inner_schema(&self) -> bool {
        matches!(self, StageFileFormatType::Parquet)
    }
}

impl Default for StageFileCompression {
    fn default() -> Self {
        Self::None
    }
}

impl FromStr for StageFileCompression {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(StageFileCompression::Auto),
            "gzip" => Ok(StageFileCompression::Gzip),
            "bz2" => Ok(StageFileCompression::Bz2),
            "brotli" => Ok(StageFileCompression::Brotli),
            "zstd" => Ok(StageFileCompression::Zstd),
            "deflate" => Ok(StageFileCompression::Deflate),
            "rawdeflate" | "raw_deflate" => Ok(StageFileCompression::RawDeflate),
            "lzo" => Ok(StageFileCompression::Lzo),
            "snappy" => Ok(StageFileCompression::Snappy),
            "xz" => Ok(StageFileCompression::Xz),
            "none" => Ok(StageFileCompression::None),
            _ => Err("Unknown file compression type, must one of { auto | gzip | bz2 | brotli | zstd | deflate | raw_deflate | lzo | snappy | xz | none }"
                .to_string()),
        }
    }
}

impl ToString for StageFileCompression {
    fn to_string(&self) -> String {
        match *self {
            StageFileCompression::Auto => "auto".to_string(),
            StageFileCompression::Gzip => "gzip".to_string(),
            StageFileCompression::Bz2 => "bz2".to_string(),
            StageFileCompression::Brotli => "brotli".to_string(),
            StageFileCompression::Zstd => "zstd".to_string(),
            StageFileCompression::Deflate => "deflate".to_string(),
            StageFileCompression::RawDeflate => "raw_deflate".to_string(),
            StageFileCompression::Lzo => "lzo".to_string(),
            StageFileCompression::Snappy => "snappy".to_string(),
            StageFileCompression::Xz => "xz".to_string(),
            StageFileCompression::None => "none".to_string(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum StageFileFormatType {
    Csv,
    Tsv,
    Json,
    NdJson,
    Avro,
    Orc,
    Parquet,
    Xml,
    None,
}

impl Default for StageFileFormatType {
    fn default() -> Self {
        Self::Parquet
    }
}

impl FromStr for StageFileFormatType {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s.to_uppercase().as_str() {
            "CSV" => Ok(StageFileFormatType::Csv),
            "TSV" | "TABSEPARATED" => Ok(StageFileFormatType::Tsv),
            "NDJSON" | "JSONEACHROW" => Ok(StageFileFormatType::NdJson),
            "PARQUET" => Ok(StageFileFormatType::Parquet),
            "XML" => Ok(StageFileFormatType::Xml),
            "JSON" => Ok(StageFileFormatType::Json),
            "ORC" => Ok(StageFileFormatType::Orc),
            "AVRO" => Err(format!(
                "File format type '{s}' not implemented yet', must be one of ( CSV | TSV | NDJSON | PARQUET | ORC)"
            )),
            _ => Err(format!(
                "Unknown file format type '{s}', must be one of ( CSV | TSV | NDJSON | PARQUET | ORC)"
            )),
        }
    }
}

impl ToString for StageFileFormatType {
    fn to_string(&self) -> String {
        format!("{:?}", *self).to_uppercase()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(default)]
pub struct FileFormatOptions {
    pub format: StageFileFormatType,
    // Number of lines at the start of the file to skip.
    pub skip_header: u64,
    pub field_delimiter: String,
    pub record_delimiter: String,
    pub nan_display: String,
    pub escape: String,
    pub compression: StageFileCompression,
    pub row_tag: String,
    pub quote: String,
    pub name: Option<String>,
}

impl Default for FileFormatOptions {
    fn default() -> Self {
        Self {
            format: StageFileFormatType::default(),
            record_delimiter: "\n".to_string(),
            field_delimiter: ",".to_string(),
            nan_display: NAN_BYTES_SNAKE.to_string(),
            skip_header: 0,
            escape: "".to_string(),
            compression: StageFileCompression::default(),
            row_tag: "row".to_string(),
            quote: "".to_string(),
            name: None,
        }
    }
}

impl FileFormatOptions {
    pub fn new() -> Self {
        Self {
            format: StageFileFormatType::None,
            field_delimiter: "".to_string(),
            record_delimiter: "".to_string(),
            nan_display: "".to_string(),
            skip_header: 0,
            escape: "".to_string(),
            compression: StageFileCompression::None,
            row_tag: "".to_string(),
            quote: "".to_string(),
            name: None,
        }
    }

    pub fn from_map(opts: &BTreeMap<String, String>) -> Result<Self> {
        let mut file_format_options = Self::new();
        file_format_options.apply(opts, false)?;
        if file_format_options.format == StageFileFormatType::None {
            return Err(ErrorCode::SyntaxException(
                "File format type must be specified",
            ));
        }
        Ok(file_format_options)
    }

    pub fn to_map(&self) -> BTreeMap<String, String> {
        let mut opts = BTreeMap::new();
        opts.insert("format".to_string(), self.format.to_string());
        opts.insert("skip_header".to_string(), self.skip_header.to_string());
        opts.insert(
            "field_delimiter".to_string(),
            self.field_delimiter.to_string(),
        );
        opts.insert(
            "record_delimiter".to_string(),
            self.record_delimiter.to_string(),
        );
        opts.insert("nan_display".to_string(), self.nan_display.to_string());
        opts.insert("escape".to_string(), self.escape.to_string());
        opts.insert("compression".to_string(), self.compression.to_string());
        opts.insert("row_tag".to_string(), self.row_tag.to_string());
        opts.insert("quote".to_string(), self.quote.to_string());
        if let Some(name) = &self.name {
            opts.insert("name".to_string(), name.to_string());
        }
        opts
    }

    pub fn default_by_type(format_type: StageFileFormatType) -> Self {
        let mut options = Self::default();
        match &format_type {
            StageFileFormatType::Csv => {
                options.quote = "\"".to_string();
            }
            StageFileFormatType::Tsv => {
                options.field_delimiter = "\t".to_string();
                options.escape = "\\".to_string();
            }
            _ => {}
        }
        options
    }

    pub fn apply(&mut self, opts: &BTreeMap<String, String>, ignore_unknown: bool) -> Result<()> {
        if opts.is_empty() {
            return Ok(());
        }
        for (k, v) in opts.iter() {
            match k.as_str() {
                "format" | "type" => {
                    let format = StageFileFormatType::from_str(v)?;
                    self.format = format;
                }
                "skip_header" => {
                    let skip_header = u64::from_str(v)?;
                    self.skip_header = skip_header;
                }
                "field_delimiter" => self.field_delimiter = v.clone(),
                "record_delimiter" => self.record_delimiter = v.clone(),
                "nan_display" => self.nan_display = v.clone(),
                "escape" => self.escape = v.clone(),
                "compression" => {
                    let compression = StageFileCompression::from_str(v)?;
                    self.compression = compression;
                }
                "row_tag" => self.row_tag = v.clone(),
                "quote" => self.quote = v.clone(),
                _ => {
                    if !ignore_unknown {
                        return Err(ErrorCode::BadArguments(format!(
                            "Unknown stage file format option {}",
                            k
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

impl Display for FileFormatOptions {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "TYPE = {}", self.format.to_string().to_uppercase())?;
        match self.format {
            StageFileFormatType::Csv => {
                write!(
                    f,
                    " FIELD_DELIMITER = '{}'",
                    escape_string(&self.field_delimiter)
                )?;
                write!(
                    f,
                    " RECORD_DELIMITER = '{}'",
                    escape_string(&self.record_delimiter)
                )?;
                write!(f, " QUOTE = '{}'", escape_string(&self.quote))?;
                write!(f, " ESCAPE = '{}'", escape_string(&self.escape))?;
                write!(f, " SKIP_HEADER = {}", &self.skip_header)?;
                write!(f, " NAN_DISPLAY = '{}'", escape_string(&self.nan_display))?;
            }
            StageFileFormatType::Tsv => {
                write!(
                    f,
                    " FIELD_DELIMITER = '{}'",
                    escape_string(&self.field_delimiter)
                )?;
                write!(
                    f,
                    " RECORD_DELIMITER = '{}'",
                    escape_string(&self.record_delimiter)
                )?;
            }
            StageFileFormatType::Xml => {
                write!(f, " ROW_TAG = {}", escape_string(&self.row_tag))?;
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Default, Clone, Debug, Eq, PartialEq)]
#[serde(default)]
pub struct StageParams {
    pub storage: StorageParams,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
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

impl FromStr for OnErrorMode {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s.to_uppercase().as_str() {
            "" | "ABORT" => Ok(OnErrorMode::AbortNum(1)),
            "CONTINUE" => Ok(OnErrorMode::Continue),
            "SKIP_FILE" => Ok(OnErrorMode::SkipFileNum(1)),
            v => {
                if v.starts_with("ABORT_") {
                    let num_str = v.replace("ABORT_", "");
                    let nums = num_str.parse::<u64>();
                    match nums {
                        Ok(n) if n < 1 => {
                            Err("OnError mode `ABORT_<num>` num must be greater than 0".to_string())
                        }
                        Ok(n) => Ok(OnErrorMode::AbortNum(n)),
                        Err(_) => Err(format!(
                            "Unknown OnError mode:{:?}, must one of {{ CONTINUE | SKIP_FILE | SKIP_FILE_<num> | ABORT | ABORT_<num> }}",
                            v
                        )),
                    }
                } else {
                    let num_str = v.replace("SKIP_FILE_", "");
                    let nums = num_str.parse::<u64>();
                    match nums {
                        Ok(n) if n < 1 => {
                            Err("OnError mode `SKIP_FILE_<num>` num must be greater than 0"
                                .to_string())
                        }
                        Ok(n) => Ok(OnErrorMode::SkipFileNum(n)),
                        Err(_) => Err(format!(
                            "Unknown OnError mode:{:?}, must one of {{ CONTINUE | SKIP_FILE | SKIP_FILE_<num> | ABORT | ABORT_<num> }}",
                            v
                        )),
                    }
                }
            }
        }
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

impl From<databend_common_ast::ast::OnErrorMode> for OnErrorMode {
    fn from(opt: databend_common_ast::ast::OnErrorMode) -> Self {
        match opt {
            databend_common_ast::ast::OnErrorMode::Continue => OnErrorMode::Continue,
            databend_common_ast::ast::OnErrorMode::SkipFileNum(n) => OnErrorMode::SkipFileNum(n),
            databend_common_ast::ast::OnErrorMode::AbortNum(n) => OnErrorMode::AbortNum(n),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Default, Debug, Eq, PartialEq)]
#[serde(default)]
pub struct CopyOptions {
    pub on_error: OnErrorMode,
    pub size_limit: usize,
    pub max_files: usize,
    pub split_size: usize,
    pub purge: bool,
    pub disable_variant_check: bool,
    pub return_failed_only: bool,

    // unload only
    pub max_file_size: usize,
    pub single: bool,
    pub detailed_output: bool,
}

impl CopyOptions {
    pub fn apply(&mut self, opts: &BTreeMap<String, String>, ignore_unknown: bool) -> Result<()> {
        if opts.is_empty() {
            return Ok(());
        }
        for (k, v) in opts.iter() {
            match k.as_str() {
                "on_error" => {
                    let on_error = OnErrorMode::from_str(v)?;
                    self.on_error = on_error;
                }
                "size_limit" => {
                    let size_limit = usize::from_str(v)?;
                    self.size_limit = size_limit;
                }
                "max_files" => {
                    let max_files = usize::from_str(v)?;
                    self.max_files = max_files;
                }
                "split_size" => {
                    let split_size = usize::from_str(v)?;
                    self.split_size = split_size;
                }
                "purge" => {
                    let purge = bool::from_str(v).map_err(|_| {
                        ErrorCode::StrParseError(format!("Cannot parse purge: {} as bool", v))
                    })?;
                    self.purge = purge;
                }
                "single" => {
                    let single = bool::from_str(v).map_err(|_| {
                        ErrorCode::StrParseError(format!("Cannot parse single: {} as bool", v))
                    })?;
                    self.single = single;
                }
                "max_file_size" => {
                    let max_file_size = usize::from_str(v)?;
                    self.max_file_size = max_file_size;
                }
                "disable_variant_check" => {
                    let disable_variant_check = bool::from_str(v).map_err(|_| {
                        ErrorCode::StrParseError(format!(
                            "Cannot parse disable_variant_check: {} as bool",
                            v
                        ))
                    })?;
                    self.disable_variant_check = disable_variant_check;
                }
                "return_failed_only" => {
                    let return_failed_only = bool::from_str(v).map_err(|_| {
                        ErrorCode::StrParseError(format!(
                            "Cannot parse return_failed_only: {} as bool",
                            v
                        ))
                    })?;
                    self.return_failed_only = return_failed_only;
                }
                _ => {
                    if !ignore_unknown {
                        return Err(ErrorCode::BadArguments(format!(
                            "Unknown stage copy option {}",
                            k
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

impl Display for CopyOptions {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "OnErrorMode {}", self.on_error)?;
        write!(f, "SizeLimit {}", self.size_limit)?;
        write!(f, "MaxFiles {}", self.max_files)?;
        write!(f, "SplitSize {}", self.split_size)?;
        write!(f, "Purge {}", self.purge)?;
        write!(f, "DisableVariantCheck {}", self.disable_variant_check)?;
        write!(f, "ReturnFailedOnly {}", self.return_failed_only)?;
        write!(f, "MaxFileSize {}", self.max_file_size)?;
        write!(f, "Single {}", self.single)?;
        write!(f, "DetailedOutput {}", self.detailed_output)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Default, Clone, Debug, Eq, PartialEq)]
#[serde(default)]
pub struct StageInfo {
    pub stage_name: String,
    pub stage_type: StageType,
    pub stage_params: StageParams,
    // on `COPY INTO xx FROM 's3://xxx?ak=?&sk=?'`, the URL(ExternalLocation) will be treated as an temporary stage.
    pub is_temporary: bool,
    pub file_format_params: FileFormatParams,
    pub copy_options: CopyOptions,
    pub comment: String,
    /// TODO(xuanwo): stage doesn't have this info anymore, remove it.
    pub number_of_files: u64,
    pub creator: Option<UserIdentity>,
    pub created_on: DateTime<Utc>,
}

impl StageInfo {
    /// Create a new internal stage.
    pub fn new_internal_stage(name: &str) -> StageInfo {
        StageInfo {
            stage_name: name.to_string(),
            stage_type: StageType::Internal,
            ..Default::default()
        }
    }

    pub fn new_external_stage(storage: StorageParams, is_temporary: bool) -> StageInfo {
        StageInfo {
            stage_name: format!("{storage}"),
            stage_type: StageType::External,
            is_temporary,
            stage_params: StageParams { storage },
            ..Default::default()
        }
    }

    /// Create a new user stage.
    pub fn new_user_stage(user: &str) -> StageInfo {
        StageInfo {
            stage_name: user.to_string(),
            stage_type: StageType::User,
            ..Default::default()
        }
    }

    /// Update user stage with stage name.
    pub fn with_stage_name(mut self, name: &str) -> StageInfo {
        self.stage_name = name.to_string();
        self
    }

    /// Get the prefix of stage.
    ///
    /// Use this function to get the prefix of this stage in the data operator.
    ///
    /// # Notes
    ///
    /// This function should never be called on external stage because it's meanless. Something must be wrong.
    pub fn stage_prefix(&self) -> String {
        match self.stage_type {
            StageType::LegacyInternal => format!("/stage/{}/", self.stage_name),
            StageType::External => {
                unreachable!("stage_prefix should never be called on external stage, must be a bug")
            }
            StageType::Internal => format!("/stage/internal/{}/", self.stage_name),
            StageType::User => format!("/stage/user/{}/", self.stage_name),
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct StageFile {
    pub path: String,
    pub size: u64,
    pub md5: Option<String>,
    pub last_modified: DateTime<Utc>,
    pub creator: Option<UserIdentity>,
    pub etag: Option<String>,
}
