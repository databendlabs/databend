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
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_io::constants::NULL_BYTES_ESCAPE;
use databend_common_io::escape_string;
use serde::Deserialize;
use serde::Serialize;

use crate::principal::StageFileCompression;
use crate::principal::StageFileFormatType;

const OPT_FIELD_DELIMITER: &str = "field_delimiter";
const OPT_RECORDE_DELIMITER: &str = "record_delimiter";
const OPT_SKIP_HEADER: &str = "skip_header";
const OPT_NAN_DISPLAY: &str = "nan_display";
const OPT_NULL_DISPLAY: &str = "null_display";
const OPT_ESCAPE: &str = "escape";
const OPT_QUOTE: &str = "quote";
const OPT_ROW_TAG: &str = "row_tag";
const OPT_ERROR_ON_COLUMN_COUNT_MISMATCH: &str = "error_on_column_count_mismatch";
const MISSING_FIELD_AS: &str = "missing_field_as";
const NULL_FIELD_AS: &str = "null_field_as";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileFormatOptionsAst {
    pub options: BTreeMap<String, String>,
}

impl Display for FileFormatOptionsAst {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.options)
    }
}

impl FileFormatOptionsAst {
    pub fn new(options: BTreeMap<String, String>) -> Self {
        FileFormatOptionsAst { options }
    }

    fn take_string(&mut self, key: &str, default: String) -> String {
        self.options.remove(key).unwrap_or(default)
    }

    fn take_type(&mut self) -> Result<StageFileFormatType> {
        let typ = match self.options.remove("type") {
            Some(t) => t,
            None => self.options.remove("format").ok_or_else(|| {
                ErrorCode::IllegalFileFormat(format!(
                    "Missing type in file format options: {:?}",
                    self.options
                ))
            })?,
        };
        StageFileFormatType::from_str(&typ).map_err(ErrorCode::IllegalFileFormat)
    }

    fn take_compression(&mut self) -> Result<StageFileCompression> {
        match self.options.remove("compression") {
            Some(c) => StageFileCompression::from_str(&c).map_err(ErrorCode::IllegalFileFormat),
            None => Ok(StageFileCompression::None),
        }
    }

    fn take_u64(&mut self, key: &str, default: u64) -> Result<u64> {
        match self.options.remove(key) {
            Some(v) => Ok(u64::from_str(&v)?),
            None => Ok(default),
        }
    }

    fn take_bool(&mut self, key: &str, default: bool) -> Result<bool> {
        match self.options.remove(key) {
            Some(v) => Ok(bool::from_str(&v.to_lowercase()).map_err(|_| {
                ErrorCode::IllegalFileFormat(format!(
                    "Invalid boolean value {} for option {}",
                    v, key
                ))
            })?),
            None => Ok(default),
        }
    }
}

/// File format parameters after checking and parsing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FileFormatParams {
    Csv(CsvFileFormatParams),
    Tsv(TsvFileFormatParams),
    NdJson(NdJsonFileFormatParams),
    Json(JsonFileFormatParams),
    Xml(XmlFileFormatParams),
    Parquet(ParquetFileFormatParams),
}

impl FileFormatParams {
    pub fn get_type(&self) -> StageFileFormatType {
        match self {
            FileFormatParams::Csv(_) => StageFileFormatType::Csv,
            FileFormatParams::Tsv(_) => StageFileFormatType::Tsv,
            FileFormatParams::NdJson(_) => StageFileFormatType::NdJson,
            FileFormatParams::Json(_) => StageFileFormatType::Json,
            FileFormatParams::Xml(_) => StageFileFormatType::Xml,
            FileFormatParams::Parquet(_) => StageFileFormatType::Parquet,
        }
    }

    pub fn default_by_type(format_type: StageFileFormatType) -> Result<Self> {
        match format_type {
            StageFileFormatType::Parquet => {
                Ok(FileFormatParams::Parquet(ParquetFileFormatParams::default()))
            }
            StageFileFormatType::Csv => Ok(FileFormatParams::Csv(CsvFileFormatParams::default())),
            StageFileFormatType::Tsv => Ok(FileFormatParams::Tsv(TsvFileFormatParams::default())),
            StageFileFormatType::NdJson => {
                Ok(FileFormatParams::NdJson(NdJsonFileFormatParams::default()))
            }
            StageFileFormatType::Json => {
                Ok(FileFormatParams::Json(JsonFileFormatParams::default()))
            }
            StageFileFormatType::Xml => Ok(FileFormatParams::Xml(XmlFileFormatParams::default())),
            _ => Err(ErrorCode::IllegalFileFormat(format!(
                "Unsupported file format type: {:?}",
                format_type
            ))),
        }
    }

    pub fn compression(&self) -> StageFileCompression {
        match self {
            FileFormatParams::Csv(v) => v.compression,
            FileFormatParams::Tsv(v) => v.compression,
            FileFormatParams::NdJson(v) => v.compression,
            FileFormatParams::Json(v) => v.compression,
            FileFormatParams::Xml(v) => v.compression,
            FileFormatParams::Parquet(_) => StageFileCompression::None,
        }
    }

    pub fn try_from_ast(ast: FileFormatOptionsAst, old: bool) -> Result<Self> {
        let mut ast = ast;
        let typ = ast.take_type()?;
        let params = match typ {
            StageFileFormatType::Xml => {
                let default = XmlFileFormatParams::default();
                let row_tag = ast.take_string(OPT_ROW_TAG, default.row_tag);
                let compression = ast.take_compression()?;
                FileFormatParams::Xml(XmlFileFormatParams {
                    compression,
                    row_tag,
                })
            }
            StageFileFormatType::Json => {
                let compression = ast.take_compression()?;
                FileFormatParams::Json(JsonFileFormatParams { compression })
            }
            StageFileFormatType::NdJson => {
                let compression = ast.take_compression()?;
                let missing_field_as = ast.options.remove(MISSING_FIELD_AS);
                let null_field_as = ast.options.remove(NULL_FIELD_AS);
                FileFormatParams::NdJson(NdJsonFileFormatParams::try_create(
                    compression,
                    missing_field_as.as_deref(),
                    null_field_as.as_deref(),
                )?)
            }
            StageFileFormatType::Parquet => FileFormatParams::Parquet(ParquetFileFormatParams {}),
            StageFileFormatType::Csv => {
                let default = CsvFileFormatParams::default();
                let compression = ast.take_compression()?;
                let headers = ast.take_u64(OPT_SKIP_HEADER, default.headers)?;
                let field_delimiter = ast.take_string(OPT_FIELD_DELIMITER, default.field_delimiter);
                let record_delimiter =
                    ast.take_string(OPT_RECORDE_DELIMITER, default.record_delimiter);
                let nan_display = ast.take_string(OPT_NAN_DISPLAY, default.nan_display);
                let escape = ast.take_string(OPT_ESCAPE, default.escape);
                let quote = ast.take_string(OPT_QUOTE, default.quote);
                let null_display = ast.take_string(OPT_NULL_DISPLAY, default.null_display);
                let error_on_column_count_mismatch = ast.take_bool(
                    OPT_ERROR_ON_COLUMN_COUNT_MISMATCH,
                    default.error_on_column_count_mismatch,
                )?;
                FileFormatParams::Csv(CsvFileFormatParams {
                    compression,
                    headers,
                    field_delimiter,
                    record_delimiter,
                    null_display,
                    nan_display,
                    escape,
                    quote,
                    error_on_column_count_mismatch,
                })
            }
            StageFileFormatType::Tsv => {
                let default = TsvFileFormatParams::default();
                let compression = ast.take_compression()?;
                let headers = ast.take_u64(OPT_SKIP_HEADER, default.headers)?;
                let field_delimiter = ast.take_string(OPT_FIELD_DELIMITER, default.field_delimiter);
                let record_delimiter =
                    ast.take_string(OPT_RECORDE_DELIMITER, default.record_delimiter);
                let nan_display = ast.take_string(OPT_NAN_DISPLAY, default.nan_display);
                let escape = ast.take_string(OPT_ESCAPE, default.escape);
                let quote = ast.take_string(OPT_QUOTE, default.quote);
                FileFormatParams::Tsv(TsvFileFormatParams {
                    compression,
                    headers,
                    field_delimiter,
                    record_delimiter,
                    nan_display,
                    quote,
                    escape,
                })
            }
            _ => {
                return Err(ErrorCode::IllegalFileFormat(format!(
                    "Unsupported file format {typ:?}"
                )));
            }
        };
        if old {
            Ok(params)
        } else {
            params.check()?;
            if ast.options.is_empty() {
                Ok(params)
            } else {
                Err(ErrorCode::IllegalFileFormat(format!(
                    "Unsupported options for {:?} {:?}",
                    typ, ast.options
                )))
            }
        }
    }

    pub fn check(&self) -> Result<()> {
        match self {
            FileFormatParams::Tsv(p) => {
                check_str_len(&p.field_delimiter, 1, 1, "TSV", "field_delimiter")?;
                check_str_len(&p.quote, 1, 1, "TSV", "quote")?;
                check_str_len(&p.escape, 1, 1, "TSV", "escape")?;
                check_nan_display(&p.nan_display)?;
                check_record_delimiter(&p.record_delimiter)?;
            }
            FileFormatParams::Csv(p) => {
                check_str_len(&p.field_delimiter, 1, 1, "CSV", "field_delimiter")?;
                check_str_len(&p.quote, 1, 1, "CSV", "quote")?;
                check_str_len(&p.escape, 0, 1, "CSV", "escape")?;
                check_nan_display(&p.nan_display)?;
                check_record_delimiter(&p.record_delimiter)?;
            }
            FileFormatParams::Xml(p) => {
                check_str_len(&p.row_tag, 1, 1014, "XML", "row_tag")?;
            }
            _ => {}
        }
        Ok(())
    }
}

impl Default for FileFormatParams {
    fn default() -> Self {
        FileFormatParams::Parquet(ParquetFileFormatParams {})
    }
}

impl TryFrom<FileFormatOptionsAst> for FileFormatParams {
    type Error = ErrorCode;

    fn try_from(ast: FileFormatOptionsAst) -> Result<Self> {
        FileFormatParams::try_from_ast(ast, false)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvFileFormatParams {
    pub compression: StageFileCompression,
    pub headers: u64,
    pub field_delimiter: String,
    pub record_delimiter: String,
    pub null_display: String,
    pub nan_display: String,
    pub escape: String,
    pub quote: String,
    pub error_on_column_count_mismatch: bool,
}

impl Default for CsvFileFormatParams {
    fn default() -> Self {
        CsvFileFormatParams {
            compression: StageFileCompression::None,
            headers: 0,
            field_delimiter: ",".to_string(),
            record_delimiter: "\n".to_string(),
            null_display: NULL_BYTES_ESCAPE.to_string(),
            nan_display: "NaN".to_string(),
            escape: "".to_string(),
            quote: "\"".to_string(),
            error_on_column_count_mismatch: true,
        }
    }
}

impl CsvFileFormatParams {
    pub fn downcast_unchecked(params: &FileFormatParams) -> &CsvFileFormatParams {
        match params {
            FileFormatParams::Csv(p) => p,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TsvFileFormatParams {
    pub compression: StageFileCompression,
    pub headers: u64,
    pub field_delimiter: String,
    pub record_delimiter: String,
    pub nan_display: String,
    pub escape: String,
    pub quote: String,
}

impl Default for TsvFileFormatParams {
    fn default() -> Self {
        TsvFileFormatParams {
            compression: StageFileCompression::None,
            headers: 0,
            field_delimiter: "\t".to_string(),
            record_delimiter: "\n".to_string(),
            nan_display: "nan".to_string(),
            escape: "\\".to_string(),
            quote: "\'".to_string(),
        }
    }
}

impl TsvFileFormatParams {
    pub fn downcast_unchecked(params: &FileFormatParams) -> &TsvFileFormatParams {
        match params {
            FileFormatParams::Tsv(p) => p,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct XmlFileFormatParams {
    pub compression: StageFileCompression,
    pub row_tag: String,
}

impl XmlFileFormatParams {
    pub fn downcast_unchecked(params: &FileFormatParams) -> &XmlFileFormatParams {
        match params {
            FileFormatParams::Xml(p) => p,
            _ => unreachable!(),
        }
    }
}

impl Default for XmlFileFormatParams {
    fn default() -> Self {
        XmlFileFormatParams {
            compression: StageFileCompression::None,
            row_tag: "row".to_string(),
        }
    }
}

/// used for both `missing_field_as` and `null_field_as`
/// for extensibility, it is stored as PB string in meta
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JsonNullAs {
    /// for `missing_field_as` only, and is default for it for safety,
    /// in case of wrong field names when creating table.
    Error,
    /// only valid for nullable column
    Null,
    /// defined when creating table
    FieldDefault,
    TypeDefault,
}

impl JsonNullAs {
    fn parse(s: Option<&str>, option_name: &str, default: Self) -> Result<Self> {
        match s {
            Some(v) => v.parse::<JsonNullAs>().map_err(|_| {
                ErrorCode::InvalidArgument(format!("invalid value ({v}) for {option_name}"))
            }),
            None => Ok(default),
        }
    }
}
impl FromStr for JsonNullAs {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "error" => Ok(JsonNullAs::Error),
            "null" => Ok(JsonNullAs::Null),
            "field_default" => Ok(JsonNullAs::FieldDefault),
            "type_default" => Ok(JsonNullAs::TypeDefault),
            _ => Err(()),
        }
    }
}

impl Display for JsonNullAs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonNullAs::Error => write!(f, "error"),
            JsonNullAs::Null => write!(f, "null"),
            JsonNullAs::FieldDefault => write!(f, "field_default"),
            JsonNullAs::TypeDefault => write!(f, "type_default"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonFileFormatParams {
    pub compression: StageFileCompression,
}

impl JsonFileFormatParams {
    pub fn downcast_unchecked(params: &FileFormatParams) -> &JsonFileFormatParams {
        match params {
            FileFormatParams::Json(p) => p,
            _ => unreachable!(),
        }
    }
}

impl Default for JsonFileFormatParams {
    fn default() -> Self {
        JsonFileFormatParams {
            compression: StageFileCompression::None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NdJsonFileFormatParams {
    pub compression: StageFileCompression,
    pub missing_field_as: JsonNullAs,
    pub null_field_as: JsonNullAs,
}

impl NdJsonFileFormatParams {
    pub fn try_create(
        compression: StageFileCompression,
        missing_field_as: Option<&str>,
        null_field_as: Option<&str>,
    ) -> Result<Self> {
        let missing_field_as =
            JsonNullAs::parse(missing_field_as, MISSING_FIELD_AS, JsonNullAs::Error)?;
        let null_field_as =
            JsonNullAs::parse(null_field_as, MISSING_FIELD_AS, JsonNullAs::FieldDefault)?;
        if matches!(null_field_as, JsonNullAs::Error) {
            return Err(ErrorCode::InvalidArgument(
                "NULL_FIELD_AS cannot be `error`",
            ));
        }
        Ok(Self {
            compression,
            missing_field_as,
            null_field_as,
        })
    }
}

impl Default for NdJsonFileFormatParams {
    fn default() -> Self {
        NdJsonFileFormatParams {
            compression: StageFileCompression::None,
            missing_field_as: JsonNullAs::Error,
            null_field_as: JsonNullAs::FieldDefault,
        }
    }
}

impl NdJsonFileFormatParams {
    pub fn downcast_unchecked(params: &FileFormatParams) -> &NdJsonFileFormatParams {
        match params {
            FileFormatParams::NdJson(p) => p,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetFileFormatParams {}

impl Display for FileFormatParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileFormatParams::Csv(params) => {
                write!(
                    f,
                    "TYPE = CSV COMPRESSION = {:?} HEADERS= {} FIELD_DELIMITER = '{}' RECORD_DELIMITER = '{}' NAN_DISPLAY = '{}' ESCAPE = '{}' QUOTE = '{}'",
                    params.compression,
                    params.headers,
                    escape_string(&params.field_delimiter),
                    escape_string(&params.record_delimiter),
                    escape_string(&params.nan_display),
                    escape_string(&params.escape),
                    escape_string(&params.quote)
                )
            }
            FileFormatParams::Tsv(params) => {
                write!(
                    f,
                    "TYPE = TSV COMPRESSION = {:?} HEADERS= {} FIELD_DELIMITER = '{}' RECORD_DELIMITER = '{}' NAN_DISPLAY = '{}' ESCAPE = '{}' QUOTE = '{}'",
                    params.compression,
                    params.headers,
                    escape_string(&params.field_delimiter),
                    escape_string(&params.record_delimiter),
                    escape_string(&params.nan_display),
                    escape_string(&params.escape),
                    escape_string(&params.quote)
                )
            }
            FileFormatParams::Xml(params) => {
                write!(
                    f,
                    "TYPE = XML, COMPRESSION = {:?}, ROW_TAG = '{}'",
                    params.compression, params.row_tag
                )
            }
            FileFormatParams::Json(params) => {
                write!(f, "TYPE = JSON, COMPRESSION = {:?}", params.compression)
            }
            FileFormatParams::NdJson(params) => {
                write!(f, "TYPE = NDJSON, COMPRESSION = {:?}", params.compression)
            }
            FileFormatParams::Parquet(_) => {
                write!(f, "TYPE = PARQUET")
            }
        }
    }
}

pub fn check_str_len(
    option: &str,
    min: usize,
    max: usize,
    fmt_name: &str,
    option_name: &str,
) -> Result<()> {
    let len = option.as_bytes().len();
    if len < min || len > max {
        Err(ErrorCode::InvalidArgument(format!(
            "len of option {option_name} for {fmt_name} must in [{min}, {max}], got {option}"
        )))
    } else {
        Ok(())
    }
}

/// `\r\n` or u8
pub fn check_record_delimiter(option: &str) -> Result<()> {
    match option.len() {
        1 => {}
        2 => {
            if option != "\r\n" {
                return Err(ErrorCode::InvalidArgument(
                    "record_delimiter with two chars can only be '\\r\\n'",
                ));
            };
        }
        _ => {
            return Err(ErrorCode::InvalidArgument(
                "record_delimiter must be one char or '\\r\\n'",
            ));
        }
    }
    Ok(())
}

fn check_nan_display(nan_display: &str) -> Result<()> {
    let lower = nan_display.to_lowercase();
    if lower != "nan" && lower != "null" {
        Err(ErrorCode::InvalidArgument(
            "nan_display must be literal `nan` or `null` (case-insensitive)",
        ))
    } else {
        Ok(())
    }
}
