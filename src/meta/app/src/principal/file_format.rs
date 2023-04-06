// Copyright 2023 Datafuse Labs.
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
use std::str::FromStr;

use common_exception::ErrorCode;
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

use crate::principal::StageFileCompression;
use crate::principal::StageFileFormatType;

const OPT_FILED_DELIMITER: &str = "field_delimiter";
const OPT_RECORDE_DELIMITER: &str = "record_delimiter";
const OPT_SKIP_HEADER: &str = "skip_header";
const OPT_NAN_DISPLAY: &str = "nan_display";
const OPT_ESCAPE: &str = "escape";
const OPT_QUOTE: &str = "quote";
const OPT_ROW_TAG: &str = "row_tag";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileFormatOptionsAst {
    pub options: BTreeMap<String, String>,
}

impl FileFormatOptionsAst {
    fn take_string(&mut self, key: &str, default: &str) -> String {
        self.options
            .remove(key)
            .unwrap_or_else(|| default.to_string())
    }

    fn take_type(&mut self) -> Result<StageFileFormatType> {
        let typ = self.options.remove("type").ok_or_else(|| {
            ErrorCode::IllegalFileFormat("Missing type in file format options".to_string())
        })?;
        StageFileFormatType::from_str(&typ).map_err(ErrorCode::IllegalFileFormat)
    }

    fn take_compression(&mut self) -> Result<StageFileCompression> {
        let compression = self.options.remove("compression").ok_or_else(|| {
            ErrorCode::IllegalFileFormat("Missing compression in file format options".to_string())
        })?;
        StageFileCompression::from_str(&compression).map_err(ErrorCode::IllegalFileFormat)
    }

    fn take_u64(&mut self, key: &str, default: u64) -> Result<u64> {
        match self.options.remove(key) {
            Some(v) => Ok(u64::from_str(&v)?),
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

impl Default for FileFormatParams {
    fn default() -> Self {
        FileFormatParams::Parquet(ParquetFileFormatParams {})
    }
}

impl TryFrom<FileFormatOptionsAst> for FileFormatParams {
    type Error = ErrorCode;
    fn try_from(ast: FileFormatOptionsAst) -> Result<Self> {
        let mut ast = ast;
        let typ = ast.take_type()?;
        let params = match typ {
            StageFileFormatType::Xml => {
                let row_tag = ast.take_string(OPT_ROW_TAG, "row");
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
                FileFormatParams::NdJson(NdJsonFileFormatParams { compression })
            }
            StageFileFormatType::Parquet => FileFormatParams::Parquet(ParquetFileFormatParams {}),
            StageFileFormatType::Csv => {
                let compression = ast.take_compression()?;
                let headers = ast.take_u64(OPT_SKIP_HEADER, 0)?;
                let field_delimiter = ast.take_string(OPT_FILED_DELIMITER, ",");
                let record_delimiter = ast.take_string(OPT_RECORDE_DELIMITER, "\n");
                let nan_display = ast.take_string(OPT_NAN_DISPLAY, "NaN");
                let escape = ast.take_string(OPT_ESCAPE, "");
                let quote = ast.take_string(OPT_QUOTE, "\"");
                FileFormatParams::Csv(CsvFileFormatParams {
                    compression,
                    headers,
                    field_delimiter,
                    record_delimiter,
                    nan_display,
                    escape,
                    quote,
                })
            }
            StageFileFormatType::Tsv => {
                let compression = ast.take_compression()?;
                let headers = ast.take_u64(OPT_SKIP_HEADER, 0)?;
                let field_delimiter = ast.take_string(OPT_FILED_DELIMITER, "\t");
                let record_delimiter = ast.take_string(OPT_RECORDE_DELIMITER, "\n");
                let nan_display = ast.take_string(OPT_NAN_DISPLAY, "nan");
                let escape = ast.take_string(OPT_ESCAPE, "\\");
                let quote = ast.take_string(OPT_QUOTE, "\'");
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvFileFormatParams {
    pub compression: StageFileCompression,
    pub headers: u64,
    pub field_delimiter: String,
    pub record_delimiter: String,
    pub nan_display: String,
    pub escape: String,
    pub quote: String,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct XmlFileFormatParams {
    pub compression: StageFileCompression,
    pub row_tag: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct JsonFileFormatParams {
    pub compression: StageFileCompression,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NdJsonFileFormatParams {
    pub compression: StageFileCompression,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ParquetFileFormatParams {}
