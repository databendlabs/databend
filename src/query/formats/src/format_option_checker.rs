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

use common_exception::ErrorCode;
use common_exception::Result;
use common_io::consts::NAN_BYTES_LOWER;
use common_io::consts::NAN_BYTES_SNAKE;
use common_meta_types::StageFileFormatType;

use crate::FileFormatOptionsExt;

pub fn get_format_option_checker(
    fmt: &StageFileFormatType,
) -> Result<Box<dyn FormatOptionChecker>> {
    match fmt {
        StageFileFormatType::Csv => Ok(Box::new(CSVFormatOptionChecker {})),
        StageFileFormatType::Tsv => Ok(Box::new(TSVFormatOptionChecker {})),
        StageFileFormatType::NdJson => Ok(Box::new(NDJsonFormatOptionChecker {})),
        StageFileFormatType::Parquet => Ok(Box::new(ParquetFormatOptionChecker {})),
        StageFileFormatType::Xml => Ok(Box::new(XMLFormatOptionChecker {})),
        _ => Err(ErrorCode::Internal(format!(
            "unexpect format type {:?}",
            fmt
        ))),
    }
}

pub trait FormatOptionChecker {
    fn name(&self) -> String;

    fn check_options(&self, options: &mut FileFormatOptionsExt) -> Result<()> {
        self.check_escape(&mut options.stage.escape)?;
        self.check_quote(&mut options.stage.quote)?;
        self.check_row_tag(&mut options.stage.row_tag)?;
        self.check_record_delimiter(&mut options.stage.record_delimiter)?;
        self.check_field_delimiter(&mut options.stage.field_delimiter)?;
        self.check_nan_display(&mut options.stage.nan_display)?;
        Ok(())
    }

    fn not_supported(&self, option_name: &str) -> ErrorCode {
        let msg = format!("{} not support option {}", self.name(), option_name);
        ErrorCode::BadArguments(msg)
    }

    fn check_escape(&self, escape: &mut String) -> Result<()> {
        if !escape.is_empty() {
            Err(self.not_supported("escape"))
        } else {
            Ok(())
        }
    }

    fn check_quote(&self, quote: &mut String) -> Result<()> {
        if !quote.is_empty() {
            Err(self.not_supported("quote"))
        } else {
            Ok(())
        }
    }

    fn check_row_tag(&self, row_tag: &mut String) -> Result<()> {
        if !row_tag.is_empty() && row_tag != "row" {
            Err(self.not_supported("row_tag"))
        } else {
            Ok(())
        }
    }

    fn check_record_delimiter(&self, record_delimiter: &mut String) -> Result<()> {
        if !record_delimiter.is_empty() {
            Err(self.not_supported("record_delimiter"))
        } else {
            Ok(())
        }
    }

    fn check_field_delimiter(&self, field_delimiter: &mut String) -> Result<()> {
        if !field_delimiter.is_empty() {
            Err(self.not_supported("field_delimiter"))
        } else {
            Ok(())
        }
    }

    fn check_nan_display(&self, nan_display: &mut String) -> Result<()> {
        if !nan_display.is_empty() {
            Err(self.not_supported("nan_display"))
        } else {
            Ok(())
        }
    }
}

pub struct CSVFormatOptionChecker {}

impl FormatOptionChecker for CSVFormatOptionChecker {
    fn name(&self) -> String {
        "CSV".to_string()
    }

    fn check_escape(&self, escape: &mut String) -> Result<()> {
        check_escape(escape, "")
    }

    fn check_quote(&self, quote: &mut String) -> Result<()> {
        check_quote(quote, "\"")
    }

    fn check_record_delimiter(&self, record_delimiter: &mut String) -> Result<()> {
        check_record_delimiter(record_delimiter)
    }

    fn check_field_delimiter(&self, field_delimiter: &mut String) -> Result<()> {
        check_field_delimiter(field_delimiter, ",")
    }

    fn check_nan_display(&self, nan_display: &mut String) -> Result<()> {
        check_nan_display(nan_display, NAN_BYTES_SNAKE)
    }
}

pub struct TSVFormatOptionChecker {}

impl FormatOptionChecker for TSVFormatOptionChecker {
    fn name(&self) -> String {
        "TSV".to_string()
    }

    fn check_escape(&self, escape: &mut String) -> Result<()> {
        check_escape(escape, "\\")
    }

    fn check_quote(&self, quote: &mut String) -> Result<()> {
        check_quote(quote, "\'")
    }

    fn check_record_delimiter(&self, record_delimiter: &mut String) -> Result<()> {
        check_record_delimiter(record_delimiter)
    }

    fn check_field_delimiter(&self, field_delimiter: &mut String) -> Result<()> {
        check_field_delimiter(field_delimiter, "\t")
    }

    fn check_nan_display(&self, nan_display: &mut String) -> Result<()> {
        check_nan_display(nan_display, NAN_BYTES_LOWER)
    }
}

pub struct NDJsonFormatOptionChecker {}

impl FormatOptionChecker for NDJsonFormatOptionChecker {
    fn name(&self) -> String {
        "NDJson".to_string()
    }

    fn check_record_delimiter(&self, record_delimiter: &mut String) -> Result<()> {
        check_record_delimiter(record_delimiter)
    }

    fn check_field_delimiter(&self, field_delimiter: &mut String) -> Result<()> {
        check_field_delimiter(field_delimiter, "\t")
    }
}

pub struct XMLFormatOptionChecker {}

impl FormatOptionChecker for XMLFormatOptionChecker {
    fn name(&self) -> String {
        "XML".to_string()
    }

    fn check_row_tag(&self, row_tag: &mut String) -> Result<()> {
        if row_tag.is_empty() {
            *row_tag = "row".to_string()
        }
        Ok(())
    }
}

pub struct ParquetFormatOptionChecker {}
impl FormatOptionChecker for ParquetFormatOptionChecker {
    fn name(&self) -> String {
        "Parquet".to_string()
    }
}

pub fn check_escape(option: &mut String, default: &str) -> Result<()> {
    if option.is_empty() {
        *option = default.to_string()
    } else if option.len() > 1 {
        return Err(ErrorCode::InvalidArgument(
            "escape can only contain one char",
        ));
    };
    Ok(())
}

pub fn check_quote(option: &mut String, default: &str) -> Result<()> {
    if option.is_empty() {
        *option = default.to_string()
    } else if option.len() > 1 {
        return Err(ErrorCode::InvalidArgument(
            "quote can only contain one char",
        ));
    };
    Ok(())
}

pub fn check_field_delimiter(option: &mut String, default: &str) -> Result<()> {
    if option.is_empty() {
        *option = default.to_string()
    } else if option.as_bytes().len() > 1 {
        return Err(ErrorCode::InvalidArgument(
            "field_delimiter can only contain one char",
        ));
    };
    Ok(())
}

/// `\r\n` or u8
pub fn check_record_delimiter(option: &mut String) -> Result<()> {
    match option.len() {
        0 => *option = "\n".to_string(),
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
                "record_delimiter can not more than two chars, please use one char or '\\r\\n'",
            ));
        }
    }

    Ok(())
}

fn check_nan_display(nan_display: &mut String, default: &str) -> Result<()> {
    if nan_display.is_empty() {
        *nan_display = default.to_string()
    } else {
        let lower = nan_display.to_lowercase();
        if lower != "nan" && lower != "null" {
            return Err(ErrorCode::InvalidArgument(
                "nan_display must be literal `nan` or `null` (case-insensitive)",
            ));
        }
    }
    Ok(())
}
