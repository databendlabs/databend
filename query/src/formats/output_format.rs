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

use std::fmt;
use std::str::FromStr;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use strum_macros::EnumIter;

use super::output_format_json_each_row::JsonEachRowOutputFormat;
use super::output_format_parquet::ParquetOutputFormat;
use super::output_format_values::ValuesOutputFormat;
use crate::formats::output_format_csv::CSVOutputFormat;
use crate::formats::output_format_csv::CSVWithNamesAndTypesOutputFormat;
use crate::formats::output_format_csv::CSVWithNamesOutputFormat;
use crate::formats::output_format_csv::TSVOutputFormat;
use crate::formats::output_format_csv::TSVWithNamesAndTypesOutputFormat;
use crate::formats::output_format_csv::TSVWithNamesOutputFormat;
use crate::formats::FormatFactory;

pub trait OutputFormat: Send {
    fn serialize_block(&mut self, _data_block: &DataBlock) -> Result<Vec<u8>> {
        unimplemented!()
    }

    fn serialize_prefix(&self) -> Result<Vec<u8>> {
        Ok(vec![])
    }

    fn finalize(&mut self) -> Result<Vec<u8>>;
}

#[derive(Clone, Copy, Default)]
pub struct HeaderConfig {
    pub with_name: bool,
    pub with_type: bool,
}

impl HeaderConfig {
    pub fn new(with_name: bool, with_type: bool) -> Self {
        Self {
            with_name,
            with_type,
        }
    }
}

#[derive(Clone, Copy, Debug, EnumIter, Eq, PartialEq)]
pub enum OutputFormatType {
    CSV,
    CSVWithNames,
    CSVWithNamesAndTypes,
    TSV,
    TSVWithNames,
    TSVWithNamesAndTypes,
    Parquet,
    JsonEachRow,
    Values,
}

impl fmt::Display for OutputFormatType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl OutputFormatType {
    pub fn with_names(&self) -> Option<OutputFormatType> {
        match self {
            OutputFormatType::CSV => Some(OutputFormatType::CSVWithNames),
            OutputFormatType::TSV => Some(OutputFormatType::TSVWithNames),
            _ => None,
        }
    }

    pub fn with_names_and_types(&self) -> Option<OutputFormatType> {
        match self {
            OutputFormatType::CSV => Some(OutputFormatType::CSVWithNamesAndTypes),
            OutputFormatType::TSV => Some(OutputFormatType::TSVWithNamesAndTypes),
            _ => None,
        }
    }

    pub fn base_alias(&self) -> Vec<String> {
        match self {
            OutputFormatType::TSV => vec!["TabSeparated".to_string()],
            OutputFormatType::JsonEachRow => vec!["NDJson".to_string()],
            _ => vec![],
        }
    }

    pub fn get_content_type(&self) -> String {
        match self {
            OutputFormatType::TSV
            | OutputFormatType::TSVWithNames
            | OutputFormatType::TSVWithNamesAndTypes => "text/tab-separated-values; charset=UTF-8",
            OutputFormatType::CSV => "text/csv; charset=UTF-8; header=absent",
            OutputFormatType::CSVWithNames | OutputFormatType::CSVWithNamesAndTypes => {
                "text/csv; charset=UTF-8; header=present"
            }
            OutputFormatType::Parquet => "application/octet-stream",
            OutputFormatType::JsonEachRow => "application/json; charset=UTF-8",
            _ => "text/plain; charset=UTF-8",
        }
        .to_string()
    }
}

impl OutputFormatType {
    pub fn create_format(
        &self,
        schema: DataSchemaRef,
        format_setting: FormatSettings,
    ) -> Box<dyn OutputFormat> {
        match self {
            OutputFormatType::TSV => Box::new(TSVOutputFormat::create(schema, format_setting)),
            OutputFormatType::TSVWithNames => {
                Box::new(TSVWithNamesOutputFormat::create(schema, format_setting))
            }
            OutputFormatType::TSVWithNamesAndTypes => Box::new(
                TSVWithNamesAndTypesOutputFormat::create(schema, format_setting),
            ),
            OutputFormatType::CSV => Box::new(CSVOutputFormat::create(schema, format_setting)),
            OutputFormatType::CSVWithNames => {
                Box::new(CSVWithNamesOutputFormat::create(schema, format_setting))
            }
            OutputFormatType::CSVWithNamesAndTypes => Box::new(
                CSVWithNamesAndTypesOutputFormat::create(schema, format_setting),
            ),
            OutputFormatType::Parquet => {
                Box::new(ParquetOutputFormat::create(schema, format_setting))
            }
            OutputFormatType::JsonEachRow => {
                Box::new(JsonEachRowOutputFormat::create(schema, format_setting))
            }
            OutputFormatType::Values => {
                Box::new(ValuesOutputFormat::create(schema, format_setting))
            }
        }
    }
}

impl Default for OutputFormatType {
    fn default() -> Self {
        Self::TSV
    }
}

impl FromStr for OutputFormatType {
    type Err = ErrorCode;
    fn from_str(s: &str) -> std::result::Result<Self, ErrorCode> {
        FormatFactory::instance().get_output(s)
    }
}
