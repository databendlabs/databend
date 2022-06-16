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
    fn serialize_block(
        &mut self,
        _data_block: &DataBlock,
        _format_setting: &FormatSettings,
    ) -> Result<Vec<u8>> {
        unimplemented!()
    }

    fn serialize_prefix(&self, _format: &FormatSettings) -> Result<Vec<u8>> {
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
}

impl OutputFormatType {
    pub fn create_format(&self, schema: DataSchemaRef) -> Box<dyn OutputFormat> {
        match self {
            OutputFormatType::TSV => Box::new(TSVOutputFormat::create(schema)),
            OutputFormatType::TSVWithNames => Box::new(TSVWithNamesOutputFormat::create(schema)),
            OutputFormatType::TSVWithNamesAndTypes => {
                Box::new(TSVWithNamesAndTypesOutputFormat::create(schema))
            }
            OutputFormatType::CSV => Box::new(CSVOutputFormat::create(schema)),
            OutputFormatType::CSVWithNames => Box::new(CSVWithNamesOutputFormat::create(schema)),
            OutputFormatType::CSVWithNamesAndTypes => {
                Box::new(CSVWithNamesAndTypesOutputFormat::create(schema))
            }
            OutputFormatType::Parquet => Box::new(ParquetOutputFormat::create(schema)),
            OutputFormatType::JsonEachRow => Box::new(JsonEachRowOutputFormat::create(schema)),
            OutputFormatType::Values => Box::new(ValuesOutputFormat::create(schema)),
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
