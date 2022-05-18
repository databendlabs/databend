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

use std::str::FromStr;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::FormatSettings;

use super::output_format_ndjson::NDJsonOutputFormat;
use super::output_format_parquet::ParquetOutputFormat;
use crate::formats::output_format_csv::CSVOutputFormat;
use crate::formats::output_format_csv::TSVOutputFormat;

pub trait OutputFormat: Send {
    fn serialize_block(
        &mut self,
        data_block: &DataBlock,
        format_setting: &FormatSettings,
    ) -> Result<Vec<u8>>;

    fn finalize(&mut self) -> Result<Vec<u8>>;
}

#[derive(Clone, Copy)]
pub enum OutputFormatType {
    Tsv,
    Csv,
    Parquet,
    NDJson,
}

impl OutputFormatType {
    pub fn create_format(&self, schema: DataSchemaRef) -> Box<dyn OutputFormat> {
        match self {
            OutputFormatType::Tsv => Box::new(TSVOutputFormat::create(schema)),
            OutputFormatType::Csv => Box::new(CSVOutputFormat::create(schema)),
            OutputFormatType::Parquet => Box::new(ParquetOutputFormat::create(schema)),
            OutputFormatType::NDJson => Box::new(NDJsonOutputFormat::create(schema)),
        }
    }
}

impl Default for OutputFormatType {
    fn default() -> Self {
        Self::Tsv
    }
}

impl FromStr for OutputFormatType {
    type Err = ErrorCode;
    fn from_str(s: &str) -> std::result::Result<Self, ErrorCode> {
        match s.to_uppercase().as_str() {
            "TSV" => Ok(OutputFormatType::Tsv),
            "CSV" => Ok(OutputFormatType::Csv),
            "NDJSON" | "JSONEACHROW" => Ok(OutputFormatType::NDJson),
            "PARQUET" => Ok(OutputFormatType::Parquet),
            _ => Err(ErrorCode::StrParseError(
                "Unknown file format type, must be one of { TSV, CSV, PARQUET, NDJSON | JSONEACHROW }".to_string(),
            )),
        }
    }
}
