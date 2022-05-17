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
use common_exception::Result;
use common_io::prelude::FormatSettings;

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
    // NDJson,
    // Parquet,
}

impl OutputFormatType {
    pub fn with_default_setting(&self) -> Box<dyn OutputFormat> {
        match self {
            OutputFormatType::Tsv => Box::new(TSVOutputFormat::default()),
            OutputFormatType::Csv => Box::new(CSVOutputFormat::default()),
        }
    }

    pub fn is_download_format(&self) -> bool {
        matches!(self, Self::Tsv | Self::Csv)
    }
}

impl Default for OutputFormatType {
    fn default() -> Self {
        Self::Tsv
    }
}

impl FromStr for OutputFormatType {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s.to_uppercase().as_str() {
            "TSV" => Ok(OutputFormatType::Tsv),
            "CSV" => Ok(OutputFormatType::Csv),
            _ => Err("Unknown file format type, must be one of { TSV, CSV }".to_string()),
        }
    }
}
