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

use crate::formats::output_format_tsv::TSVOutputFormat;

pub trait OutputFormat: Send + Default {
    fn serialize_block(
        &self,
        data_block: &DataBlock,
        format_setting: &FormatSettings,
    ) -> Result<Vec<u8>>;
}

#[derive(Clone, Copy)]
pub enum OutputFormatType {
    Tsv,
}

impl OutputFormatType {
    pub fn with_default_setting(&self) -> impl OutputFormat {
        match self {
            OutputFormatType::Tsv => TSVOutputFormat::default(),
        }
    }

    pub fn is_download_format(&self) -> bool {
        matches!(self, Self::Tsv)
    }
}

impl OutputFormat for OutputFormatType {
    fn serialize_block(
        &self,
        block: &DataBlock,
        format_setting: &FormatSettings,
    ) -> Result<Vec<u8>> {
        self.with_default_setting()
            .serialize_block(block, format_setting)
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
            _ => Err("Unknown file format type, must be one of { TSV }".to_string()),
        }
    }
}
