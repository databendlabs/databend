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

use chrono_tz::Tz;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableSchemaRef;
use common_meta_app::principal::FileFormatOptions;
use common_meta_app::principal::StageFileFormatType;
use common_settings::Settings;

use crate::delimiter::RecordDelimiter;
use crate::format_option_checker::get_format_option_checker;
use crate::output_format::CSVOutputFormat;
use crate::output_format::CSVWithNamesAndTypesOutputFormat;
use crate::output_format::CSVWithNamesOutputFormat;
use crate::output_format::JSONOutputFormat;
use crate::output_format::NDJSONOutputFormatBase;
use crate::output_format::OutputFormat;
use crate::output_format::ParquetOutputFormat;
use crate::output_format::TSVOutputFormat;
use crate::output_format::TSVWithNamesAndTypesOutputFormat;
use crate::output_format::TSVWithNamesOutputFormat;
use crate::ClickhouseFormatType;

pub trait FileFormatTypeExt {
    fn get_content_type(&self) -> String;
}

#[derive(Clone, Debug)]
pub struct FileFormatOptionsExt {
    pub stage: FileFormatOptions,
    pub ident_case_sensitive: bool,
    pub headers: usize,
    pub json_compact: bool,
    pub json_strings: bool,
    pub timezone: Tz,
}

impl FileFormatOptionsExt {
    pub fn create_from_file_format_options(
        stage: FileFormatOptions,
        settings: &Settings,
    ) -> Result<FileFormatOptionsExt> {
        let timezone = parse_timezone(settings)?;
        let options = FileFormatOptionsExt {
            stage,
            ident_case_sensitive: false,
            headers: 0,
            json_compact: false,
            json_strings: false,
            timezone,
        };
        Ok(options)
    }

    pub fn create_from_clickhouse_format(
        clickhouse_type: ClickhouseFormatType,
        settings: &Settings,
    ) -> Result<FileFormatOptionsExt> {
        let timezone = parse_timezone(settings)?;

        let mut stage = FileFormatOptions::new();
        stage.format = clickhouse_type.typ;
        let mut options = FileFormatOptionsExt {
            stage,
            ident_case_sensitive: settings.get_unquoted_ident_case_sensitive()?,
            headers: 0,
            json_compact: false,
            json_strings: false,
            timezone,
        };
        let suf = &clickhouse_type.suffixes;
        options.headers = suf.headers;
        if let Some(json) = &suf.json {
            options.json_compact = json.is_compact;
            options.json_strings = json.is_strings;
        }
        Ok(options)
    }

    pub fn check(&mut self) -> Result<()> {
        let checker = get_format_option_checker(&self.stage.format)?;
        checker.check_options(self)
    }

    pub fn get_quote_char(&self) -> u8 {
        self.stage.quote.as_bytes()[0]
    }

    pub fn get_field_delimiter(&self) -> u8 {
        let fd = &self.stage.field_delimiter;
        if fd.is_empty() {
            0 // dummy
        } else {
            fd.as_bytes()[0]
        }
    }

    pub fn get_record_delimiter(&self) -> Result<RecordDelimiter> {
        let fd = &self.stage.record_delimiter;
        if fd.is_empty() {
            Ok(RecordDelimiter::Any(0)) // dummy
        } else {
            RecordDelimiter::try_from(fd.as_bytes())
        }
    }

    pub fn get_output_format_from_clickhouse_format(
        typ: ClickhouseFormatType,
        schema: TableSchemaRef,
        settings: &Settings,
    ) -> Result<Box<dyn OutputFormat>> {
        let mut options = FileFormatOptionsExt::create_from_clickhouse_format(typ, settings)?;
        options.get_output_format(schema)
    }

    pub fn get_output_format_from_format_options(
        schema: TableSchemaRef,
        options: FileFormatOptions,
        settings: &Settings,
    ) -> Result<Box<dyn OutputFormat>> {
        let mut options = FileFormatOptionsExt::create_from_file_format_options(options, settings)?;
        options.get_output_format(schema)
    }

    pub fn get_output_format(&mut self, schema: TableSchemaRef) -> Result<Box<dyn OutputFormat>> {
        self.check()?;
        // println!("format {:?} {:?} {:?}", fmt, options, format_settings);
        let output: Box<dyn OutputFormat> = match &self.stage.format {
            StageFileFormatType::Csv => match self.headers {
                0 => Box::new(CSVOutputFormat::create(schema, self)),
                1 => Box::new(CSVWithNamesOutputFormat::create(schema, self)),
                2 => Box::new(CSVWithNamesAndTypesOutputFormat::create(schema, self)),
                _ => unreachable!(),
            },
            StageFileFormatType::Tsv => match self.headers {
                0 => Box::new(TSVOutputFormat::create(schema, self)),
                1 => Box::new(TSVWithNamesOutputFormat::create(schema, self)),
                2 => Box::new(TSVWithNamesAndTypesOutputFormat::create(schema, self)),
                _ => unreachable!(),
            },
            StageFileFormatType::NdJson => {
                match (self.headers, self.json_strings, self.json_compact) {
                    // string, compact, name, type
                    // not compact
                    (0, false, false) => Box::new(NDJSONOutputFormatBase::<
                        false,
                        false,
                        false,
                        false,
                    >::create(schema, self)),
                    (0, true, false) => Box::new(
                        NDJSONOutputFormatBase::<true, false, false, false>::create(schema, self),
                    ),
                    // compact
                    (0, false, true) => Box::new(
                        NDJSONOutputFormatBase::<false, true, false, false>::create(schema, self),
                    ),
                    (0, true, true) => Box::new(
                        NDJSONOutputFormatBase::<true, true, false, false>::create(schema, self),
                    ),
                    (1, false, true) => Box::new(
                        NDJSONOutputFormatBase::<false, true, true, false>::create(schema, self),
                    ),
                    (1, true, true) => Box::new(
                        NDJSONOutputFormatBase::<true, true, true, false>::create(schema, self),
                    ),
                    (2, false, true) => Box::new(
                        NDJSONOutputFormatBase::<false, true, true, true>::create(schema, self),
                    ),
                    (2, true, true) => Box::new(
                        NDJSONOutputFormatBase::<true, true, true, true>::create(schema, self),
                    ),
                    _ => unreachable!(),
                }
            }
            StageFileFormatType::Parquet => Box::new(ParquetOutputFormat::create(schema, self)),
            StageFileFormatType::Json => Box::new(JSONOutputFormat::create(schema, self)),
            others => {
                return Err(ErrorCode::InvalidArgument(format!(
                    "Unsupported output file format:{:?}",
                    others.to_string()
                )));
            }
        };
        Ok(output)
    }
}

impl FileFormatTypeExt for StageFileFormatType {
    fn get_content_type(&self) -> String {
        match self {
            StageFileFormatType::Tsv => "text/tab-separated-values; charset=UTF-8",
            StageFileFormatType::Csv => "text/csv; charset=UTF-8",
            StageFileFormatType::Parquet => "application/octet-stream",
            StageFileFormatType::NdJson => "application/x-ndjson; charset=UTF-8",
            StageFileFormatType::Json => "application/json; charset=UTF-8",
            _ => "text/plain; charset=UTF-8",
        }
        .to_string()
    }
}

pub fn parse_timezone(settings: &Settings) -> Result<Tz> {
    let tz = settings.get_timezone()?;
    tz.parse::<Tz>()
        .map_err(|_| ErrorCode::InvalidTimezone("Timezone has been checked and should be valid"))
}
