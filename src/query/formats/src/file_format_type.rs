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

use chrono_tz::Tz;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_meta_types::FileFormatOptions;
use common_meta_types::StageFileCompression;
use common_meta_types::StageFileFormatType;
use common_settings::Settings;

use super::clickhouse::ClickhouseSuffix;
use crate::delimiter::RecordDelimiter;
use crate::format_option_checker::get_format_option_checker;
use crate::output_format::CSVOutputFormat;
use crate::output_format::CSVWithNamesAndTypesOutputFormat;
use crate::output_format::CSVWithNamesOutputFormat;
use crate::output_format::NDJSONOutputFormatBase;
use crate::output_format::OutputFormat;
use crate::output_format::ParquetOutputFormat;
use crate::output_format::TSVOutputFormat;
use crate::output_format::TSVWithNamesAndTypesOutputFormat;
use crate::output_format::TSVWithNamesOutputFormat;
use crate::ClickhouseFormatType;

pub trait FileFormatTypeExt {
    fn get_ext_from_stage(
        stage: FileFormatOptions,
        settings: &Settings,
    ) -> Result<FileFormatOptionsExt>;

    fn get_file_format_options_from_setting(
        &self,
        settings: &Settings,
        clickhouse_suffix: Option<ClickhouseSuffix>,
    ) -> Result<FileFormatOptionsExt>;

    fn final_file_format_options(
        &self,
        options: &FileFormatOptionsExt,
    ) -> Result<FileFormatOptionsExt>;

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

    pub fn get_output_format_from_settings_clickhouse(
        typ: ClickhouseFormatType,
        schema: DataSchemaRef,
        settings: &Settings,
    ) -> Result<Box<dyn OutputFormat>> {
        let options = typ
            .typ
            .get_file_format_options_from_setting(settings, Some(typ.suffixes))?;
        options.get_output_format(schema)
    }

    pub fn get_output_format_from_settings(
        format: StageFileFormatType,
        schema: DataSchemaRef,
        settings: &Settings,
    ) -> Result<Box<dyn OutputFormat>> {
        let options = format.get_file_format_options_from_setting(settings, None)?;
        options.get_output_format(schema)
    }

    pub fn get_output_format_from_options(
        schema: DataSchemaRef,
        options: FileFormatOptions,
        settings: &Settings,
    ) -> Result<Box<dyn OutputFormat>> {
        let options = StageFileFormatType::get_ext_from_stage(options, settings)?;
        options.get_output_format(schema)
    }

    fn get_output_format(&self, schema: DataSchemaRef) -> Result<Box<dyn OutputFormat>> {
        let fmt = &self.stage.format;
        let options = fmt.final_file_format_options(self)?;
        // println!("format {:?} {:?} {:?}", fmt, options, format_settings);
        let output: Box<dyn OutputFormat> = match options.stage.format {
            StageFileFormatType::Csv => match options.headers {
                0 => Box::new(CSVOutputFormat::create(schema, &options)),
                1 => Box::new(CSVWithNamesOutputFormat::create(schema, &options)),
                2 => Box::new(CSVWithNamesAndTypesOutputFormat::create(schema, &options)),
                _ => unreachable!(),
            },
            StageFileFormatType::Tsv => match options.headers {
                0 => Box::new(TSVOutputFormat::create(schema, &options)),
                1 => Box::new(TSVWithNamesOutputFormat::create(schema, &options)),
                2 => Box::new(TSVWithNamesAndTypesOutputFormat::create(schema, &options)),
                _ => unreachable!(),
            },
            StageFileFormatType::NdJson => {
                match (options.headers, options.json_strings, options.json_compact) {
                    // string, compact, name, type
                    // not compact
                    (0, false, false) => Box::new(NDJSONOutputFormatBase::<
                        false,
                        false,
                        false,
                        false,
                    >::create(schema, &options)),
                    (0, true, false) => {
                        Box::new(NDJSONOutputFormatBase::<true, false, false, false>::create(
                            schema, &options,
                        ))
                    }
                    // compact
                    (0, false, true) => {
                        Box::new(NDJSONOutputFormatBase::<false, true, false, false>::create(
                            schema, &options,
                        ))
                    }
                    (0, true, true) => {
                        Box::new(NDJSONOutputFormatBase::<true, true, false, false>::create(
                            schema, &options,
                        ))
                    }
                    (1, false, true) => {
                        Box::new(NDJSONOutputFormatBase::<false, true, true, false>::create(
                            schema, &options,
                        ))
                    }
                    (1, true, true) => Box::new(
                        NDJSONOutputFormatBase::<true, true, true, false>::create(schema, &options),
                    ),
                    (2, false, true) => Box::new(
                        NDJSONOutputFormatBase::<false, true, true, true>::create(schema, &options),
                    ),
                    (2, true, true) => Box::new(
                        NDJSONOutputFormatBase::<true, true, true, true>::create(schema, &options),
                    ),
                    _ => unreachable!(),
                }
            }
            StageFileFormatType::Parquet => Box::new(ParquetOutputFormat::create(schema, &options)),
            StageFileFormatType::Json => {
                unreachable!()
            }
            StageFileFormatType::Avro => {
                unreachable!()
            }
            StageFileFormatType::Orc => {
                unreachable!()
            }
            StageFileFormatType::Xml => {
                unreachable!()
            }
        };
        Ok(output)
    }
}

impl FileFormatTypeExt for StageFileFormatType {
    fn get_ext_from_stage(
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

    fn get_file_format_options_from_setting(
        &self,
        settings: &Settings,
        clickhouse_suffix: Option<ClickhouseSuffix>,
    ) -> Result<FileFormatOptionsExt> {
        let timezone = parse_timezone(settings)?;

        let stage = FileFormatOptions {
            format: self.clone(),
            skip_header: settings.get_format_skip_header()?,
            field_delimiter: settings.get_format_field_delimiter()?,
            record_delimiter: settings.get_format_record_delimiter()?,
            nan_display: settings.get_format_nan_display()?,
            escape: settings.get_format_escape()?,
            compression: StageFileCompression::from_str(&settings.get_format_compression()?)
                .map_err_to_code(
                    ErrorCode::InvalidArgument,
                    || "get_file_format_options_from_setting",
                )?,
            row_tag: settings.get_row_tag()?,
            quote: settings.get_format_quote()?,
        };
        let mut options = FileFormatOptionsExt {
            stage,
            ident_case_sensitive: settings.get_unquoted_ident_case_sensitive()?,
            headers: 0,
            json_compact: false,
            json_strings: false,
            timezone,
        };
        if let Some(suf) = clickhouse_suffix {
            options.headers = suf.headers;
            if let Some(json) = &suf.json {
                options.json_compact = json.is_compact;
                options.json_strings = json.is_strings;
            }
        }
        Ok(options)
    }

    fn final_file_format_options(
        &self,
        options: &FileFormatOptionsExt,
    ) -> Result<FileFormatOptionsExt> {
        let mut options = options.to_owned();
        let checker = get_format_option_checker(&options.stage.format)?;
        checker.check_options(&mut options)?;
        Ok(options)
    }

    fn get_content_type(&self) -> String {
        match self {
            StageFileFormatType::Tsv => "text/tab-separated-values; charset=UTF-8",
            StageFileFormatType::Csv => "text/csv; charset=UTF-8",
            StageFileFormatType::Parquet => "application/octet-stream",
            StageFileFormatType::NdJson => "application/x-ndjson; charset=UTF-8",
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
