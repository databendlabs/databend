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
use common_io::consts::*;
use common_io::prelude::FormatSettings;
use common_meta_types::FileFormatOptions;
use common_meta_types::StageFileCompression;
use common_meta_types::StageFileFormatType;
use common_settings::Settings;

use super::clickhouse::ClickhouseSuffix;
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

    fn get_format_settings(
        &self,
        final_options: &FileFormatOptionsExt,
        settings: &Settings,
    ) -> Result<FormatSettings>;

    fn get_content_type(&self) -> String;
}

#[derive(Clone, Debug)]
pub struct FileFormatOptionsExt {
    pub stage: FileFormatOptions,
    pub quote: String,
    pub ident_case_sensitive: bool,
    pub headers: usize,
    pub json_compact: bool,
    pub json_strings: bool,
    pub timezone: Tz,
}

impl FileFormatOptionsExt {
    pub fn get_quote_char(&self) -> u8 {
        self.quote.as_bytes()[0]
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
            quote: "".to_string(),
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
            escape: settings.get_format_escape()?,
            compression: StageFileCompression::from_str(&settings.get_format_compression()?)
                .map_err_to_code(
                    ErrorCode::InvalidArgument,
                    || "get_file_format_options_from_setting",
                )?,
            row_tag: settings.get_row_tag()?,
        };
        let mut options = FileFormatOptionsExt {
            stage,
            quote: settings.get_format_quote()?,
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
        check_options(&mut options.stage)?;

        match self {
            StageFileFormatType::Csv => final_csv_options(&mut options)?,
            StageFileFormatType::Tsv => final_tsv_options(&mut options)?,
            StageFileFormatType::Json => {}
            StageFileFormatType::NdJson => {}
            StageFileFormatType::Avro => {}
            StageFileFormatType::Orc => {}
            StageFileFormatType::Parquet => final_tsv_options(&mut options)?,
            StageFileFormatType::Xml => {}
        }
        Ok(options)
    }

    fn get_format_settings(
        &self,
        options: &FileFormatOptionsExt,
        settings: &Settings,
    ) -> Result<FormatSettings> {
        let tz = parse_timezone(settings)?;
        let format_setting = match self {
            StageFileFormatType::Csv => format_setting_csv(options, tz),
            StageFileFormatType::Tsv => format_setting_tsv(options, tz),
            StageFileFormatType::Json => {
                unreachable!()
            }
            StageFileFormatType::NdJson => format_setting_ndjson(options, tz),
            StageFileFormatType::Avro => {
                unreachable!()
            }
            StageFileFormatType::Orc => {
                unreachable!()
            }
            StageFileFormatType::Parquet => format_setting_parquet(options, tz),
            StageFileFormatType::Xml => format_setting_xml(options, tz),
        };
        Ok(format_setting)
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

fn check_options(options: &mut FileFormatOptions) -> Result<()> {
    if options.escape.len() > 1 {
        return Err(ErrorCode::InvalidArgument(
            "escape can only contain one char",
        ));
    };

    if options.field_delimiter.len() > 1 {
        return Err(ErrorCode::InvalidArgument(
            "field_delimiter can only contain one char",
        ));
    };

    if options.record_delimiter.len() > 1 && options.record_delimiter.as_str() != "\r\n" {
        return Err(ErrorCode::InvalidArgument(
            "record_delimiter can only contain one char except '\r\n'",
        ));
    };

    if options.record_delimiter.is_empty() {
        options.record_delimiter = '\n'.to_string();
    }

    Ok(())
}

// todo(youngsofun): return error for unused options after we support NONE.
fn final_csv_options(options: &mut FileFormatOptionsExt) -> Result<()> {
    if options.stage.field_delimiter.is_empty() {
        options.stage.field_delimiter = ','.to_string();
    }
    if options.quote.is_empty() {
        options.quote = "\"".to_string();
    }
    Ok(())
}

fn final_tsv_options(options: &mut FileFormatOptionsExt) -> Result<()> {
    if options.stage.field_delimiter.is_empty() {
        options.stage.field_delimiter = '\t'.to_string();
    }
    if options.quote.is_empty() {
        options.quote = "\'".to_string();
    }
    Ok(())
}

fn format_setting_csv(options: &FileFormatOptionsExt, timezone: Tz) -> FormatSettings {
    FormatSettings {
        timezone,
        nested: Default::default(),
        true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
        false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
        nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
        inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
        null_bytes: NULL_BYTES_ESCAPE.as_bytes().to_vec(),
        quote_char: options.quote.as_bytes()[0],
        escape: FormatSettings::parse_escape(&options.stage.escape, None),
        record_delimiter: options.stage.record_delimiter.as_bytes().to_vec(),
        field_delimiter: options.stage.field_delimiter.as_bytes().to_vec(),

        // not used
        empty_as_default: true,
        json_quote_denormals: false,
        json_escape_forward_slashes: true,
        ident_case_sensitive: false,
        row_tag: vec![],
    }
}

fn format_setting_tsv(options: &FileFormatOptionsExt, timezone: Tz) -> FormatSettings {
    FormatSettings {
        timezone,
        nested: Default::default(),
        true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
        false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
        nan_bytes: NAN_BYTES_LOWER.as_bytes().to_vec(),
        inf_bytes: INF_BYTES_LOWER.as_bytes().to_vec(),
        null_bytes: NULL_BYTES_ESCAPE.as_bytes().to_vec(),
        quote_char: options.quote.as_bytes()[0],
        escape: FormatSettings::parse_escape(&options.stage.escape, Some(b'\\')),
        record_delimiter: options.stage.record_delimiter.as_bytes().to_vec(),
        field_delimiter: options.stage.field_delimiter.as_bytes().to_vec(),

        // not used
        empty_as_default: true,
        json_quote_denormals: false,
        json_escape_forward_slashes: true,
        ident_case_sensitive: false,
        row_tag: vec![],
    }
}

fn format_setting_ndjson(options: &FileFormatOptionsExt, timezone: Tz) -> FormatSettings {
    FormatSettings {
        timezone,
        nested: Default::default(),
        true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
        false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
        nan_bytes: NULL_BYTES_LOWER.as_bytes().to_vec(),
        inf_bytes: NULL_BYTES_LOWER.as_bytes().to_vec(),
        null_bytes: NULL_BYTES_LOWER.as_bytes().to_vec(),
        quote_char: b'\"',
        escape: FormatSettings::parse_escape(&options.stage.escape, Some(b'\\')),
        record_delimiter: vec![b'\n'],
        field_delimiter: vec![b','],

        // not used
        empty_as_default: true,
        json_quote_denormals: false,
        json_escape_forward_slashes: true,
        ident_case_sensitive: options.ident_case_sensitive,
        row_tag: vec![],
    }
}

fn format_setting_xml(options: &FileFormatOptionsExt, timezone: Tz) -> FormatSettings {
    FormatSettings {
        timezone,
        nested: Default::default(),
        true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
        false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
        nan_bytes: NULL_BYTES_LOWER.as_bytes().to_vec(),
        inf_bytes: NULL_BYTES_LOWER.as_bytes().to_vec(),
        null_bytes: NULL_BYTES_LOWER.as_bytes().to_vec(),
        ident_case_sensitive: options.ident_case_sensitive,
        row_tag: options.stage.row_tag.clone().into_bytes(),

        // not used
        empty_as_default: true,
        json_quote_denormals: false,
        json_escape_forward_slashes: true,
        quote_char: b'\"',
        escape: FormatSettings::parse_escape(&options.stage.escape, Some(b'\\')),
        record_delimiter: vec![b'\n'],
        field_delimiter: vec![b','],
    }
}

fn format_setting_parquet(options: &FileFormatOptionsExt, timezone: Tz) -> FormatSettings {
    // dummy
    format_setting_csv(options, timezone)
}

pub fn parse_timezone(settings: &Settings) -> Result<Tz> {
    let tz = settings.get_timezone()?;
    tz.parse::<Tz>()
        .map_err(|_| ErrorCode::InvalidTimezone("Timezone has been checked and should be valid"))
}
