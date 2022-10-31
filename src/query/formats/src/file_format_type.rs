use std::str::FromStr;

use chrono_tz::Tz;
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

pub trait FileFormatTypeExt {
    fn get_ext_from_stage(stage: FileFormatOptions) -> FileFormatOptionsExt;

    fn get_file_format_options_from_setting(
        &self,
        settings: &Settings,
        clickhouse_suffix: ClickhouseSuffix,
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
    stage: FileFormatOptions,
    quote: u8,
    ident_case_sensitive: bool,
    headers: usize,
    json_compact: bool,
    json_strings: bool,
}

impl FileFormatTypeExt for StageFileFormatType {
    fn get_ext_from_stage(stage: FileFormatOptions) -> FileFormatOptionsExt {
        FileFormatOptionsExt {
            stage,
            quote: 0,
            ident_case_sensitive: false,
            headers: 0,
            json_compact: false,
            json_strings: false,
        }
    }

    fn get_file_format_options_from_setting(
        &self,
        settings: &Settings,
        clickhouse_suffix: Option<ClickhouseSuffix>,
    ) -> Result<FileFormatOptionsExt> {
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
        };
        let mut options = FileFormatOptionsExt {
            stage,
            quote: FormatSettings::parse_quote(&settings.get_format_quote_char()?)?,
            ident_case_sensitive: settings.get_unquoted_ident_case_sensitive()?,
            headers: 0,
            json_compact: false,
            json_strings: false,
        };
        if let Some(suf) = clickhouse_suffix {
            options.headers = suf.headersh;
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
        let tz = settings.get_timezone()?;
        let tz = tz.parse::<Tz>().map_err(|_| {
            ErrorCode::InvalidTimezone("Timezone has been checked and should be valid")
        })?;
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
            StageFileFormatType::Xml => {
                unreachable!()
            }
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

    if !["", "\n", "\r\n"].contains(&options.record_delimiter.as_str()) {
        return Err(ErrorCode::InvalidArgument(
            "currently record_delimiter can only be '\r\n' or '\n'",
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
    Ok(())
}

fn final_tsv_options(options: &mut FileFormatOptionsExt) -> Result<()> {
    if options.stage.field_delimiter.is_empty() {
        options.stage.field_delimiter = '\t'.to_string();
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
        quote_char: options.quote,
        escape: FormatSettings::parse_escape(&options.stage.escape, None),
        record_delimiter: options.stage.record_delimiter.as_bytes().to_vec(),
        field_delimiter: options.stage.field_delimiter.as_bytes().to_vec(),

        // not used
        empty_as_default: true,
        json_quote_denormals: false,
        json_escape_forward_slashes: true,
        ident_case_sensitive: false,
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
        quote_char: 0, // not allowed for now
        escape: FormatSettings::parse_escape(&options.stage.escape, Some(b'\\')),
        record_delimiter: options.stage.record_delimiter.as_bytes().to_vec(),
        field_delimiter: options.stage.field_delimiter.as_bytes().to_vec(),

        // not used
        empty_as_default: true,
        json_quote_denormals: false,
        json_escape_forward_slashes: true,
        ident_case_sensitive: false,
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
    }
}

fn format_setting_parquet(options: &FileFormatOptionsExt, timezone: Tz) -> FormatSettings {
    // dummy
    format_setting_csv(options, timezone)
}
