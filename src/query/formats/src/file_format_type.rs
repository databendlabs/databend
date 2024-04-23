// Copyright 2021 Datafuse Labs
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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_io::GeometryDataType;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_settings::Settings;

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
    pub ident_case_sensitive: bool,
    pub headers: usize,
    pub json_compact: bool,
    pub json_strings: bool,
    pub disable_variant_check: bool,
    pub timezone: Tz,
    pub is_select: bool,
    pub is_clickhouse: bool,
    pub is_rounding_mode: bool,
    pub geometry_format: GeometryDataType,
}

impl FileFormatOptionsExt {
    pub fn create_from_settings(
        settings: &Settings,
        is_select: bool,
    ) -> Result<FileFormatOptionsExt> {
        let timezone = parse_timezone(settings)?;
        let geometry_format = settings.get_geometry_output_format()?;
        let numeric_cast_option = settings
            .get_numeric_cast_option()
            .unwrap_or("rounding".to_string());
        let is_rounding_mode = numeric_cast_option.as_str() == "rounding";

        let options = FileFormatOptionsExt {
            ident_case_sensitive: false,
            headers: 0,
            json_compact: false,
            json_strings: false,
            disable_variant_check: false,
            timezone,
            is_select,
            is_clickhouse: false,
            is_rounding_mode,
            geometry_format,
        };
        Ok(options)
    }

    pub fn create_from_clickhouse_format(
        clickhouse_type: ClickhouseFormatType,
        settings: &Settings,
    ) -> Result<FileFormatOptionsExt> {
        let timezone = parse_timezone(settings)?;
        let geometry_format = settings.get_geometry_output_format()?;
        let mut options = FileFormatOptionsExt {
            ident_case_sensitive: settings.get_unquoted_ident_case_sensitive()?,
            headers: 0,
            json_compact: false,
            json_strings: false,
            disable_variant_check: false,
            timezone,
            is_select: false,
            is_clickhouse: true,
            is_rounding_mode: true,
            geometry_format,
        };
        let suf = &clickhouse_type.suffixes;
        options.headers = suf.headers;
        if let Some(json) = &suf.json {
            options.json_compact = json.is_compact;
            options.json_strings = json.is_strings;
        }
        Ok(options)
    }

    pub fn get_output_format_from_clickhouse_format(
        typ: ClickhouseFormatType,
        schema: TableSchemaRef,
        settings: &Settings,
    ) -> Result<Box<dyn OutputFormat>> {
        let params = FileFormatParams::default_by_type(typ.typ.clone())?;
        let mut options = FileFormatOptionsExt::create_from_clickhouse_format(typ, settings)?;
        options.get_output_format(schema, params)
    }

    pub fn get_output_format(
        &mut self,
        schema: TableSchemaRef,
        params: FileFormatParams,
    ) -> Result<Box<dyn OutputFormat>> {
        let output: Box<dyn OutputFormat> = match &params {
            FileFormatParams::Csv(params) => {
                if self.is_clickhouse {
                    match self.headers {
                        0 => Box::new(CSVOutputFormat::create(schema, params, self)),
                        1 => Box::new(CSVWithNamesOutputFormat::create(schema, params, self)),
                        2 => Box::new(CSVWithNamesAndTypesOutputFormat::create(
                            schema, params, self,
                        )),
                        _ => unreachable!(),
                    }
                } else if params.output_header {
                    Box::new(CSVWithNamesOutputFormat::create(schema, params, self))
                } else {
                    Box::new(CSVOutputFormat::create(schema, params, self))
                }
            }
            FileFormatParams::Tsv(params) => match self.headers {
                0 => Box::new(TSVOutputFormat::create(schema, params, self)),
                1 => Box::new(TSVWithNamesOutputFormat::create(schema, params, self)),
                2 => Box::new(TSVWithNamesAndTypesOutputFormat::create(
                    schema, params, self,
                )),
                _ => unreachable!(),
            },
            FileFormatParams::NdJson(_params) => {
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
            FileFormatParams::Parquet(_) => Box::new(ParquetOutputFormat::create(schema, self)),
            FileFormatParams::Json(_) => Box::new(JSONOutputFormat::create(schema, self)),
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
