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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_io::prelude::OutputFormatSettings;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageFileFormatType;

use crate::clickhouse::ClickhouseSuffix;
use crate::field_encoder::FieldEncoderCSV;
use crate::output_format::CSVOutputFormat;
use crate::output_format::JSONOutputFormat;
use crate::output_format::NDJSONOutputFormatBase;
use crate::output_format::OutputFormat;
use crate::output_format::ParquetOutputFormat;
use crate::output_format::TSVOutputFormat;

pub trait FileFormatTypeExt {
    fn get_content_type(&self) -> String;
}

pub fn get_output_format(
    schema: TableSchemaRef,
    params: FileFormatParams,
    settings: OutputFormatSettings,
    clickhouse: Option<ClickhouseSuffix>,
) -> Result<Box<dyn OutputFormat>> {
    let output: Box<dyn OutputFormat> = match &params {
        FileFormatParams::Csv(params) => {
            let field_encoder = FieldEncoderCSV::create_csv(params, settings.clone());
            let headers = if let Some(options) = &clickhouse {
                options.headers
            } else if params.output_header {
                1
            } else {
                0
            };
            Box::new(CSVOutputFormat::create(
                schema,
                params,
                field_encoder,
                headers,
            ))
        }
        FileFormatParams::Tsv(params) => {
            let field_encoder = FieldEncoderCSV::create_tsv(params, settings.clone());
            let headers = if let Some(options) = &clickhouse {
                options.headers
            } else {
                0
            };
            Box::new(TSVOutputFormat::create(
                schema,
                params,
                field_encoder,
                headers,
            ))
        }
        FileFormatParams::NdJson(_params) => {
            let options = clickhouse.clone().unwrap_or_default();
            let headers = options.headers;
            match (options.is_strings, options.is_compact) {
                // string, compact, name, type
                // not compact
                (false, false) => Box::new(NDJSONOutputFormatBase::<false, false>::create(
                    schema, settings, headers,
                )),
                (true, false) => Box::new(NDJSONOutputFormatBase::<true, false>::create(
                    schema, settings, headers,
                )),
                // compact
                (false, true) => Box::new(NDJSONOutputFormatBase::<false, true>::create(
                    schema, settings, headers,
                )),
                (true, true) => Box::new(NDJSONOutputFormatBase::<true, true>::create(
                    schema, settings, headers,
                )),
            }
        }
        FileFormatParams::Parquet(_) => Box::new(ParquetOutputFormat::create(schema)),
        FileFormatParams::Json(_) => Box::new(JSONOutputFormat::create(schema, settings)),
        others => {
            return Err(ErrorCode::InvalidArgument(format!(
                "Unsupported output file format:{:?}",
                others.to_string()
            )));
        }
    };
    Ok(output)
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
