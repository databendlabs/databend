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

use common_exception::Result;
use common_expression::TableSchemaRef;
use common_formats::output_format::OutputFormat;
use common_formats::ClickhouseFormatType;
use common_formats::FileFormatOptionsExt;
use common_settings::Settings;

mod field_encoder;
mod output_format_json_each_row;
mod output_format_tcsv;
mod output_format_utils;

fn get_output_format_clickhouse(
    format_name: &str,
    schema: TableSchemaRef,
) -> Result<Box<dyn OutputFormat>> {
    let format = ClickhouseFormatType::parse_clickhouse_format(format_name)?;
    let settings = Settings::create("default".to_string());
    FileFormatOptionsExt::get_output_format_from_clickhouse_format(format, schema, &settings)
}
