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

use chrono_tz::UTC;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::types::DataType;
use databend_common_io::GeometryDataType;
use jiff::tz::TimeZone;
use serde_json::Value;

use crate::FileFormatOptionsExt;
use crate::field_decoder::FieldJsonAstDecoder;

fn default_json_options() -> FileFormatOptionsExt {
    FileFormatOptionsExt {
        ident_case_sensitive: false,
        headers: 0,
        json_compact: false,
        json_strings: false,
        disable_variant_check: false,
        timezone: UTC,
        jiff_timezone: TimeZone::UTC,
        is_select: false,
        is_clickhouse: false,
        is_rounding_mode: true,
        geometry_format: GeometryDataType::default(),
        enable_dst_hour_fix: false,
    }
}

pub fn column_from_json_value(data_type: &DataType, json: Value) -> Result<Column> {
    let rows = match json {
        Value::Array(values) => values,
        other => {
            return Err(ErrorCode::BadArguments(format!(
                "from_json! expects a json array to describe column values, got {other:?}"
            )));
        }
    };

    let options = default_json_options();
    let decoder = FieldJsonAstDecoder::create(&options);
    let mut builder = ColumnBuilder::with_capacity(data_type, rows.len());
    for value in rows {
        decoder.read_field(&mut builder, &value)?;
    }
    Ok(builder.build())
}

#[macro_export]
macro_rules! column_from_json {
    ($data_type:expr, $($json:tt)+) => {{
        $crate::column_from_json_value(&$data_type, ::serde_json::json!($($json)+))
            .expect("from_json! expects a valid json literal for the provided type")
    }};
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::*;

    #[test]
    fn test_from_json_macro_strings() {
        let column = column_from_json!(DataType::String, ["a", "b", "c"]);
        assert_eq!(column, StringType::from_data(vec!["a", "b", "c"]));
    }

    #[test]
    fn test_from_json_nullable_booleans() {
        let data_type = DataType::Nullable(Box::new(DataType::Boolean));
        let column = column_from_json!(data_type, [true, null, false]);
        assert_eq!(
            column,
            BooleanType::from_data_with_validity(vec![true, false, false], vec![true, false, true])
        );
    }
}
