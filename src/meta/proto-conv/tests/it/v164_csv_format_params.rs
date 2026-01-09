// Copyright 2025 Datafuse Labs.
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

use databend_common_io::GeometryDataType;
use databend_common_meta_app::principal::BinaryFormat;
use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::EmptyFieldAs;
use databend_common_meta_app::principal::StageFileCompression;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v164_csv_file_format_params() -> anyhow::Result<()> {
    let csv_file_format_params_v164 = vec![
        8, 11, 16, 2, 26, 1, 124, 34, 1, 10, 42, 8, 110, 97, 110, 95, 117, 116, 102, 56, 50, 1, 92,
        58, 1, 34, 66, 2, 92, 78, 82, 4, 78, 85, 76, 76, 90, 3, 104, 101, 120, 96, 1, 106, 4, 69,
        87, 75, 66, 112, 1, 122, 13, 70, 73, 69, 76, 68, 95, 68, 69, 70, 65, 85, 76, 84, 160, 6,
        164, 1, 168, 6, 24,
    ];
    let want = || CsvFileFormatParams {
        compression: StageFileCompression::Zip,
        headers: 2,
        output_header: true,
        field_delimiter: "|".to_string(),
        record_delimiter: "\n".to_string(),
        null_display: "\\N".to_string(),
        nan_display: "nan_utf8".to_string(),
        escape: "\\".to_string(),
        quote: "\"".to_string(),
        error_on_column_count_mismatch: true,
        allow_quoted_nulls: true,
        empty_field_as: EmptyFieldAs::Null,
        quoted_empty_field_as: EmptyFieldAs::FieldDefault,
        binary_format: BinaryFormat::Hex,
        geometry_format: GeometryDataType::EWKB,
    };

    common::test_load_old(
        func_name!(),
        csv_file_format_params_v164.as_slice(),
        164,
        want(),
    )?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}
