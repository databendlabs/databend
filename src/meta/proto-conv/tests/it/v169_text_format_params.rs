// Copyright 2026 Datafuse Labs.
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

use databend_common_meta_app::principal::EmptyFieldAs;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_meta_app::principal::TextFileFormatParams;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v169_text_file_format_params() -> anyhow::Result<()> {
    let text_file_format_params_v169 = vec![
        8, 11, 16, 2, 26, 1, 124, 34, 1, 10, 42, 8, 110, 97, 110, 95, 117, 116, 102, 56, 50, 1, 92,
        58, 1, 34, 66, 4, 78, 85, 76, 76, 72, 1, 82, 13, 70, 73, 69, 76, 68, 95, 68, 69, 70, 65,
        85, 76, 84, 88, 1, 160, 6, 169, 1, 168, 6, 24,
    ];
    let want = || TextFileFormatParams {
        compression: StageFileCompression::Zip,
        headers: 2,
        field_delimiter: "|".to_string(),
        record_delimiter: "\n".to_string(),
        escape: "\\".to_string(),
        quote: "\"".to_string(),
        error_on_column_count_mismatch: false,
        trim_space: false,
        empty_field_as: EmptyFieldAs::FieldDefault,
        output_header: true,
        nan_display: "nan_utf8".to_string(),
        null_display: "NULL".to_string(),
        encoding: "UTF-8".to_string(),
        encoding_error_mode: "strict".to_string(),
    };

    common::test_load_old(
        func_name!(),
        text_file_format_params_v169.as_slice(),
        169,
        want(),
    )?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}
