// Copyright 2023 Datafuse Labs.
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

use databend_common_meta_app as mt;
use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::StageFileCompression;
use fastrace::func_name;

use crate::common;

// These bytes are built when a new version in introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
//
#[test]
fn test_decode_v59_csv_file_format_params() -> anyhow::Result<()> {
    let file_format_params_v59 = vec![
        18, 35, 8, 1, 16, 1, 26, 2, 102, 100, 34, 2, 114, 100, 42, 3, 110, 97, 110, 50, 1, 92, 58,
        1, 39, 66, 2, 92, 78, 72, 1, 160, 6, 59, 168, 6, 24,
    ];
    let want = || {
        mt::principal::FileFormatParams::Csv(CsvFileFormatParams {
            compression: StageFileCompression::Gzip,
            headers: 1,
            output_header: false,
            field_delimiter: "fd".to_string(),
            record_delimiter: "rd".to_string(),
            null_display: "\\N".to_string(),
            nan_display: "nan".to_string(),
            escape: "\\".to_string(),
            quote: "\'".to_string(),
            error_on_column_count_mismatch: false,
            empty_field_as: Default::default(),
            binary_format: Default::default(),
            geometry_format: Default::default(),
        })
    };
    common::test_load_old(func_name!(), file_format_params_v59.as_slice(), 0, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}
