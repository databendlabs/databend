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

use databend_common_meta_app::principal::NdJsonFileFormatParams;
use databend_common_meta_app::principal::NullAs;
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
fn test_decode_v83_ndjson_file_format_params() -> anyhow::Result<()> {
    let nd_json_file_format_params_v83 = vec![
        8, 1, 18, 13, 70, 73, 69, 76, 68, 95, 68, 69, 70, 65, 85, 76, 84, 26, 13, 70, 73, 69, 76,
        68, 95, 68, 69, 70, 65, 85, 76, 84, 34, 0, 160, 6, 83, 168, 6, 24,
    ];
    let want = || NdJsonFileFormatParams {
        compression: StageFileCompression::Gzip,
        missing_field_as: NullAs::FieldDefault,
        null_field_as: NullAs::FieldDefault,
        null_if: vec!["".to_string()],
    };
    common::test_load_old(
        func_name!(),
        nd_json_file_format_params_v83.as_slice(),
        83,
        want(),
    )?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}
