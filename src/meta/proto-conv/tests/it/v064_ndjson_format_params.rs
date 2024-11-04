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
fn test_decode_v64_ndjson_file_format_params() -> anyhow::Result<()> {
    let file_format_params_v64 = vec![
        42, 29, 8, 1, 18, 13, 102, 105, 101, 108, 100, 95, 100, 101, 102, 97, 117, 108, 116, 26, 4,
        110, 117, 108, 108, 160, 6, 64, 168, 6, 24,
    ];

    let want = || {
        mt::principal::FileFormatParams::NdJson(NdJsonFileFormatParams {
            compression: StageFileCompression::Gzip,
            missing_field_as: NullAs::FieldDefault,
            null_field_as: NullAs::Null,
            null_if: vec![],
        })
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), file_format_params_v64.as_slice(), 0, want())?;
    Ok(())
}
