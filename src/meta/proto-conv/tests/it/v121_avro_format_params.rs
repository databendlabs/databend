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

use databend_common_meta_app::principal::AvroFileFormatParams;
use databend_common_meta_app::principal::FileFormatParams;
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
fn test_decode_v121_avro_file_format_params() -> anyhow::Result<()> {
    let avro_file_format_params_v121 = vec![
        8, 4, 18, 13, 70, 73, 69, 76, 68, 95, 68, 69, 70, 65, 85, 76, 84, 26, 4, 110, 117, 108,
        108, 26, 4, 78, 85, 76, 76, 160, 6, 121, 168, 6, 24,
    ];

    let want = || AvroFileFormatParams {
        compression: StageFileCompression::Zstd,
        missing_field_as: NullAs::FieldDefault,
        null_if: vec!["null".to_string(), "NULL".to_string()],
    };
    common::test_load_old(
        func_name!(),
        avro_file_format_params_v121.as_slice(),
        121,
        want(),
    )?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}

#[test]
fn test_decode_v121_file_format_params() -> anyhow::Result<()> {
    let file_format_params_v121 = vec![
        66, 35, 8, 4, 18, 13, 70, 73, 69, 76, 68, 95, 68, 69, 70, 65, 85, 76, 84, 26, 4, 110, 117,
        108, 108, 26, 4, 78, 85, 76, 76, 160, 6, 121, 168, 6, 24,
    ];
    let want = || {
        FileFormatParams::Avro(AvroFileFormatParams {
            compression: StageFileCompression::Zstd,
            missing_field_as: NullAs::FieldDefault,
            null_if: vec!["null".to_string(), "NULL".to_string()],
        })
    };
    common::test_load_old(func_name!(), file_format_params_v121.as_slice(), 0, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}
