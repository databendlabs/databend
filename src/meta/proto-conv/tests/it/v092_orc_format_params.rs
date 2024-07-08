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

use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::OrcFileFormatParams;
use minitrace::func_name;

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
fn test_decode_v92_orc_file_format_params() -> anyhow::Result<()> {
    let orc_file_format_params_v92 = vec![160, 6, 92, 168, 6, 24];
    let want = || OrcFileFormatParams {
        missing_field_as: Default::default(),
    };
    common::test_load_old(
        func_name!(),
        orc_file_format_params_v92.as_slice(),
        92,
        want(),
    )?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}

#[test]
fn test_decode_v92_file_format_params() -> anyhow::Result<()> {
    let file_format_params_v92 = vec![58, 6, 160, 6, 92, 168, 6, 24];
    let want = || {
        FileFormatParams::Orc(OrcFileFormatParams {
            missing_field_as: Default::default(),
        })
    };
    common::test_load_old(func_name!(), file_format_params_v92.as_slice(), 0, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}
