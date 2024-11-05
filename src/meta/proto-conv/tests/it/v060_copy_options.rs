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

use databend_common_meta_app::principal::CopyOptions;
use databend_common_meta_app::principal::OnErrorMode;
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
fn test_decode_v60_copy_options() -> anyhow::Result<()> {
    let copy_options_v60 = vec![
        10, 3, 32, 197, 24, 16, 142, 8, 24, 1, 32, 1, 40, 100, 48, 100, 56, 1, 64, 1,
    ];
    let want = || CopyOptions {
        on_error: OnErrorMode::SkipFileNum(3141),
        size_limit: 1038,
        max_files: 0,
        split_size: 100,
        purge: true,
        single: true,
        max_file_size: 100,
        disable_variant_check: true,
        return_failed_only: true,
        detailed_output: false,
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), copy_options_v60.as_slice(), 0, want())?;
    Ok(())
}
