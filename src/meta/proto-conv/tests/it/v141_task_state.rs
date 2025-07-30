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

use databend_common_meta_app::principal as mt;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v141_task_state() -> anyhow::Result<()> {
    let task_state_v141 = vec![];

    let want = || mt::TaskState { is_succeeded: true };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), task_state_v141.as_slice(), 141, want())?;

    Ok(())
}

#[test]
fn test_decode_v141_task_dependent() -> anyhow::Result<()> {
    let task_dependent_v141 = vec![];

    let want = || mt::TaskDependent {
        ty: mt::DependentType::After,
        source: s("a"),
        target: s("c"),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), task_dependent_v141.as_slice(), 141, want())?;

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
