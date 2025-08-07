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

use std::collections::BTreeSet;

use databend_common_meta_app::principal as mt;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v141_task_state() -> anyhow::Result<()> {
    let task_state_v141 = vec![8, 1, 160, 6, 141, 1, 168, 6, 24];

    let want = || mt::TaskStateValue { is_succeeded: true };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), task_state_v141.as_slice(), 141, want())?;

    Ok(())
}

#[test]
fn test_decode_v141_task_dependent() -> anyhow::Result<()> {
    let task_dependent_value_v141 = vec![10, 1, 97, 10, 1, 98, 160, 6, 141, 1, 168, 6, 24];
    let want = || mt::TaskDependentValue(BTreeSet::from([s("a"), s("b")]));
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        task_dependent_value_v141.as_slice(),
        141,
        want(),
    )?;

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
