// Copyright 2022 Datafuse Labs.
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

//! Test UserStageInfo

use common_datavalues::DataTypeImpl;
use common_datavalues::TimestampType;

use crate::common;

#[test]
fn test_datatype_latest() -> anyhow::Result<()> {
    common::test_pb_from_to("datatype", test_datatype())?;
    Ok(())
}

#[test]
fn test_datatype_v15() -> anyhow::Result<()> {
    // It is generated with common::test_pb_from_to.
    let datatype_v15 = vec![114, 6, 160, 6, 15, 168, 6, 1, 160, 6, 15, 168, 6, 1];
    let want = TimestampType::new_impl();
    common::test_load_old(func_name!(), datatype_v15.as_slice(), want)?;
    Ok(())
}

fn test_datatype() -> DataTypeImpl {
    TimestampType::new_impl()
}
