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

use databend_common_meta_app::schema::TableCloneBinding;
use fastrace::func_name;

use crate::common;

// These bytes are built when version 186 is introduced and must not be changed.
#[test]
fn test_decode_v186_table_clone_binding() -> anyhow::Result<()> {
    let table_clone_binding_v186 = vec![8, 42, 160, 6, 186, 1, 168, 6, 24];
    let want = TableCloneBinding {
        source_table_id: 42,
    };

    common::test_load_old(func_name!(), table_clone_binding_v186.as_slice(), 186, want)?;
    common::test_pb_from_to(func_name!(), want)?;
    Ok(())
}
