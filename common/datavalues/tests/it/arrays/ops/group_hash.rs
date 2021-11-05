// Copyright 2020 Datafuse Labs.
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

use common_datavalues::prelude::*;
use common_exception::Result;

#[test]
fn test_group_hash() -> Result<()> {
    // Create DFUint16Array
    let df_uint16_array = DFUInt16Array::new_from_iter(1u16..4u16);
    // Create a buffer
    let mut buffer = Box::new([0u16, 0, 0]);
    let ptr = buffer.as_mut_ptr() as *mut u8;
    let _ = df_uint16_array.fixed_hash(ptr, 2);

    assert_eq!(buffer[0], 1);
    assert_eq!(buffer[1], 2);
    assert_eq!(buffer[2], 3);

    Ok(())
}
