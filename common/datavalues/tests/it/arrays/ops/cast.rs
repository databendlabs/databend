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
fn test_array_cast() -> Result<()> {
    let array = DFUInt16Array::new_from_iter(1_u16..4u16);
    let result = array.cast_with_type(&DataType::UInt8)?;
    let expected = Series::new(vec![1_u8, 2, 3]);
    assert!(result.series_equal(&expected));
    Ok(())
}
