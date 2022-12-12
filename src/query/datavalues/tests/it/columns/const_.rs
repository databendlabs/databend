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

use common_datavalues::prelude::*;
use common_exception::Result;

#[test]
fn test_cast_const_string_to_stringcolumn() -> Result<()> {
    let const_string = ConstColumn::new(Series::from_data(&["a"]), 1).arc();
    let string_column: &StringColumn = Series::check_get(&const_string)?;
    println!("string: {:?}", string_column);

    let const_string_arc = const_string.arc();
    unsafe {
        let const_column: &ConstColumn = Series::static_cast(&const_string_arc);
        println!("const column: {:?}", const_column);
    }
    Ok(())
}
