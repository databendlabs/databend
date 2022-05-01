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
fn test_nullable_deserializer_pop() -> Result<()> {
    let values_vec = vec![
        DataValue::Boolean(true),
        DataValue::Null,
        DataValue::Boolean(false),
        DataValue::Null,
    ];
    let data_type = NullableType::new_impl(BooleanType::new_impl());
    let mut deserializer = data_type.create_deserializer(4);

    // Append data value
    for value in values_vec.iter() {
        deserializer.append_data_value(value.clone())?;
    }

    // Pop all data value
    for expected_value in values_vec.iter().rev() {
        let value = deserializer.pop_data_value()?;
        assert_eq!(&value, expected_value);
    }

    let result = deserializer.pop_data_value();
    assert!(result.is_err());
    let error = result.unwrap_err().to_string();
    assert_eq!(
        error,
        "Code: 1018, displayText = Nullable deserializer is empty when pop data value."
    );
    Ok(())
}
