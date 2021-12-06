// Copyright 2021 Datafuse Labs.
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
fn test_cast_by_str() -> Result<()> {
    struct Test {
        name: &'static str,
        literal: &'static str,
        value: DataValue,
    }

    let tests = vec![
        Test {
            name: "zero",
            literal: "0",
            value: DataValue::UInt8(Some(0)),
        },
        Test {
            name: "positive",
            literal: "31",
            value: DataValue::UInt8(Some(31)),
        },
        Test {
            name: "negative",
            literal: "-1",
            value: DataValue::Int8(Some(-1)),
        },
        Test {
            name: "i8::MIN",
            literal: "-128",
            value: DataValue::Int8(Some(i8::MIN)),
        },
        Test {
            name: "i16::MIN",
            literal: "-32768",
            value: DataValue::Int16(Some(i16::MIN)),
        },
        Test {
            name: "i32::MIN",
            literal: "-2147483648",
            value: DataValue::Int32(Some(i32::MIN)),
        },
        Test {
            name: "i64::MIN",
            literal: "-9223372036854775808",
            value: DataValue::Int64(Some(i64::MIN)),
        },
        Test {
            name: "u8::MAX",
            literal: "255",
            value: DataValue::UInt8(Some(u8::MAX)),
        },
        Test {
            name: "u16::MAX",
            literal: "65535",
            value: DataValue::UInt16(Some(u16::MAX)),
        },
        Test {
            name: "u32::MAX",
            literal: "4294967295",
            value: DataValue::UInt32(Some(u32::MAX)),
        },
        Test {
            name: "i64::MAX",
            literal: "18446744073709551615",
            value: DataValue::UInt64(Some(u64::MAX)),
        },
        Test {
            name: "f64",
            literal: "1844674407370955244444",
            value: DataValue::Float64(Some(1844674407370955200000.0f64)),
        },
    ];

    for test in tests {
        let literal = test.literal;
        let result = DataValue::try_from_literal(literal)?;

        assert_eq!(result.data_type(), test.value.data_type());
        assert_eq!(&result, &test.value, "test with {}", test.name);
    }

    Ok(())
}
