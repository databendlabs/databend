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
fn test_serializers() -> Result<()> {
    struct Test {
        name: &'static str,
        data_type: DataType,
        value: DataValue,
        column: DataColumn,
        val_str: &'static str,
        col_str: Vec<String>,
    }

    let tests = vec![
        Test {
            name: "boolean",
            data_type: DataType::Boolean(true),
            value: DataValue::Boolean(Some(true)),
            column: Series::new(vec![Some(true), Some(false), None]).into(),
            val_str: "1",
            col_str: vec!["1".to_owned(), "0".to_owned(), "NULL".to_owned()],
        },
        Test {
            name: "int8",
            data_type: DataType::Int8,
            value: DataValue::Int8(Some(1)),
            column: Series::new(vec![Some(1i8), Some(2i8), None]).into(),
            val_str: "1",
            col_str: vec!["1".to_owned(), "2".to_owned(), "NULL".to_owned()],
        },
        Test {
            name: "datetime32",
            data_type: DataType::DateTime32(None),
            value: DataValue::UInt32(Some(1630320462)),
            column: Series::new(vec![Some(1630320462u32), Some(1637117572u32), None]).into(),
            val_str: "2021-08-30 10:47:42",
            col_str: vec![
                "2021-08-30 10:47:42".to_owned(),
                "2021-11-17 02:52:52".to_owned(),
                "NULL".to_owned(),
            ],
        },
        Test {
            name: "date32",
            data_type: DataType::Date32,
            value: DataValue::Int32(Some(18869)),
            column: Series::new(vec![Some(18869i32), Some(18948i32), None]).into(),
            val_str: "2021-08-30",
            col_str: vec![
                "2021-08-30".to_owned(),
                "2021-11-17".to_owned(),
                "NULL".to_owned(),
            ],
        },
        Test {
            name: "string",
            data_type: DataType::String,
            value: DataValue::String(Some("hello".as_bytes().to_vec())),
            column: Series::new(vec![Some("hello"), Some("world"), None]).into(),
            val_str: "hello",
            col_str: vec!["hello".to_owned(), "world".to_owned(), "NULL".to_owned()],
        },
    ];

    for test in tests {
        let serializer = test.data_type.create_serializer();
        let val_res = serializer.serialize_value(&test.value)?;
        assert_eq!(&val_res, test.val_str, "{:#?}", test.name);

        let col_res = serializer.serialize_column(&test.column)?;
        assert_eq!(col_res, test.col_str, "{:#?}", test.name);
    }

    {
        let data_type = DataType::Struct(vec![
            DataField::new("item_0", DataType::Float64, false),
            DataField::new("item_1", DataType::String, false),
            DataField::new("item_2", DataType::Boolean(false), false),
            DataField::new("item_3", DataType::Date16, false),
        ]);
        let serializer = data_type.create_serializer();
        let value = DataValue::Struct(vec![
            DataValue::Float64(Some(1.2)),
            DataValue::String(Some("hello".as_bytes().to_vec())),
            DataValue::Boolean(Some(true)),
            DataValue::UInt16(Some(18869)),
        ]);
        let result = serializer.serialize_value(&value)?;
        let expect = "(1.2,'hello',1,'2021-08-30')";
        assert_eq!(&result, expect);
    }

    Ok(())
}
