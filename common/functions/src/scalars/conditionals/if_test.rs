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
use common_datavalues::DataType;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::scalars::*;

#[test]
fn test_if_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        args: Vec<DataType>,
        columns: Vec<DataColumn>,
        expect: Series,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
    ]);

    let tests = vec![Test {
        name: "if-passed",
        display: "IF",
        nullable: false,
        func: IfFunction::try_create_func("")?,
        args: vec![
            DataType::Boolean,
            schema.field_with_name("a")?.data_type().clone(),
            DataType::Float64,
        ],
        columns: vec![
            Series::new(vec![true, false, false, true]).into(),
            Series::new(vec![1i64, 2, 3, 4]).into(),
            DataColumn::Constant(DataValue::Float64(Some(2.5)), 4),
        ],
        expect: Series::new(vec![1f64, 2.5, 2.5, 4f64]),
        error: "",
    }];

    for t in tests {
        let func = t.func;
        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let ref v = func.eval(&t.columns, t.columns[0].len())?;
        // Type check.
        let expect_type = func.return_type(&t.args)?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);

        let cmp = v.to_array()?.eq(&t.expect)?;
        assert!(cmp.all_true());
    }

    Ok(())
}
