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
use common_datavalues::DataType;
use common_exception::Result;
use common_functions::scalars::*;
use pretty_assertions::assert_eq;

#[test]
fn test_uuid_verifier_functions() -> Result<()> {
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

    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::String, true)]);

    let tests = vec![
        Test {
            name: "is-empty-uuid-passed",
            display: "()",
            nullable: false,
            func: UUIDIsEmptyFunction::try_create("")?,
            args: vec![schema.field_with_name("a")?.data_type().clone()],
            columns: vec![
                Series::new(vec![Some("00000000-0000-0000-0000-000000000000"), None]).into(),
            ],
            expect: Series::new(vec![true, true]),
            error: "",
        },
        Test {
            name: "is-not-empty-uuid-passed",
            display: "()",
            nullable: false,
            func: UUIDIsNotEmptyFunction::try_create("")?,
            args: vec![schema.field_with_name("a")?.data_type().clone()],
            columns: vec![Series::new(vec![Some("59b69da3-81d0-4db2-96e8-3e20b505a7b2")]).into()],
            expect: Series::new(vec![true]),
            error: "",
        },
    ];

    for t in tests {
        let func = t.func;

        let columns: Vec<DataColumnWithField> = t
            .columns
            .iter()
            .map(|c| DataColumnWithField::new(c.clone(), DataField::new("a", c.data_type(), false)))
            .collect();

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let v = &(func.eval(&columns, t.columns[0].len())?);
        // Type check.
        let expect_type = func.return_type(&t.args)?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);

        let cmp = v.to_array()?.eq(&t.expect)?;
        assert!(cmp.all_true());
    }

    Ok(())
}
