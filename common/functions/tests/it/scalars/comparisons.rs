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
use common_functions::scalars::*;
use pretty_assertions::assert_eq;

#[test]
fn test_comparison_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        arg_names: Vec<&'static str>,
        columns: Vec<DataColumn>,
        expect: Series,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
    ]);

    let tests = vec![
        Test {
            name: "eq-passed",
            display: "=",
            nullable: false,
            func: ComparisonEqFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![4i64, 3, 2, 4]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![false, false, false, true]),
            error: "",
        },
        Test {
            name: "gt-passed",
            display: ">",
            nullable: false,
            func: ComparisonGtFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![4i64, 3, 2, 4]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![true, true, false, false]),
            error: "",
        },
        Test {
            name: "gt-eq-passed",
            display: ">=",
            nullable: false,
            func: ComparisonGtEqFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![4i64, 3, 2, 4]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![true, true, false, true]),
            error: "",
        },
        Test {
            name: "lt-passed",
            display: "<",
            nullable: false,
            func: ComparisonLtFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![4i64, 3, 2, 4]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![false, false, true, false]),
            error: "",
        },
        Test {
            name: "lt-eq-passed",
            display: "<=",
            nullable: false,
            func: ComparisonLtEqFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![4i64, 3, 2, 4]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![false, false, true, true]),
            error: "",
        },
        Test {
            name: "not-eq-passed",
            display: "!=",
            nullable: false,
            func: ComparisonNotEqFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![4i64, 3, 2, 4]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![true, true, true, false]),
            error: "",
        },
        Test {
            name: "like-passed",
            display: "LIKE",
            nullable: false,
            func: ComparisonLikeFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec!["abc", "abd", "abe", "abf"]).into(),
                Series::new(vec!["a%", "_b_", "abe", "a"]).into(),
            ],
            expect: Series::new(vec![true, true, true, false]),
            error: "",
        },
        Test {
            name: "not-like-passed",
            display: "NOT LIKE",
            nullable: false,
            func: ComparisonNotLikeFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec!["abc", "abd", "abe", "abf"]).into(),
                Series::new(vec!["a%", "_b_", "abe", "a"]).into(),
            ],
            expect: Series::new(vec![false, false, false, true]),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.columns[0].len();

        // Type check.
        let mut args = vec![];
        let mut fields = vec![];
        for name in t.arg_names {
            args.push(schema.field_with_name(name)?.data_type().clone());
            fields.push(schema.field_with_name(name)?.clone());
        }

        let columns: Vec<DataColumnWithField> = t
            .columns
            .iter()
            .zip(fields.iter())
            .map(|(c, f)| DataColumnWithField::new(c.clone(), f.clone()))
            .collect();

        let func = t.func;
        if let Err(e) = func.eval(&columns, rows) {
            assert_eq!(t.error, e.to_string());
        }
        func.eval(&columns, rows)?;

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let v = &(func.eval(&columns, rows)?);
        // Type check.
        let expect_type = func.return_type(&args)?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);

        let cmp = v.to_array()?.eq(&t.expect)?;
        assert!(cmp.all_true());
    }
    Ok(())
}
