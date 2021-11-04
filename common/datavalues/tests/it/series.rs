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
#[allow(clippy::eq_op)]
fn test_arithmetic_simple_series() {
    // Series +-/* Series
    let s = Series::new([1, 2, 3]);

    assert_eq!(Vec::from((&s + &s).unwrap().i64().unwrap()), vec![
        Some(2i64),
        Some(4),
        Some(6)
    ]);
    assert_eq!(Vec::from((&s - &s).unwrap().i64().unwrap()), vec![
        Some(0i64),
        Some(0),
        Some(0)
    ]);

    assert_eq!(Vec::from((&s * &s).unwrap().i64().unwrap()), vec![
        Some(1),
        Some(4),
        Some(9)
    ]);
    assert_eq!(Vec::from((&s / &s).unwrap().f64().unwrap()), vec![
        Some(1f64),
        Some(1f64),
        Some(1f64)
    ]);
}

#[test]
fn test_arithmetic_series() {
    use pretty_assertions::assert_eq;

    fn eq_series(a: &Series, b: &Series) -> Result<()> {
        assert_eq!(a.len(), b.len());
        assert_eq!(a.data_type(), b.data_type());

        let size = a.len();

        for i in 0..size {
            assert_eq!(a.try_get(i)?, b.try_get(i)?)
        }
        Ok(())
    }

    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        args: Vec<Vec<Series>>,
        expect: Vec<Series>,
        error: Vec<&'static str>,
        op: DataValueArithmeticOperator,
    }

    let tests = vec![
        ArrayTest {
            name: "plus-passed",
            args: vec![
                vec![Series::new(vec!["xx"]), Series::new(vec!["yy"])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![
                    Series::new(vec![4.0f32, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0f32, 2.0, 3.0, 4.0]),
                ],
                vec![
                    Series::new(vec![4.0f32, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0f32, 2.0, 3.0, 4.0]),
                ],
            ],
            op: DataValueArithmeticOperator::Plus,
            expect: vec![
                Series::new(vec![""]),
                Series::new(vec![5i64, 5, 5, 5]),
                Series::new(vec![5i64, 5, 5, 5]),
                Series::new(vec![5i64, 5, 5, 5]),
                Series::new(vec![5i64, 5, 5, 5]),
                Series::new(vec![5i64, 5, 5, 5]),
                Series::new(vec![5i64, 5, 5, 5]),
                Series::new(vec![5i64, 5, 5, 5]),
                Series::new(vec![5i64, 5, 5, 5]),
                Series::new(vec![5i64, 5, 5, 5]),
                Series::new(vec![5.0f64, 5.0, 5.0, 5.0]),
                Series::new(vec![5.0f64, 5.0, 5.0, 5.0]),
            ],
            error: vec![
                "Code: 10, displayText = DataValue Error: Unsupported (String) plus (String).",
            ],
        },
        ArrayTest {
            name: "minus-passed",
            args: vec![
                vec![Series::new(vec!["xx"]), Series::new(vec!["yy"])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![3, 2, 1, 0])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![3, 2, 1, 0])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![3, 2, 1, 0])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![3, 2, 1, 0])],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
            ],
            op: DataValueArithmeticOperator::Minus,

            expect: vec![
                Series::new(vec![""]),
                Series::new(vec![3i64, 1, -1, -3]),
                Series::new(vec![3i64, 1, -1, -3]),
                Series::new(vec![3i64, 1, -1, -3]),
                Series::new(vec![3i64, 1, -1, -3]),
                Series::new(vec![3i64, 1, -1, -3]),
                Series::new(vec![1i64, 1, 1, 1]),
                Series::new(vec![1i64, 1, 1, 1]),
                Series::new(vec![1i64, 1, 1, 1]),
                Series::new(vec![1i64, 1, 1, 1]),
                Series::new(vec![3.0f64, 1.0, -1.0, -3.0]),
                Series::new(vec![3.0f64, 1.0, -1.0, -3.0]),
            ],
            error: vec![
                "Code: 10, displayText = DataValue Error: Unsupported (String) minus (String).",
            ],
        },
        ArrayTest {
            name: "mul-passed",
            args: vec![
                vec![Series::new(vec!["xx"]), Series::new(vec!["yy"])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
            ],
            op: DataValueArithmeticOperator::Mul,

            expect: vec![
                Series::new(vec![""]),
                Series::new(vec![4i64, 6, 6, 4]),
                Series::new(vec![4i64, 6, 6, 4]),
                Series::new(vec![4i64, 6, 6, 4]),
                Series::new(vec![4i64, 6, 6, 4]),
                Series::new(vec![4i64, 6, 6, 4]),
                Series::new(vec![4i64, 6, 6, 4]),
                Series::new(vec![4i64, 6, 6, 4]),
                Series::new(vec![4i64, 6, 6, 4]),
                Series::new(vec![4.0f64, 6.0, 6.0, 4.0]),
                Series::new(vec![4.0f64, 6.0, 6.0, 4.0]),
            ],
            error: vec![
                "Code: 10, displayText = DataValue Error: Unsupported (String) multiply (String).",
            ],
        },
        ArrayTest {
            name: "div-passed",
            args: vec![
                vec![Series::new(vec!["xx"]), Series::new(vec!["yy"])],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
                vec![
                    Series::new(vec![4.0, 3.0, 2.0, 1.0]),
                    Series::new(vec![1.0, 2.0, 3.0, 4.0]),
                ],
            ],
            op: DataValueArithmeticOperator::Div,

            expect: vec![
                Series::new(vec![""]),
                Series::new(vec![4.0, 1.5, 0.6666666666666666, 0.25]),
                Series::new(vec![4.0, 1.5, 0.6666666666666666, 0.25]),
                Series::new(vec![4.0, 1.5, 0.6666666666666666, 0.25]),
                Series::new(vec![4.0, 1.5, 0.6666666666666666, 0.25]),
                Series::new(vec![4.0, 1.5, 0.6666666666666666, 0.25]),
                Series::new(vec![4.0, 1.5, 0.6666666666666666, 0.25]),
                Series::new(vec![4.0, 1.5, 0.6666666666666666, 0.25]),
                Series::new(vec![4.0, 1.5, 0.6666666666666666, 0.25]),
                Series::new(vec![4.0, 1.5, 0.6666666666666666, 0.25]),
                Series::new(vec![4.0, 1.5, 0.6666666666666666, 0.25]),
            ],
            error: vec![
                "Code: 10, displayText = DataValue Error: Unsupported (String) divide (String).",
            ],
        },
        ArrayTest {
            name: "rem-passed",
            args: vec![
                vec![Series::new(vec!["xx"]), Series::new(vec!["yy"])],
                vec![Series::new(vec![4, 3, 2, 1]), Series::new(vec![1, 2, 3, 4])],
                vec![
                    Series::new(vec![100, 99, 98, 96]),
                    Series::new(vec![2, 3, 2, 4]),
                ],
            ],
            op: DataValueArithmeticOperator::Modulo,

            expect: vec![
                Series::new(vec![""]),
                Series::new(vec![0i64, 1, 2, 1]),
                Series::new(vec![0i64, 0, 0, 0]),
            ],
            error: vec![
                "Code: 10, displayText = DataValue Error: Unsupported (String) modulo (String).",
            ],
        },
    ];

    for t in tests {
        for (i, args) in t.args.iter().enumerate() {
            let result = match t.op {
                DataValueArithmeticOperator::Plus => &args[0] + &args[1],
                DataValueArithmeticOperator::Minus => &args[0] - &args[1],
                DataValueArithmeticOperator::Mul => &args[0] * &args[1],
                DataValueArithmeticOperator::Div => &args[0] / &args[1],
                DataValueArithmeticOperator::Modulo => &args[0] % &args[1],
            };

            match result {
                Ok(v) => eq_series(&v, &t.expect[i]).unwrap(),
                Err(e) => assert_eq!(
                    t.error[i],
                    e.to_string(),
                    "failed in the test: {}, case: {}",
                    t.name,
                    i
                ),
            }
        }
    }
}
