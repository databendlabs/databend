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

use std::io::Write;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_formats::FastFieldDecoderValues;
use databend_common_formats::FastValuesDecodeFallback;
use databend_common_formats::FastValuesDecoder;
use databend_common_io::prelude::FormatSettings;
use goldenfile::Mint;

struct DummyFastValuesDecodeFallback {}

#[async_trait::async_trait]
impl FastValuesDecodeFallback for DummyFastValuesDecodeFallback {
    async fn parse_fallback(&self, _data: &str) -> Result<Vec<Scalar>> {
        Err(ErrorCode::Unimplemented("fallback".to_string()))
    }
}

#[tokio::test]
async fn test_fast_values_decoder_multi() -> Result<()> {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("fast_values.txt").unwrap();

    struct Test {
        data: &'static str,
        column_types: Vec<DataType>,
    }

    let cases = vec![
        Test {
            data: "(0, 1, 2), (3,4,5)",
            column_types: vec![
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
            ],
        },
        Test {
            data: "(0, 1, 2), (3,4,5), ",
            column_types: vec![
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
            ],
        },
        Test {
            data: "('', '', '')",
            column_types: vec![
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
            ],
        },
        Test {
            data: "( 1, '', '2022-10-01')",
            column_types: vec![
                DataType::Number(NumberDataType::Int16),
                DataType::String,
                DataType::Date,
            ],
        },
        Test {
            data: "(1, 2, 3), (1, 1, 1), (1, 1, 1);",
            column_types: vec![
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
            ],
        },
        Test {
            data: "(1, 2, 3), (1, 1, 1), (1, 1, 1);  ",
            column_types: vec![
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
            ],
        },
        Test {
            data: "(1.2, -2.9, 3.55), (3.12e2, 3.45e+3, -1.9e-3);",
            column_types: vec![
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
            ],
        },
    ];

    for case in cases {
        writeln!(file, "---------- Input Data ----------")?;
        writeln!(file, "{:?}", case.data)?;

        writeln!(file, "---------- Input Column Types ----------")?;
        writeln!(file, "{:?}", case.column_types)?;

        let field_decoder =
            FastFieldDecoderValues::create_for_insert(FormatSettings::default(), true);
        let mut values_decoder = FastValuesDecoder::new(case.data, &field_decoder);
        let fallback = DummyFastValuesDecodeFallback {};
        let mut columns = case
            .column_types
            .into_iter()
            .map(|dt| ColumnBuilder::with_capacity(&dt, values_decoder.estimated_rows()))
            .collect::<Vec<_>>();
        let result = values_decoder.parse(&mut columns, &fallback).await;

        writeln!(file, "---------- Output ---------")?;

        if let Err(err) = result {
            writeln!(file, "{}", err.to_string()).unwrap();
        } else {
            let columns = columns.into_iter().map(|cb| cb.build()).collect::<Vec<_>>();
            let got = DataBlock::new_from_columns(columns);
            writeln!(file, "{}", got.to_string()).unwrap();
        }
        writeln!(file, "\n")?;
    }

    Ok(())
}
