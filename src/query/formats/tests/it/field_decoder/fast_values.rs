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

struct DummyFastValuesDecodeFallback {}

#[async_trait::async_trait]
impl FastValuesDecodeFallback for DummyFastValuesDecodeFallback {
    async fn parse_fallback(&self, _data: &str) -> Result<Vec<Scalar>> {
        Err(ErrorCode::Unimplemented("fallback".to_string()))
    }
}

#[tokio::test]
async fn test_fast_values_decoder_multi() -> Result<()> {
    struct Test {
        data: &'static str,
        column_types: Vec<DataType>,
        output: Result<&'static str>,
    }

    let tests = vec![
        Test {
            data: "(0, 1, 2), (3,4,5)",
            column_types: vec![
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
            ],
            output: Ok(
                "+----------+----------+----------+\n| Column 0 | Column 1 | Column 2 |\n+----------+----------+----------+\n| 0        | 1        | 2        |\n| 3        | 4        | 5        |\n+----------+----------+----------+",
            ),
        },
        Test {
            data: "(0, 1, 2), (3,4,5), ",
            column_types: vec![
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
            ],
            output: Err(ErrorCode::BadDataValueType(
                "Must start with parentheses".to_string(),
            )),
        },
        Test {
            data: "('', '', '')",
            column_types: vec![
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
            ],
            output: Err(ErrorCode::Unimplemented("fallback".to_string())),
        },
        Test {
            data: "( 1, '', '2022-10-01')",
            column_types: vec![
                DataType::Number(NumberDataType::Int16),
                DataType::String,
                DataType::Date,
            ],
            output: Ok(
                "+----------+----------+--------------+\n| Column 0 | Column 1 | Column 2     |\n+----------+----------+--------------+\n| 1        | ''       | '2022-10-01' |\n+----------+----------+--------------+",
            ),
        },
        Test {
            data: "(1, 2, 3), (1, 1, 1), (1, 1, 1);",
            column_types: vec![
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
            ],
            output: Ok(
                "+----------+----------+----------+\n| Column 0 | Column 1 | Column 2 |\n+----------+----------+----------+\n| 1        | 2        | 3        |\n| 1        | 1        | 1        |\n| 1        | 1        | 1        |\n+----------+----------+----------+",
            ),
        },
        Test {
            data: "(1, 2, 3), (1, 1, 1), (1, 1, 1);  ",
            column_types: vec![
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
                DataType::Number(NumberDataType::Int16),
            ],
            output: Ok(
                "+----------+----------+----------+\n| Column 0 | Column 1 | Column 2 |\n+----------+----------+----------+\n| 1        | 2        | 3        |\n| 1        | 1        | 1        |\n| 1        | 1        | 1        |\n+----------+----------+----------+",
            ),
        },
    ];

    for tt in tests {
        let field_decoder = FastFieldDecoderValues::create_for_insert(FormatSettings::default());
        let mut values_decoder = FastValuesDecoder::new(tt.data, &field_decoder);
        let fallback = DummyFastValuesDecodeFallback {};
        let mut columns = tt
            .column_types
            .into_iter()
            .map(|dt| ColumnBuilder::with_capacity(&dt, values_decoder.estimated_rows()))
            .collect::<Vec<_>>();
        let result = values_decoder.parse(&mut columns, &fallback).await;
        match tt.output {
            Err(err) => {
                assert!(result.is_err());
                assert_eq!(err.to_string(), result.unwrap_err().to_string())
            }
            Ok(want) => {
                let columns = columns.into_iter().map(|cb| cb.build()).collect::<Vec<_>>();
                let got = DataBlock::new_from_columns(columns);
                assert!(result.is_ok(), "{:?}", result);
                assert_eq!(got.to_string(), want.to_string())
            }
        }
    }
    Ok(())
}
