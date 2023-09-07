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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_formats::FastFieldDecoderValues;
use common_formats::FastValuesDecodeFallback;
use common_formats::FastValuesDecoder;
use common_io::prelude::FormatSettings;
use tokio;

struct DummyFastValuesDecodeFallback {}

#[async_trait::async_trait]
impl FastValuesDecodeFallback for DummyFastValuesDecodeFallback {
    async fn parse(&self, _data: &str) -> Result<Vec<Scalar>> {
        Err(ErrorCode::Unimplemented("no fallback".to_string()))
    }
}

#[tokio::test]
async fn test_fast_values_decoder() -> Result<()> {
    let field_decoder = FastFieldDecoderValues::create_for_insert(FormatSettings::default());
    let data = "(1, 2, 3)";
    let mut values_decoder = FastValuesDecoder::new(data, &field_decoder);
    let fallback = DummyFastValuesDecodeFallback {};
    let column_types = vec![
        DataType::Number(NumberDataType::Int16),
        DataType::Number(NumberDataType::Int16),
        DataType::Number(NumberDataType::Int16),
    ];
    let rows = 1;

    let mut columns = column_types
        .into_iter()
        .map(|dt| ColumnBuilder::with_capacity(&dt, rows))
        .collect::<Vec<_>>();

    values_decoder.parse(&mut columns, &fallback).await?;

    let columns = columns
        .into_iter()
        .map(|col| col.build())
        .collect::<Vec<_>>();
    assert_eq!(columns.len(), 3);
    Ok(())
}
