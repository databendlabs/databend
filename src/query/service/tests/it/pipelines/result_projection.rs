// Copyright 2021 Datafuse Labs
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
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FunctionContext;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_pipeline::core::Pipeline;
use databend_query::pipelines::PipelineBuilder;
use databend_query::sql::ColumnBindingBuilder;
use databend_query::sql::Symbol;
use databend_query::sql::Visibility;

#[test]
fn test_result_projection_schema_mismatch_returns_error() {
    let input_schema = DataSchemaRefExt::create(vec![DataField::new(
        "0",
        DataType::Number(NumberDataType::Int64).wrap_nullable(),
    )]);
    let result_columns = vec![
        ColumnBindingBuilder::new(
            "c".to_string(),
            Symbol::new(0),
            Box::new(DataType::Number(NumberDataType::Int64)),
            Visibility::Visible,
        )
        .build(),
    ];
    let mut pipeline = Pipeline::create();

    let err = PipelineBuilder::build_result_projection(
        &FunctionContext::default(),
        input_schema,
        &result_columns,
        &mut pipeline,
        false,
    )
    .unwrap_err();

    assert_eq!(err.code(), ErrorCode::DATA_STRUCT_MISS_MATCH);
    let message = err.message();
    assert!(message.contains("Result projection schema mismatch"));
    assert!(message.contains("column #0"));
    assert!(message.contains("(c)"));
}
