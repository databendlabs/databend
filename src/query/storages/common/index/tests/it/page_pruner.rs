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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::type_check;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_functions::test_utils::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::PageIndex;
use databend_storages_common_table_meta::meta::ClusterStatistics;

#[test]
fn test_page_index() -> Result<()> {
    let columns = [("a", Int32Type::data_type())];
    let expr = parse_expr(" to_string(a) = '3'", &columns);

    let func_ctx = FunctionContext::default();
    let cluster_key_id = 0;
    let cluster_keys = vec!["a".to_string()];

    let schema = Arc::new(TableSchema::new(vec![TableField::new(
        "a",
        TableDataType::Number(NumberDataType::Int32),
    )]));

    let index = PageIndex::try_create(func_ctx, cluster_key_id, cluster_keys, &expr, schema)?;

    let stats = ClusterStatistics {
        cluster_key_id,
        min: vec![Scalar::Number(0_i32.into())],
        max: vec![Scalar::Number(10_i32.into())],
        level: 0,
        pages: Some(vec![
            Scalar::Tuple(vec![Scalar::Number(0_i32.into())]),
            Scalar::Tuple(vec![Scalar::Number(1_i32.into())]),
            Scalar::Tuple(vec![Scalar::Number(2_i32.into())]),
        ]),
    };

    let got = index.apply(&Some(stats))?;
    // assert_eq!((true, Some(0..1)), got);

    // pruner

    println!("{:?}", got);

    Ok(())
}

fn parse_expr(text: &str, columns: &[(&str, DataType)]) -> Expr<String> {
    let raw_expr = parse_raw_expr(text, columns);
    let raw_expr = raw_expr.project_column_ref(|i| columns[*i].0.to_string());
    let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
    type_check::rewrite_function_to_cast(expr)
}
