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

use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow::buffer::Buffer;
use common_exception::Result;
use common_expression::type_check::check_function;
use common_expression::types::array::ArrayColumn;
use common_expression::types::map::KvColumn;
use common_expression::types::map::KvPair;
use common_expression::types::number::NumberScalar;
use common_expression::types::number::UInt8Type;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::ConstantFolder;
use common_expression::DataBlock;
use common_expression::Expr;
use common_expression::FromData;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::Value;
use common_functions::BUILTIN_FUNCTIONS;
use storages_common_index::filters::BlockFilter as LatestBloom;
use storages_common_index::BloomIndex;
use storages_common_index::FilterEvalResult;
use storages_common_table_meta::meta::Versioned;

#[test]
fn test_bloom_filter() -> Result<()> {
    let schema = Arc::new(TableSchema::new(vec![
        TableField::new("0", TableDataType::Number(NumberDataType::UInt8)),
        TableField::new("1", TableDataType::String),
        TableField::new(
            "2",
            TableDataType::Map(Box::new(TableDataType::Tuple {
                fields_name: vec!["key".to_string(), "value".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::UInt8),
                    TableDataType::String,
                ],
            })),
        ),
    ]));

    let kv_ty = DataType::Tuple(vec![
        DataType::Number(NumberDataType::UInt8),
        DataType::String,
    ]);
    let map_ty = DataType::Map(Box::new(kv_ty));

    let blocks = vec![
        DataBlock::new(
            vec![
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::UInt8),
                    value: Value::Scalar(Scalar::Number(NumberScalar::UInt8(1))),
                },
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Scalar(Scalar::String(b"a".to_vec())),
                },
                BlockEntry {
                    data_type: map_ty.clone(),
                    value: Value::Scalar(Scalar::Map(Column::Tuple(vec![
                        UInt8Type::from_data(vec![1, 2]),
                        StringType::from_data(vec!["a", "b"]),
                    ]))),
                },
            ],
            2,
        ),
        DataBlock::new_from_columns(vec![
            UInt8Type::from_data(vec![2, 3]),
            StringType::from_data(vec!["b", "c"]),
            Column::Map(Box::new(
                ArrayColumn::<KvPair<AnyType, AnyType>> {
                    values: KvColumn {
                        keys: UInt8Type::from_data(vec![1, 2, 3]),
                        values: StringType::from_data(vec!["b", "c", "d"]),
                    },
                    offsets: Buffer::<u64>::from(vec![0, 2, 3]),
                }
                .upcast(),
            )),
        ]),
    ];
    let blocks_ref = blocks.iter().collect::<Vec<_>>();

    let index = BloomIndex::try_create(
        FunctionContext::default(),
        schema,
        LatestBloom::VERSION,
        &blocks_ref,
    )?
    .unwrap();

    assert_eq!(
        FilterEvalResult::MustFalse,
        eval_index(
            &index,
            "0",
            Scalar::Number(NumberScalar::UInt8(0)),
            DataType::Number(NumberDataType::UInt8)
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(
            &index,
            "0",
            Scalar::Number(NumberScalar::UInt8(1)),
            DataType::Number(NumberDataType::UInt8)
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(
            &index,
            "0",
            Scalar::Number(NumberScalar::UInt8(2)),
            DataType::Number(NumberDataType::UInt8)
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(&index, "1", Scalar::String(b"a".to_vec()), DataType::String)
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(&index, "1", Scalar::String(b"b".to_vec()), DataType::String)
    );
    assert_eq!(
        FilterEvalResult::MustFalse,
        eval_index(&index, "1", Scalar::String(b"d".to_vec()), DataType::String)
    );

    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_map_index(
            &index,
            "2",
            map_ty.clone(),
            Scalar::Number(NumberScalar::UInt8(1)),
            DataType::Number(NumberDataType::UInt8),
            Scalar::String(b"a".to_vec()),
            DataType::String
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_map_index(
            &index,
            "2",
            map_ty.clone(),
            Scalar::Number(NumberScalar::UInt8(2)),
            DataType::Number(NumberDataType::UInt8),
            Scalar::String(b"b".to_vec()),
            DataType::String
        )
    );
    assert_eq!(
        FilterEvalResult::MustFalse,
        eval_map_index(
            &index,
            "2",
            map_ty,
            Scalar::Number(NumberScalar::UInt8(3)),
            DataType::Number(NumberDataType::UInt8),
            Scalar::String(b"x".to_vec()),
            DataType::String
        )
    );
    Ok(())
}

fn eval_index(index: &BloomIndex, col_name: &str, val: Scalar, ty: DataType) -> FilterEvalResult {
    let expr = check_function(
        None,
        "eq",
        &[],
        &[
            Expr::ColumnRef {
                span: None,
                id: col_name.to_string(),
                data_type: ty.clone(),
                display_name: col_name.to_string(),
            },
            Expr::Constant {
                span: None,
                scalar: val,
                data_type: ty,
            },
        ],
        &BUILTIN_FUNCTIONS,
    )
    .unwrap();

    let point_query_cols = BloomIndex::find_eq_columns(&expr).unwrap();

    let mut scalar_map = HashMap::<Scalar, u64>::new();
    let func_ctx = FunctionContext::default();
    for (_, scalar, ty) in point_query_cols.iter() {
        if !scalar_map.contains_key(scalar) {
            let digest = BloomIndex::calculate_scalar_digest(&func_ctx, scalar, ty).unwrap();
            scalar_map.insert(scalar.clone(), digest);
        }
    }

    index.apply(expr, &scalar_map).unwrap()
}

fn eval_map_index(
    index: &BloomIndex,
    col_name: &str,
    map_ty: DataType,
    key: Scalar,
    key_ty: DataType,
    val: Scalar,
    ty: DataType,
) -> FilterEvalResult {
    let func_ctx = FunctionContext::default();
    let get_expr = check_function(
        None,
        "get",
        &[],
        &[
            Expr::ColumnRef {
                span: None,
                id: col_name.to_string(),
                data_type: map_ty,
                display_name: col_name.to_string(),
            },
            Expr::Constant {
                span: None,
                scalar: key,
                data_type: key_ty,
            },
        ],
        &BUILTIN_FUNCTIONS,
    )
    .unwrap();
    let expr = check_function(
        None,
        "eq",
        &[],
        &[get_expr, Expr::Constant {
            span: None,
            scalar: val,
            data_type: ty,
        }],
        &BUILTIN_FUNCTIONS,
    )
    .unwrap();
    let (expr, _) = ConstantFolder::fold(&expr, &func_ctx, &BUILTIN_FUNCTIONS);

    let point_query_cols = BloomIndex::find_eq_columns(&expr).unwrap();

    let mut scalar_map = HashMap::<Scalar, u64>::new();
    for (_, scalar, ty) in point_query_cols.iter() {
        if !scalar_map.contains_key(scalar) {
            let digest = BloomIndex::calculate_scalar_digest(&func_ctx, scalar, ty).unwrap();
            scalar_map.insert(scalar.clone(), digest);
        }
    }

    index.apply(expr, &scalar_map).unwrap()
}
