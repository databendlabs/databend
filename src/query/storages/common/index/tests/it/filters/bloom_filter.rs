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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use databend_common_expression::type_check::check_function;
use databend_common_expression::types::array::ArrayColumn;
use databend_common_expression::types::map::KvColumn;
use databend_common_expression::types::map::KvPair;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::number::UInt8Type;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VariantType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::filters::BlockFilter as LatestBloom;
use databend_storages_common_index::filters::Xor8Filter;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::Index;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::Versioned;
use goldenfile::Mint;

#[test]
fn test_bloom_filter() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("test_bloom_filter.txt").unwrap();

    test_base(file);
    test_specify(file);
    test_long_string(file);
}

fn test_base(file: &mut impl Write) {
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
        TableField::new(
            "3",
            TableDataType::Map(Box::new(TableDataType::Tuple {
                fields_name: vec!["key".to_string(), "value".to_string()],
                fields_type: vec![TableDataType::String, TableDataType::Variant],
            })),
        ),
    ]));

    let map_ty1 = DataType::Map(Box::new(DataType::Tuple(vec![
        DataType::Number(NumberDataType::UInt8),
        DataType::String,
    ])));
    let map_ty2 = DataType::Map(Box::new(DataType::Tuple(vec![
        DataType::String,
        DataType::Variant,
    ])));

    let blocks = [
        DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt8),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt8(1))),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String("a".to_string())),
                ),
                BlockEntry::new(
                    map_ty1.clone(),
                    Value::Scalar(Scalar::Map(Column::Tuple(vec![
                        UInt8Type::from_data(vec![1, 2]),
                        StringType::from_data(vec!["a", "b"]),
                    ]))),
                ),
                BlockEntry::new(
                    map_ty2.clone(),
                    Value::Scalar(Scalar::Map(Column::Tuple(vec![
                        StringType::from_data(vec!["a", "b"]),
                        VariantType::from_data(vec![
                            jsonb::parse_value(r#""abc""#.as_bytes()).unwrap().to_vec(),
                            jsonb::parse_value(r#"100"#.as_bytes()).unwrap().to_vec(),
                        ]),
                    ]))),
                ),
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
            Column::Map(Box::new(
                ArrayColumn::<KvPair<AnyType, AnyType>> {
                    values: KvColumn {
                        keys: StringType::from_data(vec!["b", "c", "d"]),
                        values: VariantType::from_data(vec![
                            jsonb::parse_value(r#""def""#.as_bytes()).unwrap().to_vec(),
                            jsonb::parse_value(r#"true"#.as_bytes()).unwrap().to_vec(),
                            jsonb::parse_value(r#""xyz""#.as_bytes()).unwrap().to_vec(),
                        ]),
                    },
                    offsets: Buffer::<u64>::from(vec![0, 2, 3]),
                }
                .upcast(),
            )),
        ]),
    ];
    let block = DataBlock::concat(&blocks).unwrap();
    let bloom_columns = bloom_columns_map(&schema, &[0, 1, 2, 3]);

    for v in [0, 1, 2] {
        eval_index(
            file,
            "0",
            Scalar::Number(NumberScalar::UInt8(v)),
            DataType::Number(NumberDataType::UInt8),
            &block,
            &bloom_columns,
            schema.clone(),
        );
    }

    for v in ["a", "b", "d"] {
        eval_index(
            file,
            "1",
            Scalar::String(v.to_string()),
            DataType::String,
            &block,
            &bloom_columns,
            schema.clone(),
        );
    }

    for (k, v) in [(1, "a"), (2, "b"), (3, "x")] {
        eval_map_index(
            file,
            2,
            map_ty1.clone(),
            Scalar::Number(NumberScalar::UInt8(k)),
            DataType::Number(NumberDataType::UInt8),
            Scalar::String(v.to_string()),
            DataType::String,
            &block,
            &bloom_columns,
            schema.clone(),
        );
    }

    for (k, v) in [
        ("b", Scalar::String("def".to_string())),
        ("d", Scalar::String("xxx".to_string())),
        ("c", Scalar::Boolean(true)),
    ] {
        let v_type = v.as_ref().infer_data_type();
        eval_map_index(
            file,
            3,
            map_ty2.clone(),
            Scalar::String(k.to_string()),
            DataType::String,
            v,
            v_type,
            &block,
            &bloom_columns,
            schema.clone(),
        );
    }
}

fn test_specify(file: &mut impl Write) {
    let schema = Arc::new(TableSchema::new(vec![
        TableField::new("0", TableDataType::Number(NumberDataType::UInt8)),
        TableField::new("1", TableDataType::String),
    ]));

    let blocks = [DataBlock::new_from_columns(vec![
        UInt8Type::from_data(vec![1, 2]),
        StringType::from_data(vec!["a", "b"]),
    ])];
    let block = DataBlock::concat(&blocks).unwrap();
    let bloom_columns = bloom_columns_map(&schema, &[0]);

    eval_index(
        file,
        "1",
        Scalar::String("d".to_string()),
        DataType::String,
        &block,
        &bloom_columns,
        schema,
    );
}

fn test_long_string(file: &mut impl Write) {
    let schema = Arc::new(TableSchema::new(vec![
        TableField::new("0", TableDataType::Number(NumberDataType::UInt8)),
        TableField::new("1", TableDataType::String),
    ]));

    let val: String = (0..512).map(|_| 'a').collect();
    let blocks = [DataBlock::new_from_columns(vec![
        UInt8Type::from_data(vec![1, 2]),
        StringType::from_data(vec![&val, "bc"]),
    ])];
    let block = DataBlock::concat(&blocks).unwrap();

    // The average length of the string column exceeds 256 bytes.
    let bloom_columns = bloom_columns_map(&schema, &[0, 1]);

    eval_index(
        file,
        "1",
        Scalar::String("d".to_string()),
        DataType::String,
        &block,
        &bloom_columns,
        schema,
    );
}

fn eval_index_expr(
    file: &mut impl Write,
    block: &DataBlock,
    bloom_columns: &BTreeMap<usize, TableField>,
    schema: Arc<TableSchema>,
    expr: Expr<String>,
) {
    writeln!(file, "{block:?}").unwrap();
    writeln!(file, "expr     : {expr}").unwrap();

    let func_ctx = FunctionContext::default();
    let (fold_expr, _) = ConstantFolder::fold(&expr, &func_ctx, &BUILTIN_FUNCTIONS);
    let expr = if fold_expr != expr {
        writeln!(file, "fold_expr: {fold_expr}").unwrap();
        fold_expr
    } else {
        expr
    };

    let fields = bloom_columns.values().cloned().collect::<Vec<_>>();
    let (_, scalars) = BloomIndex::filter_index_field(expr.clone(), &fields).unwrap();

    let mut scalar_map = HashMap::<Scalar, u64>::new();
    for (scalar, ty) in scalars.into_iter() {
        if !scalar_map.contains_key(&scalar) {
            let digest = BloomIndex::calculate_scalar_digest(&func_ctx, &scalar, &ty).unwrap();
            scalar_map.insert(scalar, digest);
        }
    }
    let column_stats = StatisticsOfColumns::new();
    let index =
        BloomIndex::try_create(func_ctx, LatestBloom::VERSION, block, bloom_columns.clone())
            .unwrap()
            .unwrap();

    let result = index
        .apply(expr, &scalar_map, &column_stats, schema)
        .unwrap();

    writeln!(file, "result   : {result:?}").unwrap();
    write!(file, "\n\n").unwrap();
}

fn eval_index(
    file: &mut impl Write,
    col_name: &str,
    val: Scalar,
    ty: DataType,
    block: &DataBlock,
    bloom_columns: &BTreeMap<usize, TableField>,
    schema: Arc<TableSchema>,
) {
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

    eval_index_expr(file, block, bloom_columns, schema, expr)
}

#[allow(clippy::too_many_arguments)]
fn eval_map_index(
    file: &mut impl Write,
    i: FieldIndex,
    map_ty: DataType,
    key: Scalar,
    key_ty: DataType,
    val: Scalar,
    ty: DataType,
    block: &DataBlock,
    bloom_columns: &BTreeMap<usize, TableField>,
    schema: Arc<TableSchema>,
) {
    let fields = schema.fields.clone();
    let col_name = &fields[i].name;
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

    let const_expr = Expr::Constant {
        span: None,
        scalar: val,
        data_type: ty,
    };

    let eq_expr =
        check_function(None, "eq", &[], &[get_expr, const_expr], &BUILTIN_FUNCTIONS).unwrap();
    let expr = check_function(None, "is_true", &[], &[eq_expr], &BUILTIN_FUNCTIONS).unwrap();

    eval_index_expr(file, block, bloom_columns, schema, expr);
}

fn bloom_columns_map(
    schema: &TableSchema,
    cols: &[FieldIndex],
) -> BTreeMap<FieldIndex, TableField> {
    let mut bloom_columns_map = BTreeMap::new();
    for &i in cols {
        let field_type = schema.field(i).data_type();
        let data_type = DataType::from(field_type);
        if Xor8Filter::supported_type(&data_type) {
            bloom_columns_map.insert(i, schema.field(i).clone());
        }
    }
    bloom_columns_map
}
