// Copyright 2022 Datafuse Labs
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

use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::ColumnRef;
use databend_common_expression::Constant;
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
use databend_common_expression::type_check;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Int8Type;
use databend_common_expression::types::Int16Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::MapType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::map::KvColumn;
use databend_common_expression::types::map::KvPair;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::test_utils::parse_raw_expr;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::BloomIndexBuilder;
use databend_storages_common_index::FilterEvalResult;
use databend_storages_common_index::Index;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_index::filters::Xor8Filter;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use goldenfile::Mint;

#[test]
fn test_bloom_filter() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("test_bloom_filter.txt").unwrap();

    test_base(file);
    test_specify(file);
    test_long_string(file);
    test_cast(file);
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
                BlockEntry::new_const_column_arg::<UInt8Type>(1, 2),
                BlockEntry::new_const_column_arg::<StringType>("a".to_string(), 2),
                BlockEntry::new_const_column_arg::<MapType<UInt8Type, StringType>>(
                    KvColumn {
                        keys: vec![1, 2].into(),
                        values: ["a", "b"].into_iter().map(String::from).collect(),
                    },
                    2,
                ),
                BlockEntry::new_const_column_arg::<MapType<StringType, VariantType>>(
                    KvColumn {
                        keys: ["a", "b"].into_iter().map(String::from).collect(),
                        values: VariantType::from_data(vec![
                            jsonb::parse_value(r#""abc""#.as_bytes()).unwrap().to_vec(),
                            jsonb::parse_value(r#"100"#.as_bytes()).unwrap().to_vec(),
                        ])
                        .into_variant()
                        .unwrap(),
                    },
                    2,
                ),
            ],
            2,
        ),
        DataBlock::new_from_columns(vec![
            UInt8Type::from_data(vec![2, 3]),
            StringType::from_data(vec![
                "The quick brown fox jumps over the lazy dog",
                "The early bird catches the worm",
            ]),
            Column::Map(Box::new(
                ArrayColumn::<KvPair<AnyType, AnyType>>::new(
                    KvColumn {
                        keys: UInt8Type::from_data(vec![1, 2, 3]),
                        values: StringType::from_data(vec!["b", "c", "d"]),
                    },
                    Buffer::<u64>::from(vec![0, 2, 3]),
                )
                .upcast(&DataType::Array(Box::new(DataType::Tuple(vec![
                    DataType::Number(NumberDataType::UInt8),
                    DataType::String,
                ])))),
            )),
            Column::Map(Box::new(
                ArrayColumn::<KvPair<AnyType, AnyType>>::new(
                    KvColumn {
                        keys: StringType::from_data(vec!["b", "c", "d"]),
                        values: VariantType::from_data(vec![
                            jsonb::parse_value(r#""def""#.as_bytes()).unwrap().to_vec(),
                            jsonb::parse_value(r#"true"#.as_bytes()).unwrap().to_vec(),
                            jsonb::parse_value(r#""xyz""#.as_bytes()).unwrap().to_vec(),
                        ]),
                    },
                    Buffer::<u64>::from(vec![0, 2, 3]),
                )
                .upcast(&DataType::Array(Box::new(DataType::Tuple(vec![
                    DataType::String,
                    DataType::Variant,
                ])))),
            )),
        ]),
    ];
    let block = DataBlock::concat(&blocks).unwrap();
    let bloom_columns = bloom_columns_map(&schema, &[0, 1, 2, 3]);
    let ngram_args = ngram_args(&schema, &[1]);

    for v in [0, 1, 2] {
        eval_index(
            file,
            "0",
            Scalar::Number(NumberScalar::UInt8(v)),
            DataType::Number(NumberDataType::UInt8),
            &block,
            &bloom_columns,
            &ngram_args,
            schema.clone(),
            false,
        );
    }

    for v in ["%fox jumps%", "%bird catches%", "%the doctor%"] {
        eval_index(
            file,
            "1",
            Scalar::String(v.to_string()),
            DataType::String,
            &block,
            &bloom_columns,
            &ngram_args,
            schema.clone(),
            true,
        );
    }

    for v in [
        "The quick brown fox jumps over the lazy dog",
        "The early bird catches the worm",
        "d",
    ] {
        eval_index(
            file,
            "1",
            Scalar::String(v.to_string()),
            DataType::String,
            &block,
            &bloom_columns,
            &ngram_args,
            schema.clone(),
            false,
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
            &ngram_args,
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
            &ngram_args,
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
        StringType::from_data(vec![
            "The quick brown fox jumps over the lazy dog",
            "The early bird catches the worm",
        ]),
    ])];
    let block = DataBlock::concat(&blocks).unwrap();
    {
        let bloom_columns = bloom_columns_map(&schema, &[0]);

        eval_index(
            file,
            "1",
            Scalar::String("d".to_string()),
            DataType::String,
            &block,
            &bloom_columns,
            &[],
            schema.clone(),
            false,
        );
    }
    {
        let ngram_args = ngram_args(&schema, &[0]);

        eval_index(
            file,
            "1",
            Scalar::String("d".to_string()),
            DataType::String,
            &block,
            &BTreeMap::new(),
            &ngram_args,
            schema,
            true,
        );
    }
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
        Scalar::String("ab".to_string()),
        DataType::String,
        &block,
        &bloom_columns,
        &[],
        schema,
        false,
    );
}

fn test_cast(file: &mut impl Write) {
    eval_text(
        file,
        "x::string = '5'",
        &[(
            "x",
            TableDataType::Number(NumberDataType::UInt8),
            UInt8Type::from_data(vec![1, 2]),
        )],
        &[0],
    );
    eval_text(
        file,
        "x::decimal(5,0) = 1.2",
        &[(
            "x",
            TableDataType::Number(NumberDataType::Int8),
            Int8Type::from_data(vec![0, 1, 2]),
        )],
        &[0],
    );
    eval_text(
        file,
        "x::decimal(5,0) = 2.00",
        &[(
            "x",
            TableDataType::Number(NumberDataType::Int8),
            Int8Type::from_data(vec![0, 1]),
        )],
        &[0],
    );
    eval_text(
        file,
        "x = 1.2",
        &[(
            "x",
            TableDataType::Number(NumberDataType::Int16),
            Int16Type::from_data(vec![0, 1, 2]),
        )],
        &[0],
    );
    eval_text(
        file,
        "x::string = '+3'",
        &[(
            "x",
            TableDataType::Number(NumberDataType::Int16),
            Int16Type::from_data(vec![1, 3, 100]),
        )],
        &[0],
    );
    eval_text(
        file,
        "x::string = '+3'",
        &[(
            "x",
            TableDataType::Number(NumberDataType::Int16),
            Int16Type::from_data(vec![100, 200]),
        )],
        &[0],
    );
    eval_text(
        file,
        "to_int32(to_int16(x)) = 1.2",
        &[(
            "x",
            TableDataType::Number(NumberDataType::Int8),
            Int8Type::from_data(vec![0, 2]),
        )],
        &[0],
    );
    eval_text(
        file,
        "x::int8 = 10",
        &[(
            "x",
            TableDataType::Number(NumberDataType::Int32),
            Int32Type::from_data(vec![0, 6000]),
        )],
        &[0],
    );
    eval_text(
        file,
        "x::datetime = '2021-03-05 01:01:01'",
        &[(
            "x",
            TableDataType::String,
            StringType::from_data(vec!["2021-03-05 01:01:01", "2021-03-05 01:01:02"]),
        )],
        &[0],
    );
    eval_text(
        file,
        "x::datetime = '2021-03-05 01:01:03'",
        &[(
            "x",
            TableDataType::Date,
            DateType::from_data(vec![18600, 18691, 19000]),
        )],
        &[0],
    );
    eval_text(
        file,
        "x::datetime = '2021-03-05 00:00:00'",
        &[(
            "x",
            TableDataType::Date,
            DateType::from_data(vec![18600, 18691, 19000]),
        )],
        &[0],
    );
    eval_text(
        file,
        "x::datetime = '2030-03-05 00:00:00'",
        &[(
            "x",
            TableDataType::Date,
            DateType::from_data(vec![18600, 18691, 19000]),
        )],
        &[0],
    );
    eval_text(
        file,
        "to_int8(x) = 1.2",
        &[(
            "x",
            TableDataType::Number(NumberDataType::Int16),
            Int16Type::from_data(vec![0, 300]),
        )],
        &[0],
    );
    eval_text(
        file,
        "x = 1::int8",
        &[(
            "x",
            TableDataType::Number(NumberDataType::Int16),
            Int16Type::from_data(vec![0, 100]).wrap_nullable(None),
        )],
        &[0],
    );
    eval_text(
        file,
        "x::int8 null = 1::int8",
        &[(
            "x",
            TableDataType::Number(NumberDataType::Int16),
            Int16Type::from_data(vec![0, 100]).wrap_nullable(None),
        )],
        &[0],
    );
}

fn eval_text(
    file: &mut impl Write,
    text: &str,
    columns: &[(&str, TableDataType, Column)],
    cols: &[usize],
) {
    let fields: Vec<_> = columns
        .iter()
        .map(|(name, data_type, _)| TableField::new(name, data_type.to_owned()))
        .collect();
    let schema = Arc::new(TableSchema::new(fields));
    let bloom_columns = bloom_columns_map(&schema, cols);
    let ngram_args = ngram_args(&schema, cols);
    let block =
        DataBlock::new_from_columns(columns.iter().map(|(_, _, col)| col.clone()).collect());

    let columns = schema
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f.data_type().into()))
        .collect::<Vec<(&str, DataType)>>();

    let raw_expr = parse_raw_expr(text, &columns);
    let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
    let expr = type_check::rewrite_function_to_cast(expr);
    let expr = expr
        .project_column_ref(|i| Ok(columns[*i].0.to_string()))
        .unwrap();

    eval_index_expr(file, &block, &bloom_columns, &ngram_args, schema, expr);
}

fn eval_index_expr(
    file: &mut impl Write,
    block: &DataBlock,
    bloom_columns: &BTreeMap<usize, TableField>,
    ngram_args: &[NgramArgs],
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

    let bloom_fields = bloom_columns.values().cloned().collect::<Vec<_>>();
    let ngram_fields = ngram_args
        .iter()
        .map(|arg| arg.field().clone())
        .collect::<Vec<_>>();
    let result = BloomIndex::filter_index_field(&expr, bloom_fields, ngram_fields).unwrap();

    let mut eq_scalar_map = HashMap::<(ColumnId, Scalar), u64>::new();
    for (index, scalar, ty) in result.bloom_scalars.into_iter() {
        let Some(field) = result.bloom_fields.get(index) else {
            continue;
        };
        let key = (field.column_id, scalar.clone());
        eq_scalar_map.entry(key).or_insert_with(|| {
            BloomIndex::calculate_scalar_digest(&func_ctx, &scalar, &ty).unwrap()
        });
    }

    let mut like_scalar_map = HashMap::<(ColumnId, Scalar), Vec<u64>>::new();
    for (index, scalar) in result.ngram_scalars.into_iter() {
        let Some(field) = result.ngram_fields.get(index) else {
            continue;
        };
        let Some(ngram_arg) = ngram_args
            .iter()
            .find(|arg| arg.column_id() == field.column_id)
        else {
            continue;
        };
        let Some(digests) = BloomIndex::calculate_ngram_nullable_column(
            Value::Scalar(scalar.clone()),
            ngram_arg.gram_size(),
            BloomIndex::ngram_hash,
        )
        .next() else {
            continue;
        };
        let key = (field.column_id, scalar);
        like_scalar_map.entry(key).or_insert(digests);
    }

    let mut builder =
        BloomIndexBuilder::create(func_ctx.clone(), bloom_columns.clone(), ngram_args).unwrap();
    builder.add_block(block).unwrap();
    let index = builder.finalize().unwrap().unwrap();

    let column_stats = block
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, entry)| {
            let field = bloom_columns.get(&i)?;
            let column = entry.as_column().unwrap();
            let null_count = column
                .as_nullable()
                .map(|nullable| nullable.validity.null_count())
                .unwrap_or_default() as u64;
            let (min, max) = column.domain().to_minmax();
            Some((field.column_id, ColumnStatistics {
                min,
                max,
                null_count,
                in_memory_size: 0,
                distinct_of_values: None,
            }))
        })
        .collect();

    let (expr, domains) = index
        .rewrite_expr(
            expr,
            &eq_scalar_map,
            &like_scalar_map,
            ngram_args,
            &column_stats,
            schema,
        )
        .unwrap();
    let result =
        match ConstantFolder::fold_with_domain(&expr, &domains, &func_ctx, &BUILTIN_FUNCTIONS).0 {
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            }) => FilterEvalResult::MustFalse,
            _ => FilterEvalResult::Uncertain,
        };
    let domains = BTreeMap::from_iter(domains);

    writeln!(file, "filter   : {expr}").unwrap();
    writeln!(file, "domains  : {domains:?}").unwrap();
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
    ngram_args: &[NgramArgs],
    schema: Arc<TableSchema>,
    is_like: bool,
) {
    let expr = check_function(
        None,
        if is_like { "like" } else { "eq" },
        &[],
        &[
            Expr::ColumnRef(ColumnRef {
                span: None,
                id: col_name.to_string(),
                data_type: ty.clone(),
                display_name: col_name.to_string(),
            }),
            Expr::Constant(Constant {
                span: None,
                scalar: val,
                data_type: ty,
            }),
        ],
        &BUILTIN_FUNCTIONS,
    )
    .unwrap();

    eval_index_expr(file, block, bloom_columns, ngram_args, schema, expr)
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
    ngram_args: &[NgramArgs],
    schema: Arc<TableSchema>,
) {
    let fields = schema.fields.clone();
    let col_name = &fields[i].name;
    let get_expr = check_function(
        None,
        "get",
        &[],
        &[
            Expr::ColumnRef(ColumnRef {
                span: None,
                id: col_name.to_string(),
                data_type: map_ty,
                display_name: col_name.to_string(),
            }),
            Expr::Constant(Constant {
                span: None,
                scalar: key,
                data_type: key_ty,
            }),
        ],
        &BUILTIN_FUNCTIONS,
    )
    .unwrap();

    let const_expr = Expr::Constant(Constant {
        span: None,
        scalar: val,
        data_type: ty,
    });

    let eq_expr =
        check_function(None, "eq", &[], &[get_expr, const_expr], &BUILTIN_FUNCTIONS).unwrap();
    let expr = check_function(None, "is_true", &[], &[eq_expr], &BUILTIN_FUNCTIONS).unwrap();

    eval_index_expr(file, block, bloom_columns, ngram_args, schema, expr);
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

fn ngram_args(schema: &TableSchema, cols: &[FieldIndex]) -> Vec<NgramArgs> {
    let mut ngram_args = Vec::new();
    for &i in cols {
        let table_field = schema.field(i);
        let data_type = DataType::from(table_field.data_type());
        if Xor8Filter::supported_type(&data_type) {
            ngram_args.push(NgramArgs::new(i, table_field.clone(), 3, 1024))
        }
    }
    ngram_args
}
