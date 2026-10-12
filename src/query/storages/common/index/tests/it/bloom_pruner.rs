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

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
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
use databend_common_expression::converts::datavalues::scalar_to_datavalue;
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
use databend_common_expression_test_support::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::BloomIndexBuilder;
use databend_storages_common_index::BloomIndexType;
use databend_storages_common_index::DEFAULT_NGRAM_FALSE_POSITIVE_RATE;
use databend_storages_common_index::FilterEvalResult;
use databend_storages_common_index::Index;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_index::NgramHashAlgorithm;
use databend_storages_common_index::filters::Filter;
use databend_storages_common_index::filters::FilterBuilder;
use databend_storages_common_index::filters::FilterImpl;
use databend_storages_common_index::filters::Xor8Builder;
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

/// Pins the raw-bit digests that pre-canonicalization writers persisted for
/// zero and NaN class members. `BloomIndex::find` probes these when reading
/// existing V3/V4 filters, so they must never drift.
#[test]
fn test_bloom_filter_raw_bits_digest_golden_vectors() {
    let f64_vectors: [(f64, u64); 6] = [
        (0.0, 0xbd60_acb6_58c7_9e45),
        (-0.0, 0xef08_26fa_9ec0_9086),
        (f64::NAN, 0xe958_07fc_db9b_69f5),
        (-f64::NAN, 0x9e6c_f27b_2190_3322),
        (f64::INFINITY, 0x5845_4220_dbdd_1e32),
        (f64::NEG_INFINITY, 0x2659_e363_3561_49e9),
    ];
    for (value, expected) in f64_vectors {
        assert_eq!(
            BloomIndex::raw_bits_digest(&value.to_bits()),
            expected,
            "f64 {value:?} ({:#x})",
            value.to_bits()
        );
    }
    let f32_vectors: [(f32, u64); 6] = [
        (0.0, 0xcc22_47b7_9ac4_8af0),
        (-0.0, 0x189c_380b_6dec_58b3),
        (f32::NAN, 0x8cdf_dbd6_3bde_4733),
        (-f32::NAN, 0x1ee6_34d9_faee_78f5),
        (f32::INFINITY, 0x8f4c_fa33_eeb7_5ea4),
        (f32::NEG_INFINITY, 0x0e00_3b0a_987b_db5e),
    ];
    for (value, expected) in f32_vectors {
        assert_eq!(
            BloomIndex::raw_bits_digest(&value.to_bits()),
            expected,
            "f32 {value:?} ({:#x})",
            value.to_bits()
        );
    }
}

/// Filters written before float canonicalization hold raw-bit digests.
/// Probing them must stay sound: a zero target finds either sign of zero, a
/// NaN target never prunes (any payload may have been stored), and unrelated
/// values are still pruned.
#[test]
fn test_bloom_filter_legacy_float_classes() -> anyhow::Result<()> {
    #[derive(Clone, Copy, Debug)]
    enum Class {
        Zero,
        Nan,
        One,
    }
    fn class_of(value: f64) -> Class {
        if value.is_nan() {
            Class::Nan
        } else if value == 0.0 {
            Class::Zero
        } else {
            Class::One
        }
    }

    let func_ctx = FunctionContext::default();
    for number_type in [NumberDataType::Float32, NumberDataType::Float64] {
        let data_type = DataType::Number(number_type);
        let field = TableField::new("x", TableDataType::Number(number_type));
        let schema = Arc::new(TableSchema::new(vec![field.clone()]));
        // f32 payloads are derived from the f64 ones by keeping the sign and
        // the quiet bit, so every stored/probe pair still has distinct bits.
        let scalar = |value: f64| {
            Scalar::Number(match number_type {
                NumberDataType::Float32 => NumberScalar::Float32((value as f32).into()),
                NumberDataType::Float64 => NumberScalar::Float64(value.into()),
                _ => unreachable!(),
            })
        };
        let stored_values = [0.0, -0.0, 1.0, f64::from_bits(0x7ff8_0000_0000_0001)];
        let probes = [0.0, -0.0, 1.0, f64::NAN, -f64::NAN, (-1.0f64).sqrt()];

        for version in [2, 3, 4] {
            // Reproduce the old writer: V2 hashed DataValue; V3/V4 hashed raw bits.
            for stored in stored_values {
                let mut builder = Xor8Builder::create();
                for value in [stored, 2.0] {
                    if version == 2 {
                        builder.add_key(&scalar_to_datavalue(&scalar(value)));
                    } else {
                        let digest = match number_type {
                            NumberDataType::Float32 => {
                                BloomIndex::raw_bits_digest(&(value as f32).to_bits())
                            }
                            NumberDataType::Float64 => {
                                BloomIndex::raw_bits_digest(&value.to_bits())
                            }
                            _ => unreachable!(),
                        };
                        builder.add_digest(digest);
                    }
                }
                let bytes = builder.build()?.to_bytes()?;
                let filter = FilterImpl::from_bytes(&bytes)?.0;
                let filter_schema = Arc::new(TableSchema::new(vec![TableField::new(
                    &BloomIndex::build_filter_bloom_name(version, &field)?,
                    TableDataType::Binary,
                )]));
                let index = BloomIndex::from_filter_block(
                    func_ctx.clone(),
                    filter_schema,
                    vec![Arc::new(filter)],
                    version,
                )?;

                for probe in probes {
                    let target = scalar(probe);
                    let digest =
                        BloomIndex::calculate_scalar_digest(&func_ctx, &target, &data_type)?;
                    let expr = check_function(
                        None,
                        "eq",
                        &[],
                        &[
                            Expr::ColumnRef(ColumnRef {
                                span: None,
                                id: "x".to_string(),
                                data_type: data_type.clone(),
                                display_name: "x".to_string(),
                            }),
                            Expr::Constant(Constant {
                                span: None,
                                scalar: target.clone(),
                                data_type: data_type.clone(),
                            }),
                        ],
                        &BUILTIN_FUNCTIONS,
                    )?;
                    let result = index.apply(
                        expr,
                        &HashMap::from([(target, digest)]),
                        &HashMap::new(),
                        &[],
                        &HashMap::new(),
                        schema.clone(),
                    )?;
                    let expected = match (class_of(stored), class_of(probe)) {
                        (_, Class::Nan) => FilterEvalResult::Uncertain,
                        (Class::Zero, Class::Zero) | (Class::One, Class::One) => {
                            FilterEvalResult::Uncertain
                        }
                        _ => FilterEvalResult::MustFalse,
                    };
                    assert_eq!(
                        result,
                        expected,
                        "v{version} {number_type:?}: stored={stored:?} ({:#x}), probe={probe:?} ({:#x})",
                        stored.to_bits(),
                        probe.to_bits()
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn test_bloom_filter_casts_string_literal_to_integer_column_type() {
    let column_type = DataType::Number(NumberDataType::Int32);
    let field = TableField::new("x", TableDataType::Number(NumberDataType::Int32));
    let expr = eq_expr_with_string_constant(column_type.clone(), "20240604");

    let result = BloomIndex::filter_index_field(&expr, vec![field.clone()], vec![]).unwrap();

    assert_eq!(result.bloom_fields, vec![field]);
    assert_eq!(result.bloom_scalars, vec![(
        0,
        Scalar::Number(NumberScalar::Int32(20240604)),
        column_type
    )]);
}

#[test]
fn test_bloom_filter_does_not_cast_number_literal_to_string_column_type() {
    let column_type = DataType::String;
    let field = TableField::new("x", TableDataType::String);
    let expr = check_function(
        None,
        "eq",
        &[],
        &[
            Expr::ColumnRef(ColumnRef {
                span: None,
                id: "x".to_string(),
                data_type: column_type,
                display_name: "x".to_string(),
            }),
            Expr::Constant(Constant {
                span: None,
                scalar: Scalar::Number(NumberScalar::Int32(123)),
                data_type: DataType::Number(NumberDataType::Int32),
            }),
        ],
        &BUILTIN_FUNCTIONS,
    )
    .unwrap();

    let result = BloomIndex::filter_index_field(&expr, vec![field], vec![]).unwrap();

    assert!(result.bloom_fields.is_empty());
    assert!(result.bloom_scalars.is_empty());
}

#[test]
fn test_bloom_filter_does_not_cast_string_literal_to_float_column_type() {
    let column_type = DataType::Number(NumberDataType::Float64);
    let field = TableField::new("x", TableDataType::Number(NumberDataType::Float64));
    let expr = eq_expr_with_string_constant(column_type, "0");

    let result = BloomIndex::filter_index_field(&expr, vec![field], vec![]).unwrap();

    assert!(result.bloom_fields.is_empty());
    assert!(result.bloom_scalars.is_empty());
}

#[test]
fn test_bloom_filter_rewrites_string_literal_integer_comparison() {
    let func_ctx = FunctionContext::default();
    let column_type = DataType::Number(NumberDataType::Int32);
    let field = TableField::new("x", TableDataType::Number(NumberDataType::Int32));
    let schema = Arc::new(TableSchema::new(vec![field.clone()]));
    let bloom_columns = bloom_columns_map(&schema, &[0]);
    let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1, 2])]);
    let expr = eq_expr_with_string_constant(column_type, "20240604");

    let result =
        BloomIndex::filter_index_field(&expr, bloom_columns.values().cloned().collect(), vec![])
            .unwrap();
    let mut eq_scalar_map = HashMap::<Scalar, u64>::new();
    for (_, scalar, ty) in result.bloom_scalars.into_iter() {
        eq_scalar_map.entry(scalar).or_insert_with_key(|scalar| {
            BloomIndex::calculate_scalar_digest(&func_ctx, scalar, &ty).unwrap()
        });
    }

    let mut builder = BloomIndexBuilder::create(
        func_ctx.clone(),
        BloomIndexType::default(),
        bloom_columns.clone(),
        &[],
    )
    .unwrap();
    builder.add_block(&block).unwrap();
    let index = builder.finalize().unwrap().unwrap();

    let column_stats = block
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, entry)| {
            let field = bloom_columns.get(&i)?;
            let column = entry.as_column().unwrap();
            let (min, max) = column.domain().to_minmax();
            Some((field.column_id, ColumnStatistics {
                min,
                max,
                null_count: 0,
                in_memory_size: 0,
                distinct_of_values: None,
            }))
        })
        .collect();

    let (expr, domains) = index
        .rewrite_expr(
            expr,
            &eq_scalar_map,
            &HashMap::new(),
            &[],
            &column_stats,
            schema,
        )
        .unwrap();
    let folded =
        ConstantFolder::fold_with_domain(Cow::Owned(expr), &domains, &func_ctx, &BUILTIN_FUNCTIONS)
            .0;

    assert!(matches!(
        folded.as_ref(),
        Expr::Constant(Constant {
            scalar: Scalar::Boolean(false),
            ..
        })
    ));
}

fn eq_expr_with_string_constant(column_type: DataType, value: &str) -> Expr<String> {
    check_function(
        None,
        "eq",
        &[],
        &[
            Expr::ColumnRef(ColumnRef {
                span: None,
                id: "x".to_string(),
                data_type: column_type,
                display_name: "x".to_string(),
            }),
            Expr::Constant(Constant {
                span: None,
                scalar: Scalar::String(value.to_string()),
                data_type: DataType::String,
            }),
        ],
        &BUILTIN_FUNCTIONS,
    )
    .unwrap()
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

    let raw_expr = parse_raw_expr(text, &columns, &BUILTIN_FUNCTIONS);
    let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
    let expr = type_check::rewrite_function_to_cast(expr, &BUILTIN_FUNCTIONS);
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
    let (fold_expr, _) = ConstantFolder::fold(Cow::Borrowed(&expr), &func_ctx, &BUILTIN_FUNCTIONS);
    let expr = match fold_expr {
        Cow::Borrowed(_) => expr,
        Cow::Owned(fold_expr) => {
            writeln!(file, "fold_expr: {fold_expr}").unwrap();
            fold_expr
        }
    };

    let bloom_fields = bloom_columns.values().cloned().collect::<Vec<_>>();
    let ngram_fields = ngram_args
        .iter()
        .map(|arg| arg.field().clone())
        .collect::<Vec<_>>();
    let result = BloomIndex::filter_index_field(&expr, bloom_fields, ngram_fields).unwrap();

    let mut eq_scalar_map = HashMap::<Scalar, u64>::new();
    for (_, scalar, ty) in result.bloom_scalars.into_iter() {
        eq_scalar_map.entry(scalar).or_insert_with_key(|scalar| {
            BloomIndex::calculate_scalar_digest(&func_ctx, scalar, &ty).unwrap()
        });
    }

    let mut like_scalar_map = HashMap::<usize, HashMap<Scalar, Vec<u64>>>::new();
    for (index, scalar) in result.ngram_scalars {
        let ngram_arg = &ngram_args[index];
        let mut digests = Vec::new();
        BloomIndex::calculate_ngram_digests(
            Value::Scalar(scalar.clone()),
            ngram_arg.gram_size(),
            ngram_arg.hash_algorithm(),
            |digest| digests.push(digest),
        );
        if !digests.is_empty() {
            like_scalar_map
                .entry(index)
                .or_default()
                .entry(scalar)
                .or_insert(digests);
        }
    }

    let mut builder = BloomIndexBuilder::create(
        func_ctx.clone(),
        BloomIndexType::default(),
        bloom_columns.clone(),
        ngram_args,
    )
    .unwrap();
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
    let result = match ConstantFolder::fold_with_domain(
        Cow::Borrowed(&expr),
        &domains,
        &func_ctx,
        &BUILTIN_FUNCTIONS,
    )
    .0
    .as_ref()
    {
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
            ngram_args.push(NgramArgs::new(
                i,
                table_field.clone(),
                3,
                1024,
                DEFAULT_NGRAM_FALSE_POSITIVE_RATE,
                NgramHashAlgorithm::City64V0,
            ))
        }
    }
    ngram_args
}
