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

use arrow_array::ArrayRef;
use arrow_ord::sort::LexicographicalComparator;
use arrow_ord::sort::SortColumn;
use arrow_schema::SortOptions;
use databend_common_base::base::OrderedFloat;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::i256;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::*;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_expression::SortField;
use databend_common_pipeline_transforms::sort::CommonRowConverter;
use itertools::Itertools;
use jsonb::parse_owned_jsonb;
use rand::distributions::Alphanumeric;
use rand::distributions::Standard;
use rand::prelude::Distribution;
use rand::thread_rng;
use rand::Rng;

#[test]
fn test_fixed_width() {
    let cols = [
        Int16Type::from_opt_data(vec![
            Some(1),
            Some(2),
            None,
            Some(-5),
            Some(2),
            Some(2),
            Some(0),
        ]),
        Float32Type::from_opt_data(vec![
            Some(OrderedFloat(1.3)),
            Some(OrderedFloat(2.5)),
            None,
            Some(OrderedFloat(4.)),
            Some(OrderedFloat(0.1)),
            Some(OrderedFloat(-4.)),
            Some(OrderedFloat(-0.)),
        ]),
    ];

    let converter = CommonRowConverter::new(vec![
        SortField::new(DataType::Number(NumberDataType::Int16).wrap_nullable()),
        SortField::new(DataType::Number(NumberDataType::Float32).wrap_nullable()),
    ])
    .unwrap();

    let rows = converter.convert_columns(&cols, cols[0].len());

    assert!(rows.index(3).unwrap() < rows.index(6).unwrap());
    assert!(rows.index(0).unwrap() < rows.index(1).unwrap());
    assert!(rows.index(3).unwrap() < rows.index(0).unwrap());
    assert!(rows.index(4).unwrap() < rows.index(1).unwrap());
    assert!(rows.index(5).unwrap() < rows.index(4).unwrap());
}

#[test]
fn test_decimal128() {
    let converter = CommonRowConverter::new(vec![SortField::new(
        DataType::Decimal(DecimalSize::new_unchecked(38, 7)).wrap_nullable(),
    )])
    .unwrap();

    let col = Decimal128Type::from_opt_data_with_size(
        vec![
            None,
            Some(i128::MIN),
            Some(-13),
            Some(46_i128),
            Some(5456_i128),
            Some(i128::MAX),
        ],
        Some(DecimalSize::new_unchecked(38, 7)),
    );

    let num_rows = col.len();
    let rows = converter.convert_columns(&[col], num_rows);

    for i in 0..rows.len() - 1 {
        assert!(rows.index(i).unwrap() < rows.index(i + 1).unwrap());
    }
}

#[test]
fn test_decimal256() {
    let converter = CommonRowConverter::new(vec![SortField::new(
        DataType::Decimal(DecimalSize::new_unchecked(76, 7)).wrap_nullable(),
    )])
    .unwrap();

    let col = Decimal256Type::from_opt_data_with_size(
        vec![
            None,
            Some(i256::DECIMAL_MIN),
            Some(i256::from_words(-1, 0)),
            Some(i256::from_words(-1, i128::MAX)),
            Some(i256::from_words(0, i128::MAX)),
            Some(i256::from_words(46_i128, 0)),
            Some(i256::from_words(46_i128, 5)),
            Some(i256::DECIMAL_MAX),
        ],
        Some(DecimalSize::new_unchecked(76, 7)),
    );

    let num_rows = col.len();
    let rows = converter.convert_columns(&[col], num_rows);

    for i in 0..rows.len() - 1 {
        assert!(rows.index(i).unwrap() < rows.index(i + 1).unwrap());
    }
}

#[test]
fn test_decimal_view() {
    fn run(size: DecimalSize, col: Column) {
        let converter =
            CommonRowConverter::new(vec![SortField::new(DataType::Decimal(size))]).unwrap();
        let num_rows = col.len();
        let rows = converter.convert_columns(&[col], num_rows);
        for i in 0..num_rows - 1 {
            assert!(rows.index(i).unwrap() <= rows.index(i + 1).unwrap());
        }
    }

    {
        let data = vec![-100i64, 50i64, 100i64, 200i64, 300i64];

        for p in [15, 20, 40] {
            let size = DecimalSize::new_unchecked(p, 2);
            let col = Decimal64Type::from_data_with_size(&data, Some(size));
            run(size, col);
        }
    }

    {
        let data = vec![-1000i128, 500i128, 1500i128, 2000i128, 3000i128];

        for p in [15, 20, 40] {
            let size = DecimalSize::new_unchecked(p, 3);
            let col = Decimal128Type::from_data_with_size(&data, Some(size));

            run(size, col);
        }
    }

    {
        let data = vec![
            i256::from(-5000),
            i256::from(10000),
            i256::from(15000),
            i256::from(20000),
        ];

        for p in [15, 20, 40] {
            let size = DecimalSize::new_unchecked(p, 4);
            let col = Decimal256Type::from_data_with_size(&data, Some(size));

            run(size, col);
        }
    }
}

#[test]
fn test_bool() {
    let converter =
        CommonRowConverter::new(vec![SortField::new(DataType::Boolean.wrap_nullable())]).unwrap();

    let col = BooleanType::from_opt_data(vec![None, Some(false), Some(true)]);
    let num_rows = col.len();

    let rows = converter.convert_columns(&[col.clone()], num_rows);

    assert!(rows.index(2).unwrap() > rows.index(1).unwrap());
    assert!(rows.index(2).unwrap() > rows.index(0).unwrap());
    assert!(rows.index(1).unwrap() > rows.index(0).unwrap());

    let converter = CommonRowConverter::new(vec![SortField::new_with_options(
        DataType::Boolean.wrap_nullable(),
        false,
        false,
    )])
    .unwrap();

    let rows = converter.convert_columns(&[col], num_rows);

    assert!(rows.index(2).unwrap() < rows.index(1).unwrap());
    assert!(rows.index(2).unwrap() < rows.index(0).unwrap());
    assert!(rows.index(1).unwrap() < rows.index(0).unwrap());
}

#[test]
fn test_null_encoding() {
    let col = Column::Null { len: 10 };
    let converter = CommonRowConverter::new(vec![SortField::new(DataType::Null)]).unwrap();
    let rows = converter.convert_columns(&[col], 10);

    assert_eq!(rows.len(), 10);
    assert_eq!(rows.index(1).unwrap().len(), 0);
}

#[test]
fn test_binary() {
    let col = BinaryType::from_opt_data(vec![
        Some("hello".as_bytes().to_vec()),
        Some("he".as_bytes().to_vec()),
        None,
        Some("foo".as_bytes().to_vec()),
        Some("".as_bytes().to_vec()),
    ]);

    let converter =
        CommonRowConverter::new(vec![SortField::new(DataType::Binary.wrap_nullable())]).unwrap();
    let num_rows = col.len();
    let rows = converter.convert_columns(&[col], num_rows);

    assert!(rows.index(1).unwrap() < rows.index(0).unwrap());
    assert!(rows.index(2).unwrap() < rows.index(4).unwrap());
    assert!(rows.index(3).unwrap() < rows.index(0).unwrap());
    assert!(rows.index(3).unwrap() < rows.index(1).unwrap());

    const BLOCK_SIZE: usize = 32;

    let col = BinaryType::from_opt_data(vec![
        None,
        Some(vec![0_u8; 0]),
        Some(vec![0_u8; 6]),
        Some(vec![0_u8; BLOCK_SIZE]),
        Some(vec![0_u8; BLOCK_SIZE + 1]),
        Some(vec![1_u8; 6]),
        Some(vec![1_u8; BLOCK_SIZE]),
        Some(vec![1_u8; BLOCK_SIZE + 1]),
        Some(vec![0xFF_u8; 6]),
        Some(vec![0xFF_u8; BLOCK_SIZE]),
        Some(vec![0xFF_u8; BLOCK_SIZE + 1]),
    ]);
    let num_rows = col.len();

    let converter =
        CommonRowConverter::new(vec![SortField::new(DataType::Binary.wrap_nullable())]).unwrap();
    let rows = converter.convert_columns(&[col.clone()], num_rows);

    for i in 0..rows.len() {
        for j in i + 1..rows.len() {
            assert!(
                rows.index(i).unwrap() < rows.index(j).unwrap(),
                "{} < {} - {:?} < {:?}",
                i,
                j,
                rows.index(i).unwrap(),
                rows.index(j).unwrap()
            );
        }
    }

    let converter = CommonRowConverter::new(vec![SortField::new_with_options(
        DataType::Binary.wrap_nullable(),
        false,
        false,
    )])
    .unwrap();
    let rows = converter.convert_columns(&[col], num_rows);

    for i in 0..rows.len() {
        for j in i + 1..rows.len() {
            assert!(
                rows.index(i).unwrap() > rows.index(j).unwrap(),
                "{} > {} - {:?} > {:?}",
                i,
                j,
                rows.index(i).unwrap(),
                rows.index(j).unwrap()
            );
        }
    }
}

#[test]
fn test_string() {
    let col =
        StringType::from_opt_data(vec![Some("hello"), Some("he"), None, Some("foo"), Some("")]);

    let converter =
        CommonRowConverter::new(vec![SortField::new(DataType::String.wrap_nullable())]).unwrap();
    let num_rows = col.len();
    let rows = converter.convert_columns(&[col], num_rows);

    assert!(rows.index(1).unwrap() < rows.index(0).unwrap());
    assert!(rows.index(2).unwrap() < rows.index(4).unwrap());
    assert!(rows.index(3).unwrap() < rows.index(0).unwrap());
    assert!(rows.index(3).unwrap() < rows.index(1).unwrap());

    const BLOCK_SIZE: usize = 32;

    let col = StringType::from_opt_data(vec![
        None,
        Some(String::from_utf8(vec![0_u8; 0]).unwrap()),
        Some(String::from_utf8(vec![0_u8; 6]).unwrap()),
        Some(String::from_utf8(vec![0_u8; BLOCK_SIZE]).unwrap()),
        Some(String::from_utf8(vec![0_u8; BLOCK_SIZE + 1]).unwrap()),
        Some(String::from_utf8(vec![1_u8; 6]).unwrap()),
        Some(String::from_utf8(vec![1_u8; BLOCK_SIZE]).unwrap()),
        Some(String::from_utf8(vec![1_u8; BLOCK_SIZE + 1]).unwrap()),
    ]);
    let num_rows = col.len();

    let converter =
        CommonRowConverter::new(vec![SortField::new(DataType::String.wrap_nullable())]).unwrap();
    let rows = converter.convert_columns(&[col.clone()], num_rows);

    for i in 0..rows.len() {
        for j in i + 1..rows.len() {
            assert!(
                rows.index(i).unwrap() < rows.index(j).unwrap(),
                "{} < {} - {:?} < {:?}",
                i,
                j,
                rows.index(i).unwrap(),
                rows.index(j).unwrap()
            );
        }
    }

    let converter = CommonRowConverter::new(vec![SortField::new_with_options(
        DataType::String.wrap_nullable(),
        false,
        false,
    )])
    .unwrap();
    let rows = converter.convert_columns(&[col], num_rows);

    for i in 0..rows.len() {
        for j in i + 1..rows.len() {
            assert!(
                rows.index(i).unwrap() > rows.index(j).unwrap(),
                "{} > {} - {:?} > {:?}",
                i,
                j,
                rows.index(i).unwrap(),
                rows.index(j).unwrap()
            );
        }
    }
}

#[test]
fn test_variant() {
    let values = vec![
        None,
        Some("false".to_string()),
        Some("true".to_string()),
        Some("-3".to_string()),
        Some("-2.1".to_string()),
        Some("1.1".to_string()),
        Some("2".to_string()),
        Some("\"\"".to_string()),
        Some("\"abc\"".to_string()),
        Some("\"de\"".to_string()),
        Some("{\"k1\":\"v1\",\"k2\":\"v2\"}".to_string()),
        Some("{\"k1\":\"v2\"}".to_string()),
        Some("[1,2,3]".to_string()),
        Some("[\"xx\",11]".to_string()),
        Some("null".to_string()),
    ];

    let mut validity = MutableBitmap::with_capacity(values.len());
    let mut builder = BinaryColumnBuilder::with_capacity(values.len(), values.len() * 10);
    for value in values {
        if let Some(value) = value {
            validity.push(true);
            let owned_jsonb = parse_owned_jsonb(value.as_bytes()).unwrap();
            let raw_jsonb = owned_jsonb.as_raw();
            let compare_buf = raw_jsonb.convert_to_comparable();
            builder.put_slice(&compare_buf);
        } else {
            validity.push(false);
        }
        builder.commit_row();
    }
    let col = NullableColumn::new_column(Column::Variant(builder.build()), validity.into());

    let converter =
        CommonRowConverter::new(vec![SortField::new(DataType::Variant.wrap_nullable())]).unwrap();
    let num_rows = col.len();
    let rows = converter.convert_columns(&[col.clone()], num_rows);

    for i in 0..rows.len() {
        for j in i + 1..rows.len() {
            assert!(
                rows.index(i).unwrap() < rows.index(j).unwrap(),
                "{} < {} - {:?} < {:?}",
                i,
                j,
                rows.index(i).unwrap(),
                rows.index(j).unwrap()
            );
        }
    }

    let converter = CommonRowConverter::new(vec![SortField::new_with_options(
        DataType::Variant.wrap_nullable(),
        false,
        false,
    )])
    .unwrap();
    let rows = converter.convert_columns(&[col], num_rows);

    for i in 0..rows.len() {
        for j in i + 1..rows.len() {
            assert!(
                rows.index(i).unwrap() > rows.index(j).unwrap(),
                "{} > {} - {:?} > {:?}",
                i,
                j,
                rows.index(i).unwrap(),
                rows.index(j).unwrap()
            );
        }
    }
}

fn generate_number_column<K>(len: usize, valid_percent: f64) -> Column
where
    K: Number,
    Standard: Distribution<K>,
    NumberType<K>: FromData<K>,
{
    let mut rng = thread_rng();
    let data = (0..len)
        .map(|_| rng.gen_bool(valid_percent).then(|| rng.gen()))
        .collect_vec();
    NumberType::<K>::from_opt_data(data)
}

fn generate_string_column(len: usize, valid_percent: f64) -> Column {
    let mut rng = thread_rng();
    let data = (0..len)
        .map(|_| {
            rng.gen_bool(valid_percent).then(|| {
                let len = rng.gen_range(0..100);
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    // randomly generate 5 characters.
                    .take(len)
                    .map(char::from)
                    .collect::<String>()
            })
        })
        .collect::<Vec<_>>();
    StringType::from_opt_data(data)
}

fn generate_column(len: usize) -> Column {
    let mut rng = thread_rng();
    match rng.gen_range(0..7) {
        0 => generate_number_column::<i32>(len, 0.8),
        1 => generate_number_column::<u32>(len, 0.8),
        2 => generate_number_column::<i64>(len, 0.8),
        3 => generate_number_column::<u64>(len, 0.8),
        4 => generate_number_column::<F32>(len, 0.8),
        5 => generate_number_column::<F64>(len, 0.8),
        6 => generate_string_column(len, 0.8),
        _ => unreachable!(),
    }
}

fn print_row(cols: &[Column], row: usize) -> String {
    let t: Vec<_> = cols
        .iter()
        .map(|x| format!("{:?}", x.index(row).unwrap()))
        .collect();
    t.join(",")
}

fn print_options(cols: &[(bool, bool)]) -> String {
    let t: Vec<_> = cols
        .iter()
        .map(|(asc, null_first)| {
            format!(
                "({}, {})",
                if *asc { "ASC" } else { "DESC" },
                if *null_first {
                    "NULL_FIRST"
                } else {
                    "NULL_LAST"
                }
            )
        })
        .collect();
    t.join(",")
}

#[test]
fn fuzz_test() {
    for _ in 0..100 {
        let mut rng = thread_rng();
        let num_columns = rng.gen_range(1..5);
        let num_rows = rng.gen_range(5..100);
        let columns = (0..num_columns)
            .map(|_| generate_column(num_rows))
            .collect::<Vec<_>>();

        let options = (0..num_columns)
            .map(|_| (rng.gen_bool(0.5), rng.gen_bool(0.5)))
            .collect::<Vec<_>>();

        let order_columns = columns
            .iter()
            .map(|col| col.clone().into_arrow_rs())
            .collect::<Vec<ArrayRef>>();

        let sort_columns = options
            .iter()
            .zip(order_columns.iter())
            .map(|((asc, nulls_first), a)| SortColumn {
                values: a.clone(),
                options: Some(SortOptions {
                    descending: !*asc,
                    nulls_first: *nulls_first,
                }),
            })
            .collect::<Vec<_>>();

        let comparator = LexicographicalComparator::try_new(&sort_columns).unwrap();

        let fields = options
            .iter()
            .zip(&columns)
            .map(|((asc, nulls_first), col)| {
                SortField::new_with_options(col.data_type(), *asc, *nulls_first)
            })
            .collect();

        let converter = CommonRowConverter::new(fields).unwrap();
        let rows = converter.convert_columns(&columns, num_rows);

        for i in 0..num_rows {
            for j in 0..num_rows {
                let row_i = rows.index(i).unwrap();
                let row_j = rows.index(j).unwrap();
                let row_cmp = row_i.cmp(row_j);
                let lex_cmp = comparator.compare(i, j);
                assert_eq!(
                    row_cmp,
                    lex_cmp,
                    "\ndata: ({:?} vs {:?})\nrow format: ({:?} vs {:?})\noptions: {:?}",
                    print_row(&columns, i),
                    print_row(&columns, j),
                    row_i,
                    row_j,
                    print_options(&options)
                );
            }
        }
    }
}
