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

use common_expression::types::number::*;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::Value;
use goldenfile::Mint;

use crate::common::*;

#[test]
pub fn test_pass() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("kernel-pass.txt").unwrap();

    run_filter(
        &mut file,
        vec![true, false, false, false, true],
        &new_block(&[
            Int32Type::from_data(vec![0i32, 1, 2, 3, -4]),
            UInt8Type::from_data_with_validity(vec![10u8, 11, 12, 13, 14], vec![
                false, true, false, false, false,
            ]),
            Column::Null { len: 5 },
            StringType::from_data_with_validity(vec!["x", "y", "z", "a", "b"], vec![
                false, true, true, false, false,
            ]),
        ]),
    );

    run_concat(&mut file, &[
        new_block(&[
            Int32Type::from_data(vec![0i32, 1, 2, 3, -4]),
            UInt8Type::from_data_with_validity(vec![10u8, 11, 12, 13, 14], vec![
                false, true, false, false, false,
            ]),
            Column::Null { len: 5 },
            Column::EmptyArray { len: 5 },
            StringType::from_data_with_validity(vec!["x", "y", "z", "a", "b"], vec![
                false, true, true, false, false,
            ]),
        ]),
        new_block(&[
            Int32Type::from_data(vec![5i32, 6]),
            UInt8Type::from_data_with_validity(vec![15u8, 16], vec![false, true]),
            Column::Null { len: 2 },
            Column::EmptyArray { len: 2 },
            StringType::from_data_with_validity(vec!["x", "y"], vec![false, true]),
        ]),
    ]);

    run_take(
        &mut file,
        &[0, 3, 1],
        &new_block(&[
            Int32Type::from_data(vec![0i32, 1, 2, 3, -4]),
            UInt8Type::from_data_with_validity(vec![10u8, 11, 12, 13, 14], vec![
                false, true, false, false, false,
            ]),
            Column::Null { len: 5 },
            StringType::from_data_with_validity(vec!["x", "y", "z", "a", "b"], vec![
                false, true, true, false, false,
            ]),
        ]),
    );

    {
        let mut blocks = Vec::with_capacity(3);
        let indices = vec![
            (0, 0, 1),
            (1, 0, 1),
            (2, 0, 1),
            (0, 1, 1),
            (1, 1, 1),
            (2, 1, 1),
            (0, 2, 1),
            (1, 2, 1),
            (2, 2, 1),
            (0, 3, 1),
            (1, 3, 1),
            (2, 3, 1),
            // repeat 3
            (0, 0, 3),
        ];
        for i in 0..3 {
            let mut columns = Vec::with_capacity(3);
            columns.push(BlockEntry::new(
                DataType::Number(NumberDataType::UInt8),
                Value::Column(UInt8Type::from_data(vec![(i + 10) as u8; 4])),
            ));
            columns.push(BlockEntry::new(
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt8))),
                Value::Column(UInt8Type::from_data_with_validity(
                    vec![(i + 10) as u8; 4],
                    vec![true, true, false, false],
                )),
            ));
            blocks.push(DataBlock::new(columns, 4))
        }

        run_take_block(&mut file, &indices, &blocks);
    }

    {
        let mut blocks = Vec::with_capacity(3);
        let indices = vec![
            (0, 0, 2),
            (1, 0, 3),
            (2, 0, 1),
            (2, 1, 1),
            (0, 2, 1),
            (2, 2, 1),
            (0, 3, 1),
            (2, 3, 1),
        ];
        for i in 0..3 {
            let mut columns = Vec::with_capacity(3);
            columns.push(BlockEntry::new(
                DataType::Number(NumberDataType::UInt8),
                Value::Column(UInt8Type::from_data(vec![(i + 10) as u8; 4])),
            ));
            columns.push(BlockEntry::new(
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt8))),
                Value::Column(UInt8Type::from_data_with_validity(
                    vec![(i + 10) as u8; 4],
                    vec![true, true, false, false],
                )),
            ));
            blocks.push(DataBlock::new(columns, 4))
        }

        run_take_block_by_slices_with_limit(&mut file, &indices, &blocks, None);
        run_take_block_by_slices_with_limit(&mut file, &indices, &blocks, Some(4));
    }

    run_take_by_slice_limit(
        &mut file,
        &new_block(&[
            Int32Type::from_data(vec![0i32, 1, 2, 3, -4]),
            UInt8Type::from_data_with_validity(vec![10u8, 11, 12, 13, 14], vec![
                false, true, false, false, false,
            ]),
            Column::Null { len: 5 },
            StringType::from_data_with_validity(vec!["x", "y", "z", "a", "b"], vec![
                false, true, true, false, false,
            ]),
        ]),
        (2, 3),
        None,
    );

    run_take_by_slice_limit(
        &mut file,
        &new_block(&[
            Int32Type::from_data(vec![0i32, 1, 2, 3, -4]),
            UInt8Type::from_data_with_validity(vec![10u8, 11, 12, 13, 14], vec![
                false, true, false, false, false,
            ]),
            Column::Null { len: 5 },
            StringType::from_data_with_validity(vec!["x", "y", "z", "a", "b"], vec![
                false, true, true, false, false,
            ]),
        ]),
        (2, 3),
        Some(2),
    );

    run_scatter(
        &mut file,
        &new_block(&[
            Int32Type::from_data(vec![0i32, 1, 2, 3, -4]),
            UInt8Type::from_data_with_validity(vec![10u8, 11, 12, 13, 14], vec![
                false, true, false, false, false,
            ]),
            Column::Null { len: 5 },
            StringType::from_data_with_validity(vec!["x", "y", "z", "a", "b"], vec![
                false, true, true, false, false,
            ]),
        ]),
        &[0, 0, 1, 2, 1],
        3,
    );
}

/// This test covers take.rs, take_chunks.rs, take_compact.rs, filter.rs, concat.rs.
#[test]
pub fn test_take_and_filter_and_concat() -> common_exception::Result<()> {
    use common_expression::types::decimal::DecimalSize;
    use common_expression::types::DataType;
    use common_expression::types::DecimalDataType;
    use common_expression::types::NumberDataType;
    use common_expression::BlockEntry;
    use common_expression::Column;
    use common_expression::DataBlock;
    use common_expression::Value;
    use common_hashtable::RowPtr;
    use itertools::Itertools;
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let num_blocks = rng.gen_range(5..30);
    let data_types = vec![
        DataType::Null,
        DataType::EmptyArray,
        DataType::EmptyMap,
        DataType::Boolean,
        DataType::String,
        DataType::Bitmap,
        DataType::Variant,
        DataType::Timestamp,
        DataType::Date,
        DataType::Number(NumberDataType::UInt8),
        DataType::Number(NumberDataType::UInt16),
        DataType::Number(NumberDataType::UInt32),
        DataType::Number(NumberDataType::UInt64),
        DataType::Number(NumberDataType::Int8),
        DataType::Number(NumberDataType::Int16),
        DataType::Number(NumberDataType::Int32),
        DataType::Number(NumberDataType::Int64),
        DataType::Number(NumberDataType::Float32),
        DataType::Number(NumberDataType::Float64),
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: 10,
            scale: 2,
        })),
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: 35,
            scale: 3,
        })),
        DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt32))),
        DataType::Nullable(Box::new(DataType::String)),
        DataType::Array(Box::new(DataType::Number(NumberDataType::UInt32))),
        DataType::Map(Box::new(DataType::Tuple(vec![
            DataType::Number(NumberDataType::UInt64),
            DataType::String,
        ]))),
    ];

    let mut count = 0;
    let mut take_indices = Vec::new();
    let mut take_chunks_indices = Vec::new();
    let mut take_compact_indices = Vec::new();
    let mut idx = 0;
    let mut blocks = Vec::with_capacity(data_types.len());
    let mut filtered_blocks = Vec::with_capacity(data_types.len());
    for i in 0..num_blocks {
        let len = rng.gen_range(5..100);
        let slice_start = rng.gen_range(0..len - 1);
        let slice_end = rng.gen_range(slice_start..len);
        let slice_len = slice_end - slice_start;
        let mut filter = Column::random(&DataType::Boolean, len)
            .into_boolean()
            .unwrap();
        filter.slice(slice_start, slice_len);
        
        let mut columns = Vec::with_capacity(data_types.len());
        for data_type in data_types.iter() {
            let column = Column::random(data_type, len);
            let column = column.slice(slice_start..slice_end);
            columns.push(column);
        }

        let mut block_entries = Vec::with_capacity(data_types.len());
        let mut filtered_block_entries = Vec::with_capacity(data_types.len());
        for (col, data_type) in columns.into_iter().zip(data_types.iter()) {
            filtered_block_entries.push(BlockEntry::new(
                data_type.clone(),
                Value::Column(col.filter(&filter)),
            ));
            block_entries.push(BlockEntry::new(data_type.clone(), Value::Column(col)));
        }

        blocks.push(DataBlock::new(block_entries, slice_len));
        filtered_blocks.push(DataBlock::new(
            filtered_block_entries,
            slice_len - filter.unset_bits(),
        ));

        for (j, val) in filter.iter().enumerate() {
            if val {
                count += 1;
                take_indices.push(idx);
                take_chunks_indices.push(RowPtr {
                    chunk_index: i as u32,
                    row_index: j as u32,
                });
                take_compact_indices.push((idx, 1));
            }
            idx += 1;
        }
    }

    let column_vec = data_types
        .iter()
        .enumerate()
        .map(|(index, data_type)| {
            let columns = blocks
                .iter()
                .map(|block| {
                    block
                        .get_by_offset(index)
                        .value
                        .clone()
                        .into_column()
                        .unwrap()
                })
                .collect_vec();
            Column::take_downcast_column_vec(&columns, data_type.clone())
        })
        .collect_vec();

    let concated_blocks = DataBlock::concat(&blocks)?;
    let block_1 = concated_blocks.take(&take_indices, &mut None)?;
    let block_2 = concated_blocks.take_compacted_indices(&take_compact_indices, count)?;
    let block_3 = DataBlock::take_column_vec(
        &column_vec,
        &data_types,
        &take_chunks_indices,
        count,
        &mut None,
    );
    let block_4 = DataBlock::concat(&filtered_blocks)?;

    assert_eq!(block_1.num_columns(), block_2.num_columns());
    assert_eq!(block_1.num_rows(), block_2.num_rows());
    assert_eq!(block_1.num_columns(), block_3.num_columns());
    assert_eq!(block_1.num_rows(), block_3.num_rows());
    assert_eq!(block_1.num_columns(), block_4.num_columns());
    assert_eq!(block_1.num_rows(), block_4.num_rows());

    let columns_1 = block_1.columns();
    let columns_2 = block_2.columns();
    let columns_3 = block_3.columns();
    let columns_4 = block_4.columns();
    for idx in 0..columns_1.len() {
        assert_eq!(columns_1[idx].data_type, columns_2[idx].data_type);
        assert_eq!(columns_1[idx].value, columns_2[idx].value);
        assert_eq!(columns_1[idx].data_type, columns_3[idx].data_type);
        assert_eq!(columns_1[idx].value, columns_3[idx].value);
        assert_eq!(columns_1[idx].data_type, columns_4[idx].data_type);
        assert_eq!(columns_1[idx].value, columns_4[idx].value);
    }

    Ok(())
}

/// Add more tests for take_compact.rs.
#[test]
pub fn test_take_compact() -> common_exception::Result<()> {
    use common_expression::types::decimal::DecimalSize;
    use common_expression::types::DataType;
    use common_expression::types::DecimalDataType;
    use common_expression::types::NumberDataType;
    use common_expression::BlockEntry;
    use common_expression::Column;
    use common_expression::DataBlock;
    use common_expression::Value;
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let data_types = vec![
        DataType::Null,
        DataType::EmptyArray,
        DataType::EmptyMap,
        DataType::Boolean,
        DataType::String,
        DataType::Bitmap,
        DataType::Variant,
        DataType::Timestamp,
        DataType::Date,
        DataType::Number(NumberDataType::UInt8),
        DataType::Number(NumberDataType::UInt16),
        DataType::Number(NumberDataType::UInt32),
        DataType::Number(NumberDataType::UInt64),
        DataType::Number(NumberDataType::Int8),
        DataType::Number(NumberDataType::Int16),
        DataType::Number(NumberDataType::Int32),
        DataType::Number(NumberDataType::Int64),
        DataType::Number(NumberDataType::Float32),
        DataType::Number(NumberDataType::Float64),
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: 10,
            scale: 2,
        })),
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: 35,
            scale: 3,
        })),
        DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt32))),
        DataType::Nullable(Box::new(DataType::String)),
        DataType::Array(Box::new(DataType::Number(NumberDataType::UInt32))),
        DataType::Map(Box::new(DataType::Tuple(vec![
            DataType::Number(NumberDataType::UInt64),
            DataType::String,
        ]))),
    ];

    for _ in 0..rng.gen_range(5..30) {
        let len = rng.gen_range(5..100);

        let mut columns = Vec::with_capacity(data_types.len());
        for data_type in data_types.iter() {
            columns.push(Column::random(data_type, len));
        }

        let mut block_entries = Vec::with_capacity(data_types.len());
        for (col, data_type) in columns.into_iter().zip(data_types.iter()) {
            block_entries.push(BlockEntry::new(data_type.clone(), Value::Column(col)));
        }
        let block = DataBlock::new(block_entries, len);

        let mut count = 0;
        let mut take_indices = Vec::new();
        let mut take_compact_indices = Vec::new();
        for _ in 0..len {
            let batch_index = rng.gen_range(0..len);
            let batch_size = rng.gen_range(1..1025);
            count += batch_size;
            take_indices.extend(std::iter::repeat(batch_index as u32).take(batch_size));
            take_compact_indices.push((batch_index as u32, batch_size as u32));
        }
        let block_1 = block.take(&take_indices, &mut None)?;
        let block_2 = block.take_compacted_indices(&take_compact_indices, count)?;

        assert_eq!(block_1.num_columns(), block_2.num_columns());
        assert_eq!(block_1.num_rows(), block_2.num_rows());

        let columns_1 = block_1.columns();
        let columns_2 = block_2.columns();
        for idx in 0..columns_1.len() {
            assert_eq!(columns_1[idx].data_type, columns_2[idx].data_type);
            assert_eq!(columns_1[idx].value, columns_2[idx].value);
        }
    }

    Ok(())
}

/// Test filter boolean when offset != 0.
#[test]
pub fn test_filter_boolean() -> common_exception::Result<()> {
    use common_expression::types::DataType;
    use common_expression::types::NumberDataType;
    use common_expression::BlockEntry;
    use common_expression::Column;
    use common_expression::DataBlock;
    use common_expression::Value;
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let num_blocks = rng.gen_range(5..30);
    let data_types = vec![
        DataType::Boolean,
        DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt32))),
    ];

    let mut take_indices = Vec::new();
    let mut idx = 0;
    let mut blocks = Vec::with_capacity(data_types.len());
    let mut filtered_blocks = Vec::with_capacity(data_types.len());
    for _ in 0..num_blocks {
        let len = rng.gen_range(5..100);
        let offset = rng.gen_range(0..std::cmp::min(7, len - 1));
        let mut filter = Column::random(&DataType::Boolean, len)
            .into_boolean()
            .unwrap();
        filter.slice(offset, len - offset);

        let mut columns = Vec::with_capacity(data_types.len());
        for data_type in data_types.iter() {
            let column = Column::random(data_type, len);
            columns.push(column.slice(offset..len));
        }

        let mut block_entries = Vec::with_capacity(data_types.len());
        let mut filtered_block_entries = Vec::with_capacity(data_types.len());
        for (col, data_type) in columns.into_iter().zip(data_types.iter()) {
            filtered_block_entries.push(BlockEntry::new(
                data_type.clone(),
                Value::Column(col.filter(&filter)),
            ));
            block_entries.push(BlockEntry::new(data_type.clone(), Value::Column(col)));
        }

        blocks.push(DataBlock::new(block_entries, len - offset));
        filtered_blocks.push(DataBlock::new(
            filtered_block_entries,
            len - offset - filter.unset_bits(),
        ));

        for val in filter.iter() {
            if val {
                take_indices.push(idx);
            }
            idx += 1;
        }
    }

    let block_1 = DataBlock::concat(&blocks)?.take(&take_indices, &mut None)?;
    let block_2 = DataBlock::concat(&filtered_blocks)?;

    assert_eq!(block_1.num_columns(), block_2.num_columns());
    assert_eq!(block_1.num_rows(), block_2.num_rows());

    let columns_1 = block_1.columns();
    let columns_2 = block_2.columns();
    for idx in 0..columns_1.len() {
        assert_eq!(columns_1[idx].data_type, columns_2[idx].data_type);
        assert_eq!(columns_1[idx].value, columns_2[idx].value);
    }

    Ok(())
}
