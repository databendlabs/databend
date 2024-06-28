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

use core::ops::Range;

use databend_common_expression::block_debug::assert_block_value_eq;
use databend_common_expression::types::number::*;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Value;
use goldenfile::Mint;

use crate::common::*;
use crate::get_all_test_data_types;
use crate::rand_block_for_all_types;

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

// Build a range selection from a selection array.
pub fn build_range_selection(selection: &[u32], count: usize) -> Vec<Range<u32>> {
    let mut range_selection = Vec::with_capacity(count);
    let mut start = selection[0];
    let mut idx = 1;
    while idx < count {
        if selection[idx] != selection[idx - 1] + 1 {
            range_selection.push(start..selection[idx - 1] + 1);
            start = selection[idx];
        }
        idx += 1;
    }
    range_selection.push(start..selection[count - 1] + 1);
    range_selection
}

/// This test covers take.rs, take_chunks.rs, take_compact.rs, take_ranges.rs, filter.rs, concat.rs.
#[test]
pub fn test_take_and_filter_and_concat() -> databend_common_exception::Result<()> {
    use databend_common_expression::types::DataType;
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_hashtable::RowPtr;
    use itertools::Itertools;
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let num_blocks = rng.gen_range(5..30);
    let data_types: Vec<DataType> = get_all_test_data_types();

    let mut count = 0;
    let mut take_indices = Vec::new();
    let mut take_chunks_indices = Vec::new();
    let mut take_compact_indices = Vec::new();
    let mut idx = 0;
    let mut blocks = Vec::with_capacity(num_blocks);
    let mut filtered_blocks = Vec::with_capacity(data_types.len());

    for i in 0..num_blocks {
        let len = rng.gen_range(2..100);
        let slice_start = rng.gen_range(0..len - 1);
        let slice_end = rng.gen_range(slice_start..len);
        let slice_len = slice_end - slice_start;

        let mut filter = Column::random(&DataType::Boolean, len, None)
            .into_boolean()
            .unwrap();
        filter.slice(slice_start, slice_len);

        let random_block = rand_block_for_all_types(len);
        let random_block = random_block.slice(slice_start..slice_end);

        filtered_blocks.push(random_block.clone().filter_with_bitmap(&filter)?);

        blocks.push(random_block);

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
    let block_5 = concated_blocks.take_ranges(
        &build_range_selection(&take_indices, take_indices.len()),
        take_indices.len(),
    )?;

    assert_eq!(block_1.num_columns(), block_2.num_columns());
    assert_eq!(block_1.num_rows(), block_2.num_rows());
    assert_eq!(block_1.num_columns(), block_3.num_columns());
    assert_eq!(block_1.num_rows(), block_3.num_rows());
    assert_eq!(block_1.num_columns(), block_4.num_columns());
    assert_eq!(block_1.num_rows(), block_4.num_rows());
    assert_eq!(block_1.num_columns(), block_5.num_columns());
    assert_eq!(block_1.num_rows(), block_5.num_rows());

    let columns_1 = block_1.columns();
    let columns_2 = block_2.columns();
    let columns_3 = block_3.columns();
    let columns_4 = block_4.columns();
    let columns_5 = block_5.columns();
    for idx in 0..columns_1.len() {
        assert_eq!(columns_1[idx].data_type, columns_2[idx].data_type);
        assert_eq!(columns_1[idx].value, columns_2[idx].value);
        assert_eq!(columns_1[idx].data_type, columns_3[idx].data_type);
        assert_eq!(columns_1[idx].value, columns_3[idx].value);
        assert_eq!(columns_1[idx].data_type, columns_4[idx].data_type);
        assert_eq!(columns_1[idx].value, columns_4[idx].value);
        assert_eq!(columns_1[idx].data_type, columns_5[idx].data_type);
        assert_eq!(columns_1[idx].value, columns_5[idx].value);
    }

    Ok(())
}

#[test]
pub fn test_concat_scalar() -> databend_common_exception::Result<()> {
    use databend_common_expression::types::DataType;
    use databend_common_expression::DataBlock;
    use databend_common_expression::Scalar;
    use databend_common_expression::Value;

    let ty = DataType::Number(NumberDataType::UInt8);
    let scalar = Value::Scalar(Scalar::Number(NumberScalar::UInt8(1)));
    let column = Value::Column(UInt8Type::from_data(vec![2, 3]));

    let blocks = [
        DataBlock::new(
            vec![
                BlockEntry::new(ty.clone(), scalar.clone()),
                BlockEntry::new(ty.clone(), scalar.clone()),
            ],
            2,
        ),
        DataBlock::new(
            vec![
                BlockEntry::new(ty.clone(), scalar.clone()),
                BlockEntry::new(ty.clone(), column),
            ],
            2,
        ),
    ];
    let block = DataBlock::concat(&blocks)?;
    let expect = DataBlock::new(
        vec![
            BlockEntry::new(ty.clone(), scalar.clone()),
            BlockEntry::new(
                ty.clone(),
                Value::Column(UInt8Type::from_data(vec![1, 1, 2, 3])),
            ),
        ],
        4,
    );
    assert_eq!(block.columns(), expect.columns());
    assert_eq!(block.num_rows(), expect.num_rows());
    Ok(())
}

/// Add more tests for take_compact.rs.
#[test]
pub fn test_take_compact() -> databend_common_exception::Result<()> {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    for _ in 0..rng.gen_range(5..30) {
        let len = rng.gen_range(5..100);

        let block = rand_block_for_all_types(len);

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

/// Random Block A
/// +----+----+----+----+----+----+----+----+----+----+
/// B = A + A + A,  l = A.len()
/// B.slice(0, l) == B.slice(l, l) == A
#[test]
pub fn test_filters() -> databend_common_exception::Result<()> {
    use databend_common_expression::types::DataType;
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let num_rows = 24;
    let blocks = 10;
    for _ in 0..rng.gen_range(5..30) {
        let a = rand_block_for_all_types(num_rows);
        let b = DataBlock::concat(&vec![a.clone(); blocks])?;
        let c = b.clone();

        assert!(c.num_rows() == a.num_rows() * blocks);
        assert!(c.num_columns() == a.num_columns());

        // slice and filters and take
        for i in 0..blocks - 1 {
            let offset = rng.gen_range(0..num_rows);

            let start = i * num_rows + offset;
            let end = (start + rng.gen_range(1..num_rows)).min(c.num_rows() - a.num_rows());

            // random filter
            let mut f = Column::random(&DataType::Boolean, num_rows * blocks, None)
                .into_boolean()
                .unwrap();
            f.slice(start, end - start);

            let bb = b.slice(start..end);
            let cc = c.slice(a.num_rows()..c.num_rows()).slice(start..end);

            assert_eq!(f.len(), end - start);
            assert_eq!(f.len(), bb.num_rows());
            assert_eq!(f.len(), cc.num_rows());

            assert_block_value_eq(&bb, &cc);

            let indices = f
                .iter()
                .enumerate()
                .filter(|(_, v)| *v)
                .map(|(i, _)| i as u32)
                .collect::<Vec<_>>();

            let t_b = bb.take(&indices, &mut None)?;
            let t_c = cc.take(&indices, &mut None)?;

            let f_b = bb.filter_with_bitmap(&f)?;
            let f_c = cc.filter_with_bitmap(&f)?;

            assert_block_value_eq(&f_b, &f_c);

            assert_block_value_eq(&f_b, &t_b);
            assert_block_value_eq(&f_c, &t_c);
        }
    }

    Ok(())
}

#[test]
pub fn test_divide_indices_by_scatter_size() -> databend_common_exception::Result<()> {
    use databend_common_expression::DataBlock;
    use itertools::Itertools;
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let num_blocks = rng.gen_range(5..30);

    for _ in 0..num_blocks {
        let len = rng.gen_range(2..100);
        let scatter_size = rng.gen_range(2..50);

        let random_indices = (0..len)
            .map(|_| rng.gen_range(0..scatter_size) as u32)
            .collect_vec();
        let scatter_indices =
            DataBlock::divide_indices_by_scatter_size(&random_indices, scatter_size);
        let mut blocks_idx = 0;
        let mut row_idx = 0;

        for i in 0..scatter_size as u32 {
            for (j, index) in random_indices.iter().enumerate() {
                if *index == i {
                    while row_idx == scatter_indices[blocks_idx].len() {
                        blocks_idx += 1;
                        assert!(blocks_idx < scatter_indices.len());
                        row_idx = 0;
                    }
                    assert_eq!(j as u32, scatter_indices[blocks_idx][row_idx]);
                    row_idx += 1;
                }
            }
        }
    }

    Ok(())
}

/// This test covers scatter.rs.
#[test]
pub fn test_scatter() -> databend_common_exception::Result<()> {
    use databend_common_expression::DataBlock;
    use itertools::Itertools;
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let num_blocks = rng.gen_range(5..30);

    for _ in 0..num_blocks {
        let len = rng.gen_range(2..300);
        let slice_start = rng.gen_range(0..len - 1);
        let slice_end = rng.gen_range(slice_start..len);
        let scatter_size = rng.gen_range(2..25);

        let random_block = rand_block_for_all_types(len);
        let random_block = random_block.slice(slice_start..slice_end);
        let len = slice_end - slice_start;

        let random_indices: Vec<u32> = (0..len)
            .map(|_| rng.gen_range(0..scatter_size))
            .collect_vec();
        let scattered_blocks = random_block.scatter(&random_indices, scatter_size as usize)?;

        let mut take_indices = Vec::with_capacity(len);
        for i in 0..scatter_size {
            for (j, index) in random_indices.iter().enumerate() {
                if *index == i {
                    take_indices.push(j as u32);
                }
            }
        }

        let block_1 = random_block.take(&take_indices, &mut None)?;
        let block_2 = DataBlock::concat(&scattered_blocks)?;

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
