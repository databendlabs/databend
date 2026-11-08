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

use std::collections::HashSet;

use databend_common_expression::BlockEntry;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;

fn nullable_int64() -> DataType {
    DataType::Number(NumberDataType::Int64).wrap_nullable()
}

/// An Int32 column 0..rows starting at `start`, a constant NULL and a
/// constant string.
fn const_block(rows: usize, start: i32, text: &str) -> DataBlock {
    DataBlock::new(
        vec![
            Int32Type::from_data((start..start + rows as i32).collect::<Vec<_>>()).into(),
            BlockEntry::new_const_column(nullable_int64(), Scalar::Null, rows),
            BlockEntry::new_const_column(DataType::String, Scalar::String(text.to_string()), rows),
        ],
        rows,
    )
}

fn column_block(rows: usize, start: i32) -> DataBlock {
    DataBlock::new(
        vec![
            Int32Type::from_data((start..start + rows as i32).collect::<Vec<_>>()).into(),
            Int64Type::from_opt_data((0..rows as i64).map(Some).collect()).into(),
            StringType::from_data((0..rows).map(|i| i.to_string()).collect::<Vec<_>>()).into(),
        ],
        rows,
    )
}

/// The block with every constant expanded, as the stream used to copy it.
fn expanded(block: &DataBlock) -> DataBlock {
    let columns = block
        .columns()
        .iter()
        .map(|entry| entry.to_column().into())
        .collect();
    DataBlock::new(columns, block.num_rows())
}

fn assert_same_values(actual: &DataBlock, expected: &DataBlock) {
    assert_eq!(actual.num_rows(), expected.num_rows());
    assert_eq!(actual.num_columns(), expected.num_columns());
    for (a, e) in actual.columns().iter().zip(expected.columns()) {
        assert_eq!(a.data_type(), e.data_type());
        assert_eq!(a.to_column(), e.to_column());
    }
}

/// Partition every block with `indices_of`, into a stream fed the blocks as
/// they are and one fed them expanded, and check that both cut the same
/// blocks with the same values.
fn assert_same_as_expanded(
    blocks: &[DataBlock],
    indices_of: impl Fn(usize) -> u64,
    rows_threshold: usize,
    bytes_threshold: usize,
) -> Vec<(usize, DataBlock)> {
    let mut stream = BlockPartitionStream::create(rows_threshold, bytes_threshold, 2);
    let mut reference = BlockPartitionStream::create(rows_threshold, bytes_threshold, 2);
    let mut output = Vec::new();
    for block in blocks {
        let indices = (0..block.num_rows()).map(&indices_of).collect::<Vec<_>>();
        let actual = stream.partition(indices.clone(), block.clone(), true);
        let expected = reference.partition(indices, expanded(block), true);
        assert_eq!(actual.len(), expected.len());
        for ((id, a), (expected_id, e)) in actual.iter().zip(expected.iter()) {
            assert_eq!(id, expected_id);
            assert_same_values(a, e);
        }
        output.extend(actual);
    }
    for id in 0..2 {
        let actual = stream.finalize_partition(id);
        let expected = reference.finalize_partition(id);
        assert_eq!(actual.is_some(), expected.is_some());
        if let (Some(a), Some(e)) = (&actual, &expected) {
            assert_same_values(a, e);
        }
        output.extend(actual.map(|block| (id, block)));
    }
    output
}

#[test]
fn test_partition_keeps_constants() {
    let blocks = vec![const_block(5, 0, "x"), const_block(7, 5, "x")];
    let output = assert_same_as_expanded(&blocks, |row| (row % 2) as u64, 0, 0);
    assert_eq!(output.len(), 2);
    for (_, block) in &output {
        assert!(block.get_by_offset(0).as_column().is_some());
        assert_eq!(block.get_by_offset(1).as_scalar(), Some(&Scalar::Null));
        assert_eq!(
            block.get_by_offset(2).as_scalar(),
            Some(&Scalar::String("x".to_string()))
        );
    }
}

#[test]
fn test_partition_expands_mixed_rows() {
    // A constant followed by a column, and two different constants.
    let blocks = vec![
        const_block(4, 0, "x"),
        column_block(3, 4),
        const_block(2, 7, "y"),
    ];
    let output = assert_same_as_expanded(&blocks, |_| 0, 0, 0);
    let (_, block) = &output[0];
    assert_eq!(block.num_rows(), 9);
    assert!(block.get_by_offset(1).as_column().is_some());
    assert!(block.get_by_offset(2).as_column().is_some());
    assert_eq!(
        block.get_by_offset(1).to_column(),
        Int64Type::from_opt_data(vec![
            None,
            None,
            None,
            None,
            Some(0),
            Some(1),
            Some(2),
            None,
            None
        ])
    );
    assert_eq!(
        block.get_by_offset(2).to_column(),
        StringType::from_data(vec!["x", "x", "x", "x", "0", "1", "2", "y", "y"])
    );
}

#[test]
fn test_partition_constant_after_output() {
    // A block cut by the row threshold leaves the stream free to keep the
    // constant of the next one.
    let blocks = vec![column_block(4, 0), const_block(4, 4, "x")];
    let output = assert_same_as_expanded(&blocks, |_| 1, 4, 0);
    assert_eq!(output.len(), 2);
    assert!(output[0].1.get_by_offset(1).as_column().is_some());
    assert_eq!(
        output[1].1.get_by_offset(1).as_scalar(),
        Some(&Scalar::Null)
    );
}

#[test]
fn test_partition_cuts_constants_by_expanded_size() {
    // Cut by bytes only: constants count as if expanded, so blocks are cut
    // where they were when constants were copied.
    let blocks = (0..12)
        .map(|i| const_block(64, i * 64, "constant text longer than inlined"))
        .collect::<Vec<_>>();
    let one_block = expanded(&blocks[0]).memory_size();
    let output = assert_same_as_expanded(&blocks, |row| (row % 2) as u64, 0, one_block * 2);
    assert!(output.len() > 2);
    assert!(
        output
            .iter()
            .all(|(_, block)| block.get_by_offset(2).as_scalar().is_some())
    );
}

#[test]
fn test_partition_null_type_turns_nullable() {
    // A column typed Null in the first block and Nullable in later ones.
    let null_block = DataBlock::new(
        vec![BlockEntry::new_const_column(
            DataType::Null,
            Scalar::Null,
            3,
        )],
        3,
    );
    let nullable_block = DataBlock::new(
        vec![Int64Type::from_opt_data(vec![Some(1), None]).into()],
        2,
    );
    let blocks = vec![null_block.clone(), nullable_block, null_block];
    let output = assert_same_as_expanded(&blocks, |_| 0, 0, 0);
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].1.get_by_offset(0).data_type(), nullable_int64());
}

#[test]
fn test_take_partitions_keeps_constants() {
    let mut stream = BlockPartitionStream::create(0, 0, 3);
    let block = const_block(6, 0, "x");
    let indices = vec![0, 1, 0, 1, 0, 1];
    assert!(stream.partition(indices, block, false).is_empty());
    assert_eq!(stream.partition_ids(), vec![0, 1]);

    let taken = stream.take_partitions(&HashSet::from([1]));
    assert_eq!(taken.len(), 2);
    let (id, block) = &taken[0];
    assert_eq!((*id, block.num_rows()), (0, 3));
    assert_eq!(block.get_by_offset(1).as_scalar(), Some(&Scalar::Null));
    // A partition with no rows is taken as an empty block.
    let (id, block) = &taken[1];
    assert_eq!((*id, block.num_rows()), (2, 0));
    assert_eq!(stream.partition_ids(), vec![1]);
    assert_eq!(stream.finalize_partition(1).unwrap().num_rows(), 3);
}
