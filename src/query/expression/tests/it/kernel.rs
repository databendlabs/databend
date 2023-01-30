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

use std::io::Write;

use common_arrow::arrow::compute::merge_sort::MergeSlice;
use common_expression::types::number::*;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::BlockEntry;
use common_expression::BlockRowIndex;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::Value;
use common_expression::types::string::StringColumn;
use goldenfile::Mint;
use rand::RngCore;

use crate::common::new_block;

#[test]
pub fn test_tmp(){
    let offsets: Vec<u64> = (0..=2048).collect();
    let block = new_block(&[
            Column::String(StringColumn { data: vec![1u8; 2048].into(), offsets: offsets.into() })
    ]);
    let mut rng = rand::thread_rng();
    
    for i in 0..1000 {
        let scatter_size = 16;
        // let indices = (0..2048).map(|i| ( rng.next_u32() % scatter_size as u32) as u32).collect::<Vec<_>>();
    let indices = [9, 8, 0, 1, 12, 6, 10, 1, 2, 12, 4, 10, 13, 8, 5, 12, 10, 9, 4, 0, 11, 14, 4, 14, 11, 10, 0, 3, 3, 2, 8, 12, 7, 11, 3, 3, 13, 10, 0, 15, 7, 3, 8, 15, 0, 9, 10, 7, 2, 8, 11, 14, 8, 14, 3, 15, 3, 15, 11, 5, 12, 8, 11, 0, 9, 12, 3, 1, 5, 2, 5, 10, 6, 3, 5, 9, 3, 15, 3, 6, 5, 4, 11, 7, 6, 6, 10, 14, 6, 5, 1, 5, 4, 12, 4, 1, 10, 15, 2, 4, 15, 9, 8, 14, 13, 15, 14, 8, 7, 1, 7, 1, 4, 14, 15, 2, 10, 11, 1, 12, 4, 8, 15, 9, 0, 10, 5, 6, 4, 14, 0, 6, 5, 13, 8, 13, 6, 0, 1, 6, 10, 5, 14, 2, 11, 15, 11, 7, 14, 3, 8, 13, 4, 15, 10, 13, 4, 10, 10, 11, 9, 13, 6, 15, 4, 12, 2, 14, 5, 4, 14, 2, 9, 10, 13, 0, 6, 10, 11, 15, 11, 6, 3, 6, 14, 5, 9, 12, 1, 4, 5, 6, 2, 11, 2, 0, 12, 14, 15, 5, 13, 9, 11, 8, 11, 6, 15, 8, 14, 15, 14, 10, 13, 10, 7, 2, 11, 7, 6, 10, 13, 8, 3, 5, 1, 0, 5, 1, 11, 4, 0, 3, 13, 2, 6, 10, 2, 14, 2, 0, 13, 12, 1, 7, 15, 12, 11, 8, 12, 12, 5, 8, 11, 10, 14, 7, 12, 11, 2, 15, 3, 13, 4, 14, 8, 0, 12, 7, 2, 7, 6, 5, 3, 0, 10, 8, 7, 6, 6, 10, 9, 4, 15, 1, 7, 8, 6, 11, 0, 15, 12, 0, 10, 9, 9, 6, 7, 1, 10, 13, 10, 9, 10, 9, 3, 4, 3, 10, 0, 14, 10, 14, 15, 0, 15, 11, 13, 12, 9, 0, 4, 11, 4, 8, 15, 3, 3, 5, 5, 0, 3, 11, 3, 11, 7, 8, 2, 13, 13, 0, 3, 9, 14, 7, 6, 3, 9, 6, 11, 10, 9, 1, 1, 15, 1, 9, 3, 3, 14, 14, 12, 0, 14, 2, 8, 12, 1, 10, 0, 6, 12, 1, 8, 2, 8, 1, 4, 8, 0, 1, 9, 11, 10, 12, 8, 3, 9, 7, 13, 10, 14, 11, 8, 4, 6, 2, 8, 15, 1, 7, 5, 15, 0, 7, 15, 9, 10, 12, 13, 3, 5, 5, 2, 6, 7, 3, 14, 12, 12, 14, 12, 5, 5, 14, 10, 4, 4, 3, 12, 7, 9, 0, 2,14, 7, 13, 4, 14, 6, 4, 10, 10, 2, 0, 14, 13, 4, 4, 15, 14, 11, 0, 9, 4, 10, 12, 12, 6, 9, 12, 12, 13, 11, 15, 9, 12, 10, 11, 3, 10, 4, 12, 0, 6, 6, 14, 6, 2, 0, 6, 8, 2, 7, 0, 2, 3, 11, 15, 3, 12, 11, 11, 3, 2, 13, 6, 14, 10, 15, 1, 7, 11, 6, 14, 3, 11, 5, 1, 2, 12, 1, 7, 5, 8, 11, 14, 8, 9, 4, 6, 2, 8, 0, 14, 6, 3, 7, 1, 1, 0, 9, 6, 6, 3, 1, 1, 13, 2, 0, 2, 8, 2, 14, 10, 10, 2, 6, 6, 12, 2, 11, 12, 9, 10, 3, 14, 15, 5, 5, 8, 10, 1, 5, 1, 4, 1, 14, 0, 15, 3, 10, 0, 5, 0, 3, 2, 0, 2, 15, 12, 8, 9, 7, 7, 7, 1, 7, 4, 11, 8, 8, 10, 7, 3, 4, 6, 3, 12, 8, 12, 10, 7, 13, 6, 14, 9, 13, 13, 0, 8, 3, 0, 13, 1, 15, 13, 8, 1, 9, 10, 3, 8, 14, 7, 14, 0, 7, 14, 11, 8, 1, 5, 4, 14, 2, 15, 1, 5, 3, 8, 8, 11, 9, 3, 1, 8, 2, 13, 1, 1, 15, 13, 11,15, 1, 6, 9, 4, 11, 10, 11, 3, 6, 2, 2, 8, 1, 4, 5, 5, 8, 9, 3, 1, 3, 5, 6, 8, 9, 0, 1, 7, 7, 4, 9, 0, 8, 2, 8, 15, 10, 14, 8, 10, 10, 13, 8, 6, 14, 1, 2, 8, 5, 3, 11, 15, 9, 2, 0, 1, 0, 0, 1, 3, 8, 14, 13, 5, 12, 1, 3, 15, 1, 10, 3, 8, 0, 4, 5, 4, 4, 1, 0, 2, 13, 8, 5, 13, 13, 13, 15, 3, 0, 0, 2, 5, 1, 6, 9, 3, 8, 10, 1, 1, 7, 5, 9, 7, 5, 15, 3, 11, 11, 1, 8, 7, 8, 1, 8, 15, 9, 0, 14, 11, 8, 5, 7, 6, 1, 7, 9, 15, 4, 12, 1, 7, 0, 12, 10, 1, 6, 11, 6, 2, 6, 2, 3, 8, 13, 9, 4, 3, 12, 7, 12, 10, 7, 0, 12, 2, 11, 0, 6, 8, 5, 12, 10, 8, 5, 12, 9, 2, 5, 13, 13, 11, 6, 1, 8, 5, 6, 12, 10, 14, 4, 3, 13, 5, 3, 13, 4, 9, 12, 8, 7, 15, 7, 8, 13, 3, 15, 1, 11, 1, 2, 2, 8, 9, 1, 13, 7, 3, 12, 8, 5, 11, 0, 7, 15, 0, 13, 5, 9, 3, 10, 5, 4, 12, 12, 10, 10, 0, 4, 13, 15, 7, 8, 7, 4, 14, 12, 3, 3, 10, 0, 3, 2, 12, 0, 3, 12, 4, 3, 4, 14, 1, 1, 0, 7, 11, 5, 9, 15, 15, 15, 12, 13, 2, 1, 12, 1, 3, 10, 4, 7, 10, 4, 0, 11, 14, 12, 2, 13, 12, 3, 7, 15, 0, 12, 15, 4, 15, 13, 13, 0, 8, 8, 13, 6, 8, 1, 11, 8, 10, 12, 8, 15, 4, 8, 3, 6, 4, 6, 3, 10, 13, 4, 15, 1, 11, 9, 12, 4, 0, 2, 8, 12, 3, 7, 14, 7, 5, 9, 6, 15, 11, 11, 6, 10, 3, 10, 0, 2, 7, 8, 0, 11, 0, 3, 11, 5, 0, 4, 15, 3, 13, 12, 5, 3, 3, 7, 9, 2, 15, 6, 9, 0, 12, 6, 8, 15, 3, 8, 0, 12, 15, 14, 15, 4, 8, 2, 2, 15, 3, 6, 9, 7, 8, 9, 13, 10, 2, 8, 1, 13, 6, 3, 15, 11, 11, 2, 4, 15, 4, 5, 11, 15, 10, 7, 6, 10, 13, 13, 8, 13, 1, 14, 15, 11, 3, 11, 12, 7, 10, 8, 3, 10, 5, 11, 9, 13, 3, 9, 11, 0, 11, 3, 3, 2, 11, 12, 4, 13, 8, 6, 3, 9, 4, 5, 1, 8, 12, 15, 7, 11, 4, 7, 13, 5, 13, 4, 7, 2, 2, 0, 2, 15, 7, 14, 7, 5, 0, 15, 2, 1, 9, 15, 9, 13, 7, 7, 15, 4, 4, 12, 4, 0, 12, 15, 11, 15, 15, 15, 6, 15, 3, 14, 7, 14, 2,13, 2, 15, 3, 4, 2, 5, 9, 14, 10, 14, 5, 11, 6, 8, 12, 11, 7, 4, 5, 9, 10, 8, 11, 13, 6, 11, 12, 13, 3, 12, 8, 6, 6, 12, 13, 8, 4, 15, 10, 3, 13, 8, 0, 14, 9, 15, 15, 7, 14, 2, 6, 1, 2, 3, 3, 8, 12, 7, 0, 12, 0, 2, 13, 2, 15, 15, 2, 5, 2, 1, 14, 1, 6, 11, 8, 8, 7, 9, 9, 13, 12, 15, 6, 10, 10, 13, 2, 13, 2, 2, 13, 6, 5, 1, 0, 7, 12, 5, 7, 6, 0, 3, 2, 4, 6, 4, 10, 10, 1, 14, 1, 2, 13, 11, 5, 13, 10, 6, 4, 4, 9, 11, 15, 4, 2, 5, 9, 15, 9, 2, 6, 5, 10, 10, 0, 12, 12, 7, 8, 14, 1, 13, 9, 0, 4, 6, 12, 5, 12, 9, 10, 14, 1, 12, 11, 9, 15, 11, 3, 7, 13, 11,4, 10, 14, 10, 3, 0, 6, 11, 9, 3, 13, 5, 4, 3, 5, 5, 1, 8, 11, 15, 11, 2, 2, 10, 1, 5, 15, 4, 11, 3, 2, 2, 0, 1, 15, 4, 14, 6, 11, 1, 11, 8, 14, 3, 12, 12, 7, 3, 3, 4, 9, 5, 15, 1, 1, 2, 11, 10, 5, 15, 14, 15, 14, 1, 10, 11, 8, 5, 13, 5, 14, 14, 3, 9, 15, 13, 5, 8, 10, 0, 13, 9, 7, 15, 10, 13, 4, 7, 4, 7, 2, 5, 5, 10, 9, 13, 7, 13, 8, 13, 14, 11, 2, 13, 5, 5, 8, 5, 12, 13, 9, 0, 5, 15, 12, 5, 11, 3, 4, 9, 11, 5, 1, 15, 2, 2, 5, 12, 4, 7, 5, 1, 1, 14, 8, 11, 12, 11, 7, 14, 9, 11, 10, 3, 3, 5, 0, 6, 8, 5, 5, 2, 11, 14, 14, 5, 11, 15, 2, 15, 6, 1, 11,0, 10, 13, 14, 13, 7, 14, 10, 10, 7, 0, 12, 10, 13, 7, 9, 5, 7, 15, 0, 0, 6, 3, 3, 7, 1, 8, 15, 6, 14, 1, 1, 11, 12, 6, 6, 5, 9, 14, 6, 15, 13, 2, 4, 1, 3, 14, 3, 3, 13, 5, 0, 10, 8, 11, 12, 9, 1, 5, 2, 9, 11, 5, 7, 2, 15, 2, 6, 7, 2, 4, 11, 12, 2, 15, 11, 15, 8, 3, 15, 11, 11, 9, 1, 8, 1, 5, 13, 4, 1, 14, 12, 14, 9, 8, 1, 13, 2, 9, 3, 4, 3, 4, 2, 7, 15, 15, 15, 2, 8, 11, 12, 3, 14, 3, 3, 2, 15, 10, 4, 1, 11, 13, 1, 4, 6, 10, 14, 10, 14, 11, 14, 1, 5, 8, 0, 5, 8, 8, 8, 7, 3, 13, 12, 4, 10, 5, 14, 12, 15, 12, 8, 6, 6, 3, 7, 4, 1, 14, 10, 15, 12, 12,11, 15, 4, 14, 8, 2, 13, 4, 3, 12, 12, 5, 6, 8, 3, 13, 0, 9, 14, 11, 7, 7, 8, 6, 0, 1, 14, 10, 9, 0, 6, 10, 8, 9, 6, 8, 9, 4, 4, 4, 1, 12, 0, 5, 0, 9, 6, 7, 9, 11, 11, 8, 0, 12, 10, 14, 11, 1, 8, 7, 7, 9, 8, 5, 8, 1, 0, 9, 3, 4, 3, 5, 10, 2, 14, 0, 14, 1, 7, 12, 7, 11, 6, 10, 5, 3, 6, 15, 10, 13, 5, 12, 13, 15, 15, 15, 9, 14, 4, 15, 3, 13, 5, 15, 4, 12, 13, 12, 10, 7, 14, 9, 5, 0, 7, 9, 4, 7, 5, 0, 1, 3, 13, 4, 3, 3, 15, 4, 13, 8, 4, 1, 10, 0, 11, 8, 6, 15, 8, 4, 2, 1, 6, 6, 15, 5, 6, 1, 8, 0, 5, 8, 11, 4, 5, 5, 2, 8, 9, 6, 12, 15, 0, 6, 10, 4, 10,5, 1, 7, 3, 4, 4, 11, 12, 0, 7, 15, 6, 2, 13, 3, 15, 13, 14, 2, 7, 2, 4, 12, 7, 7, 4, 3, 6, 7, 6, 6, 2, 11, 11, 1, 7, 9, 3, 10, 13, 0, 1, 9, 13, 0, 0, 14, 1, 2, 7, 8, 10, 1, 15, 13, 1,15, 7, 8, 4, 11, 3, 11, 12, 4, 13, 9, 6, 12, 8, 8, 2, 14, 1, 12, 5, 2, 5, 5, 13, 13, 10, 10, 12, 15, 9, 0, 7, 5, 7, 15, 12, 7, 11, 14, 9, 3, 15, 11, 8, 9, 6, 8, 4, 3, 14, 13, 15, 13, 10, 6, 11, 2, 2, 7, 15, 7, 9, 6, 6, 14, 9, 15, 14, 1, 0, 3, 2, 0, 9, 15, 10, 1, 15, 8, 9, 2, 2, 6, 14, 4, 8, 4, 14, 3, 7, 10, 8, 6, 4, 7, 5, 10, 2, 3, 10, 13, 2, 12, 14, 1, 7, 6, 13, 3, 8, 14, 8, 4, 10, 10, 11, 8, 2, 0, 13, 14, 2, 7, 2, 8, 9, 10, 9, 2, 6, 10, 5, 5, 9, 11, 9, 8, 7, 15, 3, 5, 0, 10, 11, 13, 10, 15, 0, 12, 5, 0, 12, 0, 3, 1, 4, 1, 15, 0, 5, 1, 15, 1, 5, 10, 10, 4, 1, 10, 1, 8, 12, 11, 2, 8, 3, 8, 3, 11, 5, 12, 1, 4, 6, 15, 0];
        let result = DataBlock::scatter(&block, &indices, scatter_size).unwrap();
        let sum_rows = result.iter().map(|b| b.num_rows()).sum::<usize>();
        assert!(sum_rows == block.num_rows());
    }
}

#[test]
pub fn test_pass() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("kernel-pass.txt").unwrap();

    for filter in vec![
        BooleanType::from_data(vec![true, false, false, false, true]),
        BooleanType::from_data_with_validity(vec![true, true, false, true, true], vec![
            false, true, true, false, false,
        ]),
        StringType::from_data(vec!["a", "b", "", "", "c"]),
        Int32Type::from_data(vec![0, 1, 2, 3, 0]),
    ] {
        run_filter(
            &mut file,
            filter,
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
    }

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
            columns.push(BlockEntry {
                data_type: DataType::Number(NumberDataType::UInt8),
                value: Value::Column(UInt8Type::from_data(vec![(i + 10) as u8; 4])),
            });
            columns.push(BlockEntry {
                data_type: DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt8))),
                value: Value::Column(UInt8Type::from_data_with_validity(
                    vec![(i + 10) as u8; 4],
                    vec![true, true, false, false],
                )),
            });
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
            columns.push(BlockEntry {
                data_type: DataType::Number(NumberDataType::UInt8),
                value: Value::Column(UInt8Type::from_data(vec![(i + 10) as u8; 4])),
            });
            columns.push(BlockEntry {
                data_type: DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt8))),
                value: Value::Column(UInt8Type::from_data_with_validity(
                    vec![(i + 10) as u8; 4],
                    vec![true, true, false, false],
                )),
            });
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

fn run_filter(file: &mut impl Write, predicate: Column, block: &DataBlock) {
    let predicate = Value::Column(predicate);
    let result = block.clone().filter(&predicate);

    match result {
        Ok(result_block) => {
            writeln!(file, "Filter:         {predicate}").unwrap();
            writeln!(file, "Source:\n{block}").unwrap();
            writeln!(file, "Result:\n{result_block}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

fn run_concat(file: &mut impl Write, blocks: &[DataBlock]) {
    let result = DataBlock::concat(blocks);

    match result {
        Ok(result_block) => {
            for (i, c) in blocks.iter().enumerate() {
                writeln!(file, "Concat-Column {}:", i).unwrap();
                writeln!(file, "{:?}", c).unwrap();
            }
            writeln!(file, "Result:\n{result_block}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

fn run_take(file: &mut impl Write, indices: &[u32], block: &DataBlock) {
    let result = DataBlock::take(block, indices);

    match result {
        Ok(result_block) => {
            writeln!(file, "Take:         {indices:?}").unwrap();
            writeln!(file, "Source:\n{block}").unwrap();
            writeln!(file, "Result:\n{result_block}").unwrap();
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}

fn run_take_block(file: &mut impl Write, indices: &[BlockRowIndex], blocks: &[DataBlock]) {
    let result = DataBlock::take_blocks(blocks, indices);
    writeln!(file, "Take Block indices:         {indices:?}").unwrap();
    for (i, block) in blocks.iter().enumerate() {
        writeln!(file, "Block{i}:\n{block}").unwrap();
    }
    writeln!(file, "Result:\n{result}").unwrap();
    write!(file, "\n\n").unwrap();
}

fn run_take_block_by_slices_with_limit(
    file: &mut impl Write,
    slices: &[MergeSlice],
    blocks: &[DataBlock],
    limit: Option<usize>,
) {
    let result = DataBlock::take_by_slices_limit_from_blocks(blocks, slices, limit);
    writeln!(
        file,
        "Take Block by slices (limit: {limit:?}):       {slices:?}"
    )
    .unwrap();
    for (i, block) in blocks.iter().enumerate() {
        writeln!(file, "Block{i}:\n{block}").unwrap();
    }
    writeln!(file, "Result:\n{result}").unwrap();
    write!(file, "\n\n").unwrap();
}

fn run_take_by_slice_limit(
    file: &mut impl Write,
    block: &DataBlock,
    slice: (usize, usize),
    limit: Option<usize>,
) {
    let result = DataBlock::take_by_slice_limit(block, slice, limit);
    writeln!(file, "Take Block by slice (limit: {limit:?}): {slice:?}").unwrap();
    writeln!(file, "Block:\n{block}").unwrap();
    writeln!(file, "Result:\n{result}").unwrap();
    write!(file, "\n\n").unwrap();
}

fn run_scatter(file: &mut impl Write, block: &DataBlock, indices: &[u32], scatter_size: usize) {
    let result = DataBlock::scatter(block, indices, scatter_size);

    match result {
        Ok(result_block) => {
            writeln!(file, "Scatter:         {indices:?}").unwrap();
            writeln!(file, "Source:\n{block}").unwrap();

            for (i, c) in result_block.iter().enumerate() {
                writeln!(file, "Result-{i}:\n{c}").unwrap();
            }
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "error: {}\n", err.message()).unwrap();
        }
    }
}
