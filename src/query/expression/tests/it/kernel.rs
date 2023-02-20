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
