// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;

#[test]
fn test_filter_non_const_data_block() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i8::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);

    let block = DataBlock::create(schema, vec![
        Series::from_data(vec![1i8, 1, 2, 1, 2, 3]),
        Series::from_data(vec!["x1", "x1", "x2", "x1", "x2", "x3"]),
    ]);

    let predicate = Series::from_data(vec![true, false, true, true, false, false]);
    let block = DataBlock::filter_block(&block, &predicate)?;

    common_datablocks::assert_blocks_eq(
        vec![
            "+---+----+",
            "| a | b  |",
            "+---+----+",
            "| 1 | x1 |",
            "| 2 | x2 |",
            "| 1 | x1 |",
            "+---+----+",
        ],
        &[block],
    );

    Ok(())
}

#[test]
fn test_filter_all_false_data_block() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i8::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);

    let block = DataBlock::create(schema, vec![
        Series::from_data(vec![1i8, 1, 2, 1, 2, 3]),
        Series::from_data(vec!["x1", "x1", "x2", "x1", "x2", "x3"]),
    ]);

    let predicate = Series::from_data(vec![false, false, false, false, false, false]);
    let block = DataBlock::filter_block(&block, &predicate)?;

    common_datablocks::assert_blocks_eq(
        vec!["+---+---+", "| a | b |", "+---+---+", "+---+---+"],
        &[block],
    );

    Ok(())
}

#[test]
fn test_filter_const_data_block() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i8::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);

    let block = DataBlock::create(schema, vec![
        Series::from_data(vec![1i8, 1, 2, 1, 2, 3]),
        Arc::new(ConstColumn::new(
            Series::from_data(vec![vec![b'x', b'1']]),
            6,
        )),
    ]);

    let predicate = Series::from_data(vec![true, false, true, true, false, false]);
    let block = DataBlock::filter_block(&block, &predicate)?;

    common_datablocks::assert_blocks_eq(
        vec![
            "+---+----+",
            "| a | b  |",
            "+---+----+",
            "| 1 | x1 |",
            "| 2 | x1 |",
            "| 1 | x1 |",
            "+---+----+",
        ],
        &[block],
    );

    Ok(())
}

#[test]
fn test_filter_all_const_data_block() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i8::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);

    let block = DataBlock::create(schema, vec![
        Arc::new(ConstColumn::new(Series::from_data(vec![1i8]), 6)),
        Arc::new(ConstColumn::new(
            Series::from_data(vec![vec![b'x', b'1']]),
            6,
        )),
    ]);

    let predicate = Series::from_data(vec![true, false, true, true, false, false]);
    let block = DataBlock::filter_block(&block, &predicate)?;

    common_datablocks::assert_blocks_eq(
        vec![
            "+---+----+",
            "| a | b  |",
            "+---+----+",
            "| 1 | x1 |",
            "| 1 | x1 |",
            "| 1 | x1 |",
            "+---+----+",
        ],
        &[block],
    );

    Ok(())
}
