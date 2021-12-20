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

use common_datablocks::DataBlock;
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::SeriesFrom;
use common_datavalues::series::Series;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;

#[test]
fn test_filter_non_const_data_block() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int8, false),
        DataField::new("b", DataType::String, false),
    ]);

    let block = DataBlock::create_by_array(schema, vec![
        Series::new(vec![1i8, 1, 2, 1, 2, 3]),
        Series::new(vec!["x1", "x1", "x2", "x1", "x2", "x3"]),
    ]);

    let predicate = Series::new(vec![true, false, true, true, false, false]).into();
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
        DataField::new("a", DataType::Int8, false),
        DataField::new("b", DataType::String, false),
    ]);

    let block = DataBlock::create_by_array(schema, vec![
        Series::new(vec![1i8, 1, 2, 1, 2, 3]),
        Series::new(vec!["x1", "x1", "x2", "x1", "x2", "x3"]),
    ]);

    let predicate = Series::new(vec![false, false, false, false, false, false]).into();
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
        DataField::new("a", DataType::Int8, false),
        DataField::new("b", DataType::String, false),
    ]);

    let block = DataBlock::create(schema, vec![
        DataColumn::Array(Series::new(vec![1i8, 1, 2, 1, 2, 3])),
        DataColumn::Constant(DataValue::String(Some(vec![b'x', b'1'])), 6),
    ]);

    let predicate = Series::new(vec![true, false, true, true, false, false]).into();
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
        DataField::new("a", DataType::Int8, false),
        DataField::new("b", DataType::String, false),
    ]);

    let block = DataBlock::create(schema, vec![
        DataColumn::Constant(DataValue::UInt8(Some(1)), 6),
        DataColumn::Constant(DataValue::String(Some(vec![b'x', b'1'])), 6),
    ]);

    let predicate = Series::new(vec![true, false, true, true, false, false]).into();
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
