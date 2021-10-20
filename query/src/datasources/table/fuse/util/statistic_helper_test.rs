//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_datablocks::DataBlock;
use common_datavalues::prelude::SeriesFrom;
use common_datavalues::series::Series;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;

use super::statistic_helper;

struct TestFixture {}

impl TestFixture {
    fn gen_block_stream(num: u32) -> Vec<DataBlock> {
        (0..num)
            .into_iter()
            .map(|_v| {
                let schema =
                    DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int32, false)]);
                DataBlock::create_by_array(schema, vec![Series::new(vec![1, 2, 3])])
            })
            .collect()
    }
}

#[test]
fn test_ft_stats_block_stats() -> common_exception::Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int32, false)]);
    let block = DataBlock::create_by_array(schema, vec![Series::new(vec![1, 2, 3])]);
    let r = statistic_helper::block_stats(&block)?;
    assert_eq!(1, r.len());
    let col_stats = r.get(&0).unwrap();
    assert_eq!(col_stats.min, DataValue::Int32(Some(1)));
    assert_eq!(col_stats.max, DataValue::Int32(Some(3)));
    Ok(())
}

#[test]
fn test_ft_stats_col_stats_reduce() -> common_exception::Result<()> {
    let blocks = TestFixture::gen_block_stream(10);
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int32, false)]);
    let col_stats = blocks
        .iter()
        .map(statistic_helper::block_stats)
        .collect::<common_exception::Result<Vec<_>>>()?;
    let r = statistic_helper::column_stats_reduce_with_schema(&col_stats, &schema);
    assert!(r.is_ok());
    let r = r.unwrap();
    assert_eq!(1, r.len());
    let col_stats = r.get(&0).unwrap();
    assert_eq!(col_stats.min, DataValue::Int32(Some(1)));
    assert_eq!(col_stats.max, DataValue::Int32(Some(3)));
    Ok(())
}

#[test]
fn test_ft_stats_accumulator() -> common_exception::Result<()> {
    let blocks = TestFixture::gen_block_stream(10);
    let mut stats_acc = statistic_helper::StatisticsAccumulator::new();
    let mut meta_acc = statistic_helper::BlockMetaAccumulator::new();
    blocks.iter().try_for_each(|item| {
        stats_acc.acc(item)?;
        meta_acc.acc(1, "".to_owned(), &mut stats_acc);
        Ok::<_, ErrorCode>(())
    })?;
    assert_eq!(10, stats_acc.blocks_stats.len());
    // TODO more cases here pls
    Ok(())
}
