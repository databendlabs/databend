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

use databend_common_base::obfuscator::generate;
use databend_common_base::obfuscator::CodePoint;
use databend_common_base::obfuscator::Histogram;
use databend_common_base::obfuscator::NGramHash;
use databend_common_base::obfuscator::Table;
use databend_common_column::buffer::Buffer;
use databend_common_expression::types::map::KvPair;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::MapType;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::Column;

struct ColumnHistogram {
    total: u32,
    count_end: u32,
    codes: Buffer<u32>,
    weights: Buffer<u32>,
}

impl Histogram<'_> for ColumnHistogram {
    fn sample(&self, random: u64, end_multiplier: f64) -> Option<CodePoint> {
        let range = self.total + (self.count_end as f64 * end_multiplier) as u32;
        if range == 0 {
            return None;
        }

        let mut random = random as u32 % range;
        self.codes
            .iter()
            .zip(self.weights.iter())
            .find(|(_, weighted)| {
                if random <= **weighted {
                    true
                } else {
                    random -= **weighted;
                    false
                }
            })
            .map(|(code, _)| *code)
    }
}

struct ColumnTable {
    hash: Buffer<NGramHash>,
    total: Buffer<u32>,
    count_end: Buffer<u32>,
    buckets: ArrayColumn<KvPair<UInt32Type, UInt32Type>>,
}

impl Table<'_, ColumnHistogram> for ColumnTable {
    fn get(&self, context_hash: &NGramHash) -> Option<ColumnHistogram> {
        let row = self.hash.binary_search(context_hash).ok()?;
        let bucket = self.buckets.index(row).unwrap();
        Some(ColumnHistogram {
            total: self.total[row],
            count_end: self.count_end[row],
            codes: bucket.keys,
            weights: bucket.values,
        })
    }
}

impl ColumnTable {
    fn try_new(column: Column) -> Option<Self> {
        let Column::Array(box array) = column else {
            return None;
        };
        let Column::Tuple(mut tuple) = array.values else {
            return None;
        };
        if tuple.len() != 4 {
            return None;
        }
        let buckets = tuple.pop()?;
        let buckets = MapType::<UInt32Type, UInt32Type>::try_downcast_column(&buckets)?;
        let Some(Column::Number(NumberColumn::UInt32(count_end))) = tuple.pop() else {
            return None;
        };
        let Some(Column::Number(NumberColumn::UInt32(total))) = tuple.pop() else {
            return None;
        };
        let Some(Column::Number(NumberColumn::UInt32(hash))) = tuple.pop() else {
            return None;
        };

        Some(ColumnTable {
            hash,
            total,
            count_end,
            buckets,
        })
    }
}

#[test]
fn xxx() {
    // let table = ColumnTable {
    //     hash: todo!(),
    //     total: todo!(),
    //     count_end: todo!(),
    //     buckets: todo!(),
    // };
    // generate(
    //     &table,
    //     order,
    //     seed,
    //     writer,
    //     desired_size,
    //     determinator_sliding_window_size,
    //     determinator_data,
    //     code_points,
    // );
    println!("x")
}
