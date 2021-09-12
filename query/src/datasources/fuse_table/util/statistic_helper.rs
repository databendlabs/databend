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

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use common_datavalues::DataType;
use common_exception::Result;

use crate::datasources::fuse_table::meta::table_snapshot::ColStats;
use crate::datasources::fuse_table::meta::table_snapshot::ColumnId;

pub fn column_stats_reduce(
    stats: Vec<HashMap<ColumnId, (DataType, ColStats)>>,
) -> Result<HashMap<ColumnId, ColStats>> {
    let len = stats.len();
    let col_stat_list = stats.into_iter().fold(HashMap::new(), |acc, item| {
        item.into_iter().fold(
            acc,
            |mut acc: HashMap<ColumnId, (DataType, Vec<ColStats>)>,
             (col_id, (data_type, stats))| {
                let entry = acc.entry(col_id);
                match entry {
                    Entry::Occupied(_) => {
                        entry.and_modify(|v| v.1.push(stats));
                    }
                    Entry::Vacant(_) => {
                        entry.or_insert((data_type, vec![stats]));
                    }
                }
                acc
            },
        )
    });

    col_stat_list.iter().try_fold(
        HashMap::with_capacity(len),
        |mut acc, (id, (data_type, stats))| {
            let mut min_stats = Vec::with_capacity(stats.len());
            let mut max_stats = Vec::with_capacity(stats.len());
            let mut null_count = 0;

            for col_stats in stats {
                min_stats.push(col_stats.min.clone());
                max_stats.push(col_stats.max.clone());
                null_count += col_stats.null_count;
            }

            let min =
                common_datavalues::DataValue::try_into_data_array(min_stats.as_slice(), data_type)?
                    .min()?;

            let max =
                common_datavalues::DataValue::try_into_data_array(min_stats.as_slice(), data_type)?
                    .max()?;

            acc.insert(*id, ColStats {
                min,
                max,
                null_count,
            });
            Ok(acc)
        },
    )
}
