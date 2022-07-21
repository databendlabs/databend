//  Copyright 2022 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::ColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataValue;
use common_exception::Result;
use common_functions::aggregates::eval_aggr;
use common_fuse_meta::meta::ColumnStatistics;
use common_fuse_meta::meta::StatisticsOfColumns;

use crate::storages::index::MinMaxIndex;
use crate::storages::index::SupportedType;

pub fn gen_columns_statistics(data_block: &DataBlock) -> Result<StatisticsOfColumns> {
    let mut statistics = StatisticsOfColumns::new();

    let leaves = traverse::traverse_columns_dfs(data_block.columns())?;

    for (idx, col) in leaves.iter().enumerate() {
        let col_data_type = col.data_type();
        if !MinMaxIndex::is_supported_type(&col_data_type) {
            continue;
        }

        // later, during the evaluation of expressions, name of field does not matter
        let data_field = DataField::new("", col_data_type);
        let column_field = ColumnWithField::new(col.clone(), data_field);
        let mut min = DataValue::Null;
        let mut max = DataValue::Null;

        let rows = col.len();

        let mins = eval_aggr("min", vec![], &[column_field.clone()], rows)?;
        let maxs = eval_aggr("max", vec![], &[column_field], rows)?;

        if mins.len() > 0 {
            min = mins.get(0);
        }
        if maxs.len() > 0 {
            max = maxs.get(0);
        }
        let (is_all_null, bitmap) = col.validity();
        let unset_bits = match (is_all_null, bitmap) {
            (true, _) => rows,
            (false, Some(bitmap)) => bitmap.unset_bits(),
            (false, None) => 0,
        };

        let in_memory_size = col.memory_size() as u64;
        let col_stats = ColumnStatistics {
            min,
            max,
            null_count: unset_bits as u64,
            in_memory_size,
        };

        statistics.insert(idx as u32, col_stats);
    }
    Ok(statistics)
}

pub mod traverse {
    use common_datavalues::ColumnRef;
    use common_datavalues::DataTypeImpl;
    use common_datavalues::Series;
    use common_datavalues::StructColumn;

    use super::*;

    // traverses columns and collects the leaves in depth first manner
    pub fn traverse_columns_dfs(columns: &[ColumnRef]) -> Result<Vec<ColumnRef>> {
        let mut leaves = vec![];
        for f in columns {
            traverse_recursive(f, &mut leaves)?;
        }
        Ok(leaves)
    }

    fn traverse_recursive(column: &ColumnRef, leaves: &mut Vec<ColumnRef>) -> Result<()> {
        match column.data_type() {
            DataTypeImpl::Struct(_) => {
                let struct_col: &StructColumn = Series::check_get(column)?;
                for f in struct_col.values() {
                    traverse_recursive(f, leaves)?
                }
            }
            _ => {
                leaves.push(column.clone());
            }
        }
        Ok(())
    }
}
