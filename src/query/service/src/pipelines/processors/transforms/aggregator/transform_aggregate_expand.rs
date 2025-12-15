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

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_pipeline_transforms::AccumulatingTransform;

pub struct TransformExpandGroupingSets {
    group_bys: Vec<usize>,
    grouping_ids: Vec<usize>,
}

impl TransformExpandGroupingSets {
    pub fn new(group_bys: Vec<usize>, grouping_ids: Vec<usize>) -> Self {
        TransformExpandGroupingSets {
            grouping_ids,
            group_bys,
        }
    }
}

impl AccumulatingTransform for TransformExpandGroupingSets {
    const NAME: &'static str = "TransformExpandGroupingSets";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let num_rows = data.num_rows();
        let num_group_bys = self.group_bys.len();
        let mut output_blocks = Vec::with_capacity(self.grouping_ids.len());
        let dup_group_by_cols = self
            .group_bys
            .iter()
            .map(|i| data.get_by_offset(*i).clone())
            .collect::<Vec<_>>();

        let mut entries = data
            .columns()
            .iter()
            .cloned()
            .chain(dup_group_by_cols.clone())
            .collect::<Vec<_>>();

        // all group columns should be nullable
        for i in 0..num_group_bys {
            let entry = unsafe {
                let offset = self.group_bys.get_unchecked(i);
                entries.get_unchecked_mut(*offset)
            };
            match entry {
                BlockEntry::Const(_, data_type, _) => {
                    *data_type = data_type.wrap_nullable();
                }
                BlockEntry::Column(column) => *column = column.clone().wrap_nullable(None),
            };
        }

        for &id in &self.grouping_ids {
            // Repeat data for each grouping set.
            let grouping_id_column = BlockEntry::new_const_column(
                DataType::Number(NumberDataType::UInt32),
                Scalar::Number(NumberScalar::UInt32(id as u32)),
                num_rows,
            );
            // This is a copy of entries which clones the buffer of columns
            // So it's memory efficient
            let mut current_group_entries = entries.clone();
            current_group_entries.push(grouping_id_column);

            let bits = !id;
            for i in 0..num_group_bys {
                let entry = unsafe {
                    let offset = self.group_bys.get_unchecked(i);
                    current_group_entries.get_unchecked_mut(*offset)
                };
                // Reset the column to be nullable
                if bits & (1 << i) == 0 {
                    *entry = BlockEntry::new_const_column(entry.data_type(), Scalar::Null, num_rows)
                }
            }
            output_blocks.push(DataBlock::new(current_group_entries, num_rows));
        }

        Ok(output_blocks)
    }
}
