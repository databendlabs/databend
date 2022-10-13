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

use std::iter::TrustedLen;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_datablocks::DataBlock;
use common_datavalues::wrap_nullable;
use common_datavalues::BooleanColumn;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hashtable::HashMap;
use common_hashtable::HashTableKeyable;

use crate::pipelines::processors::transforms::hash_join::desc::MarkerKind;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;
use crate::sql::plans::JoinType;

impl JoinHashTable {
    pub(crate) fn probe_left_join<const WITH_OTHER_CONJUNCT: bool, Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        let probe_indexs = &mut probe_state.probe_indexs;
        let local_build_indexes = &mut probe_state.build_indexs;
        let valids = &probe_state.valids;

        let row_state = &mut probe_state.row_state;

        if WITH_OTHER_CONJUNCT {
            row_state.resize(keys_iter.size_hint().0, 0);
        }

        let mut validity = MutableBitmap::new();
        // Start to probe hash table
        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                hash_table.find_key(&key)
            } else {
                self.probe_key(hash_table, key, valids, i)
            };

            match probe_result_ptr {
                Some(v) => {
                    let probe_result_ptrs = v.get_value();

                    if self.hash_join_desc.join_type == JoinType::Full {
                        let mut build_indexes =
                            self.hash_join_desc.right_join_desc.build_indexes.write();
                        build_indexes.extend(probe_result_ptrs);
                    }

                    if self.hash_join_desc.join_type == JoinType::Single
                        && probe_result_ptrs.len() > 1
                    {
                        return Err(ErrorCode::LogicalError(
                            "Scalar subquery can't return more than one row",
                        ));
                    }
                    local_build_indexes.extend_from_slice(probe_result_ptrs);
                    probe_indexs.extend(std::iter::repeat(i as u32).take(probe_result_ptrs.len()));

                    if WITH_OTHER_CONJUNCT {
                        row_state[i] += probe_result_ptrs.len() as u32;
                    }
                    validity.extend_constant(probe_result_ptrs.len(), true);
                }
                None => {
                    if self.hash_join_desc.join_type == JoinType::Full {
                        let mut build_indexes =
                            self.hash_join_desc.right_join_desc.build_indexes.write();
                        // dummy row ptr
                        // here assume there is no RowPtr, which chunk_index is u32::MAX and row_index is u32::MAX
                        build_indexes.push(RowPtr {
                            chunk_index: u32::MAX,
                            row_index: u32::MAX,
                            marker: None,
                        });
                    }
                    // dummy row ptr
                    local_build_indexes.push(RowPtr {
                        chunk_index: 0,
                        row_index: 0,
                        marker: None,
                    });
                    probe_indexs.push(i as u32);
                    validity.push(false);

                    if WITH_OTHER_CONJUNCT {
                        row_state[i] += 1;
                    }
                }
            }
        }

        let validity: Bitmap = validity.into();
        let build_block = if !self.hash_join_desc.from_correlated_subquery
            && self.hash_join_desc.join_type == JoinType::Single
            && validity.unset_bits() == input.num_rows()
        {
            // Uncorrelated scalar subquery and no row was returned by subquery
            // We just construct a block with NULLs
            let build_data_schema = self.row_space.data_schema.clone();
            let columns = build_data_schema
                .fields()
                .iter()
                .map(|field| {
                    let data_type = wrap_nullable(field.data_type());
                    data_type.create_column(&vec![DataValue::Null; input.num_rows()])
                })
                .collect::<Result<Vec<ColumnRef>>>()?;
            DataBlock::create(build_data_schema, columns)
        } else {
            self.row_space.gather(local_build_indexes)?
        };

        // For left join, wrap nullable for build block
        let nullable_columns =
            if self.row_space.datablocks().is_empty() && !local_build_indexes.is_empty() {
                build_block
                    .columns()
                    .iter()
                    .map(|c| {
                        c.data_type()
                            .create_constant_column(&DataValue::Null, local_build_indexes.len())
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                build_block
                    .columns()
                    .iter()
                    .map(|c| Self::set_validity(c, &validity))
                    .collect::<Result<Vec<_>>>()?
            };
        let nullable_build_block =
            DataBlock::create(self.row_space.data_schema.clone(), nullable_columns.clone());

        // For full join, wrap nullable for probe block
        let mut probe_block = DataBlock::block_take_by_indices(input, probe_indexs)?;
        if self.hash_join_desc.join_type == JoinType::Full {
            let nullable_probe_columns = probe_block
                .columns()
                .iter()
                .map(|c| {
                    let mut probe_validity = MutableBitmap::new();
                    probe_validity.extend_constant(c.len(), true);
                    let probe_validity: Bitmap = probe_validity.into();
                    Self::set_validity(c, &probe_validity)
                })
                .collect::<Result<Vec<_>>>()?;
            probe_block = DataBlock::create(self.probe_schema.clone(), nullable_probe_columns);
        }

        let merged_block = self.merge_eq_block(&nullable_build_block, &probe_block)?;

        if !WITH_OTHER_CONJUNCT {
            return Ok(vec![merged_block]);
        }

        // Process non-equi conditions
        let (bm, all_true, all_false) = self.get_other_filters(
            &merged_block,
            self.hash_join_desc.other_predicate.as_ref().unwrap(),
        )?;

        if all_true {
            return Ok(vec![merged_block]);
        }

        let validity = match (bm, all_false) {
            (Some(b), _) => b,
            (None, true) => Bitmap::new_zeroed(merged_block.num_rows()),
            // must be one of above
            _ => unreachable!(),
        };

        let nullable_columns = nullable_columns
            .iter()
            .map(|c| Self::set_validity(c, &validity))
            .collect::<Result<Vec<_>>>()?;
        let nullable_build_block =
            DataBlock::create(self.row_space.data_schema.clone(), nullable_columns.clone());
        let merged_block = self.merge_eq_block(&nullable_build_block, &probe_block)?;

        let mut bm = validity.into_mut().right().unwrap();

        if self.hash_join_desc.join_type == JoinType::Full {
            let mut build_indexes = self.hash_join_desc.right_join_desc.build_indexes.write();
            for (idx, build_index) in build_indexes.iter_mut().enumerate() {
                if !bm.get(idx) {
                    build_index.marker = Some(MarkerKind::False);
                }
            }
        }
        self.fill_null_for_left_join(&mut bm, probe_indexs, row_state);

        let predicate = BooleanColumn::from_arrow_data(bm.into()).arc();
        Ok(vec![DataBlock::filter_block(merged_block, &predicate)?])
    }

    // keep at least one index of the positive state and the null state
    // bitmap: [1, 0, 1] with row_state [2, 1], probe_index: [0, 0, 1]
    // bitmap will be [1, 0, 1] -> [1, 0, 1] -> [1, 0, 1] -> [1, 0, 1]
    // row_state will be [2, 1] -> [2, 1] -> [1, 1] -> [1, 1]
    fn fill_null_for_left_join(
        &self,
        bm: &mut MutableBitmap,
        probe_indexs: &[u32],
        row_state: &mut [u32],
    ) {
        for (index, row) in probe_indexs.iter().enumerate() {
            let row = *row as usize;
            if row_state[row] == 0 {
                bm.set(index, true);
                continue;
            }

            if row_state[row] == 1 {
                if !bm.get(index) {
                    bm.set(index, true)
                }
                continue;
            }

            if !bm.get(index) {
                row_state[row] -= 1;
            }
        }
    }
}
