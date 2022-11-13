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

use std::iter::repeat;
use std::iter::TrustedLen;
use std::sync::atomic::Ordering;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_datablocks::DataBlock;
use common_datavalues::wrap_nullable;
use common_datavalues::ColumnRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;

use crate::pipelines::processors::transforms::hash_join::desc::MarkerKind;
use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_BLOCK_SIZE;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;
use crate::sql::plans::JoinType;

impl JoinHashTable {
    pub(crate) fn probe_left_join<
        'a,
        const WITH_OTHER_CONJUNCT: bool,
        H: HashtableLike<Value = Vec<RowPtr>>,
        IT,
    >(
        &self,
        hash_table: &H,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        IT: Iterator<Item = &'a H::Key> + TrustedLen,
        H::Key: 'a,
    {
        let valids = &probe_state.valids;

        // The left join will return multiple data blocks of similar size
        let mut probed_blocks = vec![];
        let mut probe_indexes = Vec::with_capacity(JOIN_MAX_BLOCK_SIZE);
        let mut probe_indexes_vec = Vec::new();
        // Collect each probe_indexes, used by non-equi conditions filter
        let mut local_build_indexes = Vec::with_capacity(JOIN_MAX_BLOCK_SIZE);
        let mut validity = MutableBitmap::with_capacity(JOIN_MAX_BLOCK_SIZE);

        let mut row_state = match WITH_OTHER_CONJUNCT {
            true => vec![0; keys_iter.size_hint().0],
            false => vec![],
        };

        let dummy_probed_rows = vec![RowPtr {
            row_index: 0,
            chunk_index: 0,
            marker: Some(MarkerKind::False),
        }];

        // Start to probe hash table
        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = match self.hash_join_desc.from_correlated_subquery {
                true => hash_table.entry(key),
                false => self.probe_key(hash_table, key, valids, i),
            };

            let (validity_value, probed_rows) = match probe_result_ptr {
                None => (false, &dummy_probed_rows),
                Some(v) => (true, v.get()),
            };

            if self.hash_join_desc.join_type == JoinType::Full {
                let mut build_indexes = self.hash_join_desc.join_state.build_indexes.write();
                // dummy row ptr
                // here assume there is no RowPtr, which chunk_index is usize::MAX and row_index is usize::MAX
                match probe_result_ptr {
                    None => build_indexes.push(RowPtr {
                        chunk_index: usize::MAX,
                        row_index: usize::MAX,
                        marker: Some(MarkerKind::False),
                    }),
                    Some(_) => build_indexes.extend(probed_rows),
                };
            }

            if self.hash_join_desc.join_type == JoinType::Single && probed_rows.len() > 1 {
                return Err(ErrorCode::Internal(
                    "Scalar subquery can't return more than one row",
                ));
            }

            if WITH_OTHER_CONJUNCT {
                row_state[i] += probed_rows.len() as u32;
            }

            if probe_indexes.len() + probed_rows.len() < probe_indexes.capacity() {
                local_build_indexes.extend_from_slice(probed_rows);
                validity.extend_constant(probed_rows.len(), validity_value);
                probe_indexes.extend(repeat(i as u32).take(probed_rows.len()));
            } else {
                let mut index = 0_usize;
                let mut remain = probed_rows.len();

                while index < probed_rows.len() {
                    if probe_indexes.len() + remain < probe_indexes.capacity() {
                        validity.extend_constant(remain, validity_value);
                        probe_indexes.extend(repeat(i as u32).take(remain));
                        local_build_indexes.extend_from_slice(&probed_rows[index..]);
                        index += remain;
                    } else {
                        if self.interrupt.load(Ordering::Relaxed) {
                            return Err(ErrorCode::AbortedQuery(
                                "Aborted query, because the server is shutting down or the query was killed.",
                            ));
                        }

                        let addition = probe_indexes.capacity() - probe_indexes.len();
                        let new_index = index + addition;

                        validity.extend_constant(addition, validity_value);
                        probe_indexes.extend(repeat(i as u32).take(addition));
                        local_build_indexes.extend_from_slice(&probed_rows[index..new_index]);

                        let validity_bitmap: Bitmap = validity.into();
                        let build_block = if !self.hash_join_desc.from_correlated_subquery
                            && self.hash_join_desc.join_type == JoinType::Single
                            && validity_bitmap.unset_bits() == input.num_rows()
                        {
                            // Uncorrelated scalar subquery and no row was returned by subquery
                            // We just construct a block with NULLs
                            let build_data_schema = self.row_space.data_schema.clone();
                            let columns = build_data_schema
                                .fields()
                                .iter()
                                .map(|field| {
                                    let data_type = wrap_nullable(field.data_type());
                                    data_type
                                        .create_column(&vec![DataValue::Null; input.num_rows()])
                                })
                                .collect::<Result<Vec<ColumnRef>>>()?;
                            DataBlock::create(build_data_schema, columns)
                        } else {
                            self.row_space.gather(&local_build_indexes)?
                        };

                        // For left join, wrap nullable for build block
                        let nullable_columns = if self.row_space.datablocks().is_empty()
                            && !local_build_indexes.is_empty()
                        {
                            build_block
                                .columns()
                                .iter()
                                .map(|c| {
                                    c.data_type().create_constant_column(
                                        &DataValue::Null,
                                        local_build_indexes.len(),
                                    )
                                })
                                .collect::<Result<Vec<_>>>()?
                        } else {
                            build_block
                                .columns()
                                .iter()
                                .map(|c| Self::set_validity(c, &validity_bitmap))
                                .collect::<Result<Vec<_>>>()?
                        };

                        let nullable_build_block = DataBlock::create(
                            self.row_space.data_schema.clone(),
                            nullable_columns.clone(),
                        );

                        // For full join, wrap nullable for probe block
                        let mut probe_block =
                            DataBlock::block_take_by_indices(input, &probe_indexes)?;
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
                            probe_block = DataBlock::create(
                                self.probe_schema.clone(),
                                nullable_probe_columns,
                            );
                        }

                        let merged_block =
                            self.merge_eq_block(&nullable_build_block, &probe_block)?;

                        if !merged_block.is_empty() {
                            probe_indexes_vec.push(probe_indexes.clone());
                            probed_blocks.push(merged_block);
                        }

                        index = new_index;
                        remain -= addition;
                        probe_indexes.clear();
                        local_build_indexes.clear();
                        validity = MutableBitmap::with_capacity(JOIN_MAX_BLOCK_SIZE);
                    }
                }
            }
        }

        // For full join, wrap nullable for probe block
        let mut probe_block = DataBlock::block_take_by_indices(input, &probe_indexes)?;
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

        if !WITH_OTHER_CONJUNCT {
            let mut rest_build_indexes = self.hash_join_desc.join_state.rest_build_indexes.write();
            rest_build_indexes.extend(local_build_indexes);
            let mut rest_probe_blocks = self.hash_join_desc.join_state.rest_probe_blocks.write();
            rest_probe_blocks.push(probe_block);
            let validity: Bitmap = validity.into();
            let mut validity_state = self.hash_join_desc.join_state.validity.write();
            validity_state.extend_from_bitmap(&validity);
            return Ok(probed_blocks);
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
            self.row_space.gather(&local_build_indexes)?
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

        // Process no-equi conditions
        let merged_block = self.merge_eq_block(&nullable_build_block, &probe_block)?;

        if !merged_block.is_empty() || probed_blocks.is_empty() {
            probe_indexes_vec.push(probe_indexes.clone());
            probed_blocks.push(merged_block);
        }

        self.non_equi_conditions_for_left_join(&probed_blocks, &probe_indexes_vec, &mut row_state)
    }

    // keep at least one index of the positive state and the null state
    // bitmap: [1, 0, 1] with row_state [2, 1], probe_index: [0, 0, 1]
    // bitmap will be [1, 0, 1] -> [1, 0, 1] -> [1, 0, 1] -> [1, 0, 1]
    // row_state will be [2, 1] -> [2, 1] -> [1, 1] -> [1, 1]
    pub(crate) fn fill_null_for_left_join(
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
