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
use std::sync::atomic::Ordering;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::Chunk;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;
use common_sql::plans::JoinType;

use crate::pipelines::processors::transforms::hash_join::desc::JOIN_MAX_CHUNK_SIZE;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::JoinHashTable;

impl JoinHashTable {
    /// Used by right join/right semi(anti) join
    pub(crate) fn probe_right_join<'a, H: HashtableLike<Value = Vec<RowPtr>>, IT>(
        &self,
        hash_table: &H,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &Chunk,
    ) -> Result<Vec<Chunk>>
    where
        IT: Iterator<Item = &'a H::Key> + TrustedLen,
        H::Key: 'a,
    {
        let valids = &probe_state.valids;
        // The right join will return multiple data chunks of similar size
        let mut probed_chunks = vec![];
        let mut local_probe_indexes = Vec::with_capacity(JOIN_MAX_CHUNK_SIZE);
        let mut local_build_indexes = Vec::with_capacity(JOIN_MAX_CHUNK_SIZE);
        let mut validity = MutableBitmap::with_capacity(JOIN_MAX_CHUNK_SIZE);
        let mut build_indexes = self.hash_join_desc.join_state.build_indexes.write();
        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = self.probe_key(hash_table, key, valids, i);

            if let Some(v) = probe_result_ptr {
                let probed_rows = v.get();

                if local_probe_indexes.len() + probed_rows.len() < local_probe_indexes.capacity() {
                    build_indexes.extend(probed_rows);
                    local_build_indexes.extend_from_slice(probed_rows);
                    local_probe_indexes.extend(std::iter::repeat(i as u32).take(probed_rows.len()));
                    validity.extend_constant(probed_rows.len(), true);
                } else {
                    let mut index = 0_usize;
                    let mut remain = probed_rows.len();

                    while index < probed_rows.len() {
                        if local_probe_indexes.len() + remain < local_probe_indexes.capacity() {
                            build_indexes.extend_from_slice(&probed_rows[index..]);
                            local_build_indexes.extend_from_slice(&probed_rows[index..]);
                            local_probe_indexes.extend(std::iter::repeat(i as u32).take(remain));
                            validity.extend_constant(remain, true);

                            index += remain;
                        } else {
                            if self.interrupt.load(Ordering::Relaxed) {
                                return Err(ErrorCode::AbortedQuery(
                                    "Aborted query, because the server is shutting down or the query was killed.",
                                ));
                            }

                            let addition =
                                local_probe_indexes.capacity() - local_probe_indexes.len();
                            let new_index = index + addition;

                            build_indexes.extend_from_slice(&probed_rows[index..new_index]);
                            local_probe_indexes.extend(std::iter::repeat(i as u32).take(addition));
                            local_build_indexes.extend_from_slice(&probed_rows[index..new_index]);
                            validity.extend_constant(addition, true);

                            let build_chunk = self.row_space.gather(&local_build_indexes)?;
                            let mut probe_chunk = Chunk::take(input, &local_probe_indexes)?;

                            // If join type is right join, need to wrap nullable for probe side
                            // If join type is semi/anti right join, directly merge `build_chunk` and `probe_chunk`
                            if self.hash_join_desc.join_type == JoinType::Right {
                                let validity: Bitmap = validity.into();
                                let nullable_columns = probe_chunk
                                    .columns()
                                    .iter()
                                    .map(|c| Self::set_validity(c, &validity))
                                    .collect::<Vec<_>>();
                                probe_chunk = Chunk::new(nullable_columns, validity.len());
                            }

                            if !probe_chunk.is_empty() {
                                probed_chunks
                                    .push(self.merge_eq_chunk(&build_chunk, &probe_chunk)?);
                            }

                            index = new_index;
                            remain -= addition;

                            local_probe_indexes.clear();
                            local_build_indexes.clear();
                            validity = MutableBitmap::new();
                        }
                    }
                }
            }
        }

        let mut probe_chunk = Chunk::take(input, &local_probe_indexes)?;

        // If join type is right join, need to wrap nullable for probe side
        // If join type is semi/anti right join, directly merge `build_chunk` and `probe_chunk`
        if self.hash_join_desc.join_type == JoinType::Right {
            let validity: Bitmap = validity.into();
            let nullable_columns = probe_chunk
                .columns()
                .iter()
                .map(|c| Self::set_validity(c, &validity))
                .collect::<Vec<_>>();
            probe_chunk = Chunk::new(nullable_columns, validity.len());
        }

        let mut rest_build_indexes = self.hash_join_desc.join_state.rest_build_indexes.write();
        let mut rest_probe_chunks = self.hash_join_desc.join_state.rest_probe_chunks.write();
        rest_probe_chunks.push(probe_chunk);
        rest_build_indexes.extend(local_build_indexes);

        Ok(probed_chunks)
    }
}
