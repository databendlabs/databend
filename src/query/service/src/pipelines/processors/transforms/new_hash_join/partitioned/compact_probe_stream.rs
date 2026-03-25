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

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodKind;
use databend_common_expression::KeyAccessor;
use databend_common_expression::KeysState;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::with_hash_method;

use super::compact_hash_table::CompactJoinHashTable;
use super::partitioned_build::CHUNK_BITS;
use super::partitioned_build::CHUNK_SIZE;
use super::partitioned_build::flat_to_row_ptr;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::common::probe_stream::ProbedRows;

const CHUNK_MASK: usize = CHUNK_SIZE - 1;

struct CompactProbeStream<'a, Key: ?Sized + Eq, const MATCHED: bool> {
    key_idx: usize,
    build_idx: u32,
    matched_num_rows: usize,
    probe_validity: Option<Bitmap>,

    probe_hashes: Vec<u64>,
    bucket_mask: usize,
    probe_acc: Box<dyn KeyAccessor<Key = Key>>,
    build_accs: Vec<Box<dyn KeyAccessor<Key = Key>>>,

    hash_table: &'a CompactJoinHashTable<u32>,
}

impl<'a, Key: ?Sized + Eq + Send + Sync + 'static, const MATCHED: bool> ProbeStream
    for CompactProbeStream<'a, Key, MATCHED>
{
    fn advance(&mut self, res: &mut ProbedRows, max_rows: usize) -> Result<()> {
        while self.key_idx < self.probe_acc.len() {
            if res.matched_probe.len() >= max_rows {
                break;
            }

            if self
                .probe_validity
                .as_ref()
                .is_some_and(|validity| !validity.get_bit(self.key_idx))
            {
                if !MATCHED {
                    res.unmatched.push(self.key_idx as u64);
                }
                self.key_idx += 1;
                continue;
            }

            if self.build_idx == 0 {
                let bucket = (self.probe_hashes[self.key_idx] as usize) & self.bucket_mask;
                self.build_idx = self.hash_table.first_index(bucket);

                if self.build_idx == 0 {
                    if !MATCHED {
                        res.unmatched.push(self.key_idx as u64);
                    }
                    self.key_idx += 1;
                    continue;
                }
            }

            let probe_key = unsafe { self.probe_acc.key_unchecked(self.key_idx) };

            while self.build_idx != 0 {
                let bi = self.build_idx as usize;
                let row_idx = (bi - 1) & CHUNK_MASK;
                let chunk_idx = (bi - 1) >> CHUNK_BITS;
                let build_key = unsafe { self.build_accs[chunk_idx].key_unchecked(row_idx) };

                if build_key == probe_key {
                    res.matched_probe.push(self.key_idx as u64);
                    res.matched_build.push(flat_to_row_ptr(bi));
                    self.matched_num_rows += 1;

                    if res.matched_probe.len() >= max_rows {
                        self.build_idx = self.hash_table.next_index(self.build_idx);
                        if self.build_idx == 0 {
                            self.key_idx += 1;
                            self.matched_num_rows = 0;
                        }
                        return Ok(());
                    }
                }
                self.build_idx = self.hash_table.next_index(self.build_idx);
            }

            if !MATCHED && self.matched_num_rows == 0 {
                res.unmatched.push(self.key_idx as u64);
            }
            self.key_idx += 1;
            self.matched_num_rows = 0;
        }
        Ok(())
    }
}

fn create_probe_stream_inner<'a, M: HashMethod, const MATCHED: bool>(
    method: &M,
    hash_table: &'a CompactJoinHashTable<u32>,
    build_keys_states: &'a [KeysState],
    desc: &HashJoinDesc,
    function_ctx: &FunctionContext,
    data: &DataBlock,
) -> Result<Box<dyn ProbeStream + Send + Sync + 'a>>
where
    M::HashKey: Send + Sync,
{
    let probe_keys_entries = desc.probe_key(data, function_ctx)?;
    let mut probe_keys_block = DataBlock::new(probe_keys_entries, data.num_rows());
    let probe_validity = match desc.from_correlated_subquery {
        true => None,
        false => desc.build_valids_by_keys(&probe_keys_block)?,
    };

    desc.remove_keys_nullable(&mut probe_keys_block);

    let keys = ProjectedBlock::from(probe_keys_block.columns());
    let probe_ks = method.build_keys_state(keys, data.num_rows())?;
    let mut probe_hashes = Vec::with_capacity(data.num_rows());
    method.build_keys_hashes(&probe_ks, &mut probe_hashes);

    let probe_acc = method.build_keys_accessor(probe_ks)?;
    let build_accs = build_keys_states
        .iter()
        .map(|ks| method.build_keys_accessor(ks.clone()))
        .collect::<Result<Vec<_>>>()?;

    let bucket_mask = hash_table.bucket_mask();

    Ok(Box::new(CompactProbeStream::<'a, M::HashKey, MATCHED> {
        key_idx: 0,
        build_idx: 0,
        matched_num_rows: 0,
        probe_validity,
        probe_hashes,
        bucket_mask,
        probe_acc,
        build_accs,
        hash_table,
    }))
}

/// Create a CompactProbeStream that only tracks matched rows (MATCHED=true).
/// For inner join, left semi, right series.
pub fn create_compact_probe_matched<'a>(
    hash_table: &'a CompactJoinHashTable<u32>,
    build_keys_states: &'a [KeysState],
    method: &HashMethodKind,
    desc: &HashJoinDesc,
    function_ctx: &FunctionContext,
    data: &DataBlock,
) -> Result<Box<dyn ProbeStream + Send + Sync + 'a>> {
    with_hash_method!(|T| match method {
        HashMethodKind::T(method) => {
            create_probe_stream_inner::<_, true>(
                method,
                hash_table,
                build_keys_states,
                desc,
                function_ctx,
                data,
            )
        }
    })
}

/// Create a CompactProbeStream that also tracks unmatched rows (MATCHED=false).
/// For left join, left anti.
pub fn create_compact_probe<'a>(
    hash_table: &'a CompactJoinHashTable<u32>,
    build_keys_states: &'a [KeysState],
    method: &HashMethodKind,
    desc: &HashJoinDesc,
    function_ctx: &FunctionContext,
    data: &DataBlock,
) -> Result<Box<dyn ProbeStream + Send + Sync + 'a>> {
    with_hash_method!(|T| match method {
        HashMethodKind::T(method) => {
            create_probe_stream_inner::<_, false>(
                method,
                hash_table,
                build_keys_states,
                desc,
                function_ctx,
                data,
            )
        }
    })
}
