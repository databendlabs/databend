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

use std::sync::Arc;
use std::sync::PoisonError;

use databend_common_base::base::ProgressValues;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_hashtable::RowPtr;

use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::OneBlockJoinStream;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::JoinStream;
use crate::pipelines::processors::transforms::NestedLoopDesc;
use crate::pipelines::processors::transforms::RuntimeFiltersDesc;

pub struct NestedLoopJoin<T> {
    inner: T,
    state: Arc<BasicHashJoinState>,
    desc: NestedLoopDesc,
}

impl<T> NestedLoopJoin<T> {
    pub fn create(inner: T, state: Arc<BasicHashJoinState>, desc: NestedLoopDesc) -> Self {
        Self { inner, state, desc }
    }

    fn finalize_chunks(&mut self) {
        if !self.state.columns.is_empty() {
            return;
        }

        let locked = self.state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        if !self.state.columns.is_empty() {
            return;
        }
        let num_columns = if let Some(block) = self.state.chunks.first() {
            *self.state.column_types.as_mut() = block
                .columns()
                .iter()
                .map(|entry| entry.data_type())
                .collect();
            block.num_columns()
        } else {
            return;
        };

        *self.state.columns.as_mut() = (0..num_columns)
            .map(|offset| {
                let full_columns = self
                    .state
                    .chunks
                    .iter()
                    .map(|block| block.get_by_offset(offset).to_column())
                    .collect::<Vec<_>>();

                Column::take_downcast_column_vec(&full_columns)
            })
            .collect();
    }

    fn handle_block(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        let HashJoinHashTable::NestedLoop(build_blocks) = &*self.state.hash_table else {
            unreachable!()
        };

        let probe_rows = data.num_rows();
        let mut matched = Vec::with_capacity(probe_rows);
        for (chunk_index, build) in build_blocks.iter().enumerate() {
            for row_index in 0..build.num_rows() {
                let entries = data
                    .columns()
                    .iter()
                    .cloned()
                    .chain(build.columns().iter().map(|entry| {
                        BlockEntry::Const(
                            entry.index(row_index).unwrap().to_owned(),
                            entry.data_type(),
                            probe_rows,
                        )
                    }))
                    .collect();
                let result_count = self
                    .desc
                    .filter
                    .select(&DataBlock::new(entries, probe_rows))?;

                matched.extend(
                    self.desc.filter.true_selection()[..result_count]
                        .iter()
                        .copied()
                        .map(|probe| {
                            (probe, RowPtr {
                                chunk_index: chunk_index as _,
                                row_index: row_index as _,
                            })
                        }),
                );
            }
        }

        if matched.is_empty() {
            return Ok(None);
        }

        let mut bitmap = MutableBitmap::with_capacity(matched.len());
        for (i, _) in &matched {
            bitmap.set(*i as _, true);
        }
        let probe = data.filter_with_bitmap(&bitmap.freeze())?;

        matched.sort_by_key(|(v, _)| *v);
        let indices = matched.into_iter().map(|(_, row)| row).collect::<Vec<_>>();

        let build_entries = self
            .state
            .columns
            .iter()
            .zip(&*self.state.column_types)
            .map(|(columns, data_type)| {
                Column::take_column_vec_indices(columns, data_type.clone(), &indices, indices.len())
                    .into()
            });

        let data_block = DataBlock::from_iter(
            probe.take_columns().into_iter().chain(build_entries),
            indices.len(),
        );

        let Some(field_reorder) = &self.desc.field_reorder else {
            return Ok(Some(data_block));
        };
        let data_block = DataBlock::from_iter(
            field_reorder
                .iter()
                .map(|offset| data_block.get_by_offset(*offset).clone()),
            data_block.num_rows(),
        );

        Ok(Some(data_block))
    }
}

impl<T: Join> Join for NestedLoopJoin<T> {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.inner.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.inner.final_build()
    }

    fn build_runtime_filter(&self, desc: &RuntimeFiltersDesc) -> Result<JoinRuntimeFilterPacket> {
        self.inner.build_runtime_filter(desc)
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() || *self.state.build_rows == 0 {
            return Ok(Box::new(EmptyJoinStream));
        }

        if !matches!(*self.state.hash_table, HashJoinHashTable::NestedLoop(_)) {
            return self.inner.probe_block(data);
        }

        self.finalize_chunks();

        Ok(Box::new(OneBlockJoinStream(self.handle_block(data)?)))
    }
}
