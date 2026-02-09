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

use std::collections::VecDeque;
use std::collections::btree_map::Entry;
use std::sync::Arc;
use std::sync::PoisonError;

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_pipeline_transforms::traits::Location;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ReadSettings;

use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::get_hashes;
use crate::pipelines::processors::transforms::new_hash_join::grace::grace_memory::GraceMemoryJoin;
use crate::pipelines::processors::transforms::new_hash_join::grace::grace_state::GraceHashJoinState;
use crate::pipelines::processors::transforms::new_hash_join::grace::grace_state::SpillMetadata;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::sessions::QueryContext;
use crate::spillers::Layout;
use crate::spillers::SpillAdapter;
use crate::spillers::SpillTarget;
use crate::spillers::SpillsBufferPool;
use crate::spillers::SpillsDataReader;
use crate::spillers::SpillsDataWriter;

pub struct GraceHashJoin<T: GraceMemoryJoin> {
    pub(crate) desc: Arc<HashJoinDesc>,
    pub(crate) hash_method_kind: HashMethodKind,
    pub(crate) function_context: FunctionContext,

    pub(crate) location_prefix: String,
    pub(crate) shift_bits: usize,

    pub(crate) stage: RestoreStage,
    pub(crate) state: Arc<GraceHashJoinState>,

    pub(crate) partitions: Vec<GraceJoinPartition>,
    pub(crate) build_partition_stream: BlockPartitionStream,
    pub(crate) probe_partition_stream: BlockPartitionStream,

    pub(crate) memory_hash_join: T,
    pub(crate) read_settings: ReadSettings,
}

unsafe impl<T: GraceMemoryJoin> Send for GraceHashJoin<T> {}
unsafe impl<T: GraceMemoryJoin> Sync for GraceHashJoin<T> {}

impl<T: GraceMemoryJoin> Join for GraceHashJoin<T> {
    fn build_runtime_filter(&self) -> Result<JoinRuntimeFilterPacket> {
        // TODO: this is hacked to mark it as disabled, we may need look back latter
        Ok(JoinRuntimeFilterPacket::disable_all(0))
    }

    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        let ready_partitions = match data {
            None => self.finalize_build_data(),
            Some(data) => self.partition_build_data(data)?,
        };

        for (id, data_block) in ready_partitions {
            self.partitions[id].writer.write(data_block)?;
            self.partitions[id].writer.flush()?;
        }

        Ok(())
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        let mut partitions_meta = Vec::with_capacity(self.partitions.len());

        let mut ready_partitions = Vec::with_capacity(self.partitions.len());
        for id in 0..self.partitions.len() {
            let mut partition = GraceJoinPartition::create(&self.location_prefix)?;

            std::mem::swap(&mut self.partitions[id], &mut partition);
            ready_partitions.push(partition);
        }

        for (id, partition) in ready_partitions.into_iter().enumerate() {
            let path = partition.path;
            let (written, row_groups) = partition.writer.close()?;

            self.state
                .ctx
                .add_spill_file(Location::Remote(path.clone()), Layout::Parquet, written);

            if !row_groups.is_empty() {
                partitions_meta.push((id, SpillMetadata { path, row_groups }));
            }
        }

        if !partitions_meta.is_empty() {
            let locked = self.state.mutex.lock();
            let _locked = locked.unwrap_or_else(PoisonError::into_inner);

            for (id, metadata) in partitions_meta {
                match self.state.build_row_groups.as_mut().entry(id) {
                    Entry::Occupied(mut v) => {
                        v.get_mut().push(metadata);
                    }
                    Entry::Vacant(v) => {
                        v.insert(vec![metadata]);
                    }
                }
            }
        }

        Ok(None)
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        let ready_partitions = self.partition_probe_data(data)?;

        for (id, data_block) in ready_partitions {
            self.partitions[id].writer.write(data_block)?;
            self.partitions[id].writer.flush()?;
        }

        Ok(Box::new(EmptyJoinStream))
    }

    fn final_probe(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
        match self.stage {
            RestoreStage::FlushMemory => {
                self.finalize_probe_data()?;
                self.stage = RestoreStage::RestoreBuild;
                Ok(Some(Box::new(EmptyJoinStream)))
            }
            RestoreStage::RestoreBuild => {
                if !self.advance_restore_partition() {
                    return Ok(None);
                }

                self.restore_build_data()?;
                self.stage = RestoreStage::RestoreBuildFinal;
                Ok(Some(Box::new(EmptyJoinStream)))
            }
            RestoreStage::RestoreBuildFinal => {
                self.stage = RestoreStage::RestoreProbe;
                while let Some(_x) = self.memory_hash_join.final_build()? {}
                Ok(Some(Box::new(EmptyJoinStream)))
            }
            RestoreStage::RestoreProbe => {
                self.stage = RestoreStage::RestoreProbeFinal;
                Ok(Some(RestoreProbeStream::create(self)))
            }
            RestoreStage::RestoreProbeFinal => unsafe {
                // Note that this is safe: we are not violating the uniqueness of mutable references,
                // as the reuse of join always waits for the stream to be fully consumed.
                // However, it seems impossible to express this without using unsafe code.
                let join: &mut T = &mut *(&mut self.memory_hash_join as *mut _);
                if let Some(stream) = join.final_probe()? {
                    return Ok(Some(stream));
                }

                let locked = self.state.mutex.lock();
                let _locked = locked.unwrap_or_else(PoisonError::into_inner);

                self.stage = RestoreStage::RestoreBuild;
                *self.state.restore_partition.as_mut() = None;
                self.memory_hash_join.reset_memory();
                Ok(Some(Box::new(EmptyJoinStream)))
            },
        }
    }

    fn is_spill_happened(&self) -> bool {
        true
    }
}

impl<T: GraceMemoryJoin> GraceHashJoin<T> {
    pub fn create(
        ctx: Arc<QueryContext>,
        function_ctx: FunctionContext,
        hash_method_kind: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        state: Arc<GraceHashJoinState>,
        memory_hash_join: T,
        shift_bits: usize,
    ) -> Result<GraceHashJoin<T>> {
        let settings = ctx.get_settings();
        let rows = settings.get_max_block_size()? as usize;
        let bytes = settings.get_max_block_bytes()? as usize;
        let location_prefix = ctx.query_id_spill_prefix();

        let mut partitions = Vec::with_capacity(16);

        for _ in 0..16 {
            partitions.push(GraceJoinPartition::create(&location_prefix)?);
        }

        Ok(GraceHashJoin {
            desc,
            state,
            shift_bits,
            location_prefix,
            hash_method_kind,
            memory_hash_join,
            function_context: function_ctx,
            stage: RestoreStage::FlushMemory,
            partitions,
            read_settings: ReadSettings::from_settings(&ctx.get_settings())?,
            build_partition_stream: BlockPartitionStream::create(rows, bytes, 16),
            probe_partition_stream: BlockPartitionStream::create(rows, bytes, 16),
        })
    }

    fn advance_restore_partition(&mut self) -> bool {
        let locked = self.state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        if self.state.restore_partition.is_none() {
            let Some((id, data)) = self.state.build_row_groups.as_mut().pop_first() else {
                let Some((id, data)) = self.state.probe_row_groups.as_mut().pop_first() else {
                    *self.state.finished.as_mut() = true;
                    return false;
                };

                // Only left join?
                *self.state.restore_partition.as_mut() = Some(id);
                self.state.restore_build_queue.as_mut().clear();
                *self.state.restore_probe_queue.as_mut() = VecDeque::from(data);
                return true;
            };

            *self.state.restore_partition.as_mut() = Some(id);
            *self.state.restore_build_queue.as_mut() = VecDeque::from(data);

            self.state.restore_probe_queue.as_mut().clear();
            if let Some(probe_spills_data) = self.state.probe_row_groups.as_mut().remove(&id) {
                *self.state.restore_probe_queue.as_mut() = VecDeque::from(probe_spills_data);
            }
        }

        !*self.state.finished
    }

    fn steal_restore_build_task(&mut self) -> Option<SpillMetadata> {
        let locked = self.state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);
        self.state.restore_build_queue.as_mut().pop_front()
    }

    fn restore_build_data(&mut self) -> Result<()> {
        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let operator = data_operator.spill_operator();

        while let Some(data) = self.steal_restore_build_task() {
            let buffer_pool = SpillsBufferPool::instance();
            let mut reader =
                buffer_pool.reader(operator.clone(), data.path, data.row_groups, target)?;

            while let Some(data_block) = reader.read(self.read_settings)? {
                self.memory_hash_join.add_block(Some(data_block))?;
            }
        }

        self.memory_hash_join.add_block(None)
    }

    fn partition_build_data(&mut self, data: DataBlock) -> Result<Vec<(usize, DataBlock)>> {
        let mut hashes = Vec::with_capacity(data.num_rows());

        get_hashes(
            &self.function_context,
            &data,
            &self.desc.build_keys,
            &self.hash_method_kind,
            self.desc.join_type,
            true,
            &self.desc.is_null_equal,
            &mut hashes,
        )?;

        for hash in hashes.iter_mut() {
            *hash = ((*hash << self.shift_bits) >> 60) & 0b1111;
        }

        Ok(self.build_partition_stream.partition(hashes, data, true))
    }

    fn partition_probe_data(&mut self, data: DataBlock) -> Result<Vec<(usize, DataBlock)>> {
        let mut hashes = Vec::with_capacity(data.num_rows());

        get_hashes(
            &self.function_context,
            &data,
            &self.desc.probe_keys,
            &self.hash_method_kind,
            self.desc.join_type,
            false,
            &self.desc.is_null_equal,
            &mut hashes,
        )?;

        for hash in hashes.iter_mut() {
            *hash = ((*hash << self.shift_bits) >> 60) & 0b1111;
        }

        Ok(self.probe_partition_stream.partition(hashes, data, true))
    }

    fn finalize_build_data(&mut self) -> Vec<(usize, DataBlock)> {
        let ready_partitions_id = self.build_partition_stream.partition_ids();
        let mut ready_partitions = Vec::with_capacity(ready_partitions_id.len());
        for id in ready_partitions_id {
            if let Some(data) = self.build_partition_stream.finalize_partition(id) {
                ready_partitions.push((id, data));
            }
        }
        ready_partitions
    }

    fn finalize_probe_data(&mut self) -> Result<()> {
        let ready_partitions_id = self.probe_partition_stream.partition_ids();

        for id in ready_partitions_id {
            if let Some(data_block) = self.probe_partition_stream.finalize_partition(id) {
                self.partitions[id].writer.write(data_block)?;
                self.partitions[id].writer.flush()?;
            }
        }

        let ready_partitions = std::mem::take(&mut self.partitions);
        let mut partitions_meta = Vec::with_capacity(self.partitions.len());

        for (id, partition) in ready_partitions.into_iter().enumerate() {
            let path = partition.path;
            let (written, row_groups) = partition.writer.close()?;

            self.state
                .ctx
                .add_spill_file(Location::Remote(path.clone()), Layout::Parquet, written);

            if !row_groups.is_empty() {
                partitions_meta.push((id, SpillMetadata { path, row_groups }));
            }
        }

        if !partitions_meta.is_empty() {
            let locked = self.state.mutex.lock();
            let _locked = locked.unwrap_or_else(PoisonError::into_inner);

            for (id, metadata) in partitions_meta {
                match self.state.probe_row_groups.as_mut().entry(id) {
                    Entry::Occupied(mut v) => {
                        v.get_mut().push(metadata);
                    }
                    Entry::Vacant(v) => {
                        v.insert(vec![metadata]);
                    }
                }
            }
        }

        Ok(())
    }
}

pub enum RestoreStage {
    FlushMemory,
    RestoreBuild,
    RestoreBuildFinal,
    RestoreProbe,
    RestoreProbeFinal,
}

pub struct GraceJoinPartition {
    path: String,
    writer: SpillsDataWriter,
}

impl GraceJoinPartition {
    pub fn create(prefix: &str) -> Result<GraceJoinPartition> {
        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());

        let operator = data_operator.spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let file_path = format!("{}/{}", prefix, GlobalUniq::unique());
        let spills_data_writer = buffer_pool.writer(operator, file_path.clone(), target)?;

        Ok(GraceJoinPartition {
            path: file_path,
            writer: spills_data_writer,
        })
    }
}

pub struct RestoreProbeStream<'a, T: GraceMemoryJoin> {
    join: &'a mut GraceHashJoin<T>,
    spills_reader: Option<SpillsDataReader>,
    joined_stream: Option<Box<dyn JoinStream + 'a>>,
}

impl<'a, T: GraceMemoryJoin> RestoreProbeStream<'a, T> {
    pub fn create(join: &'a mut GraceHashJoin<T>) -> Box<dyn JoinStream + 'a> {
        Box::new(RestoreProbeStream::<'a, T> {
            join,
            spills_reader: None,
            joined_stream: None,
        })
    }

    fn steal_restore_probe_task(&mut self) -> Option<SpillMetadata> {
        let locked = self.join.state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);
        self.join.state.restore_probe_queue.as_mut().pop_front()
    }
}

impl<'a, T: GraceMemoryJoin> JoinStream for RestoreProbeStream<'a, T> {
    #[allow(clippy::missing_transmute_annotations)]
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if let Some(mut joined_stream) = self.joined_stream.take() {
                if let Some(joined_data) = joined_stream.next()? {
                    self.joined_stream = Some(joined_stream);
                    return Ok(Some(joined_data));
                }
            }

            let Some(block) = self.next_probe_block()? else {
                return Ok(None);
            };

            // Note that this is safe: we are not violating the uniqueness of mutable references,
            // as the reuse of join always waits for the stream to be fully consumed.
            // However, it seems impossible to express this without using unsafe code.
            unsafe {
                let join: &mut GraceHashJoin<T> = &mut *(self.join as *mut _);
                self.joined_stream = Some(std::mem::transmute(
                    join.memory_hash_join.probe_block(block)?,
                ));
            }
        }
    }
}

impl<'a, T: GraceMemoryJoin> RestoreProbeStream<'a, T> {
    fn next_probe_block(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if self.spills_reader.is_none() {
                while let Some(data) = self.steal_restore_probe_task() {
                    if data.row_groups.is_empty() {
                        continue;
                    }

                    let data_operator = DataOperator::instance();
                    let target = SpillTarget::from_storage_params(data_operator.spill_params());
                    let operator = data_operator.spill_operator();
                    let buffer_pool = SpillsBufferPool::instance();
                    let reader =
                        buffer_pool.reader(operator, data.path, data.row_groups, target)?;
                    self.spills_reader = Some(reader);
                    break;
                }

                if self.spills_reader.is_none() {
                    return Ok(None);
                }
            }

            if let Some(mut spills_reader) = self.spills_reader.take() {
                if let Some(v) = spills_reader.read(self.join.read_settings)? {
                    self.spills_reader = Some(spills_reader);
                    return Ok(Some(v));
                }
            }
        }
    }
}
