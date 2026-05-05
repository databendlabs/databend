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

use std::cmp::max;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::traits::Location;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ReadSettings;
use opendal::Operator;
use parquet::file::metadata::RowGroupMetaData;

use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinFactory;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;
use crate::spillers::Layout;
use crate::spillers::SpillTarget;
use crate::spillers::SpillsBufferPool;
use crate::spillers::SpillsDataWriter;

pub enum CrossHashJoin {
    Memory(CrossHashJoinMemory),
    Spill(CrossHashJoinSpill),
}

unsafe impl Send for CrossHashJoin {}
unsafe impl Sync for CrossHashJoin {}

impl CrossHashJoin {
    pub fn create(
        ctx: Arc<QueryContext>,
        desc: Arc<HashJoinDesc>,
        shared: Arc<CrossJoinShared>,
        cache_state: Option<Arc<BasicHashJoinState>>,
        memory_settings: MemorySettings,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let max_block_size = settings.get_max_block_size()? as usize;
        let max_block_bytes = settings.get_max_block_bytes()? as usize;

        Ok(CrossHashJoin::Memory(CrossHashJoinMemory {
            ctx,
            desc,
            shared,
            cache_state,
            memory_settings,
            max_block_size,
            max_block_bytes,
            build_builder: FixedBlockBuilder::new(max_block_size, max_block_bytes),
            probe_builder: FixedBlockBuilder::new(max_block_size, max_block_bytes),
            local_build_groups: Vec::new(),
            build_finished: false,
            build_published: false,
            cache_published: false,
            progress_reported: false,
        }))
    }

    fn transition_to_spill(&mut self, probe_groups: Vec<DataBlock>) -> Result<()> {
        let spill = match self {
            CrossHashJoin::Memory(memory) => memory.create_spill(probe_groups)?,
            CrossHashJoin::Spill(spill) => {
                spill.write_probe_groups(probe_groups)?;
                return Ok(());
            }
        };
        *self = CrossHashJoin::Spill(spill);
        Ok(())
    }

    fn add_build_block(&mut self, block: DataBlock) -> Result<()> {
        let mut block = Some(block);
        loop {
            match self {
                CrossHashJoin::Memory(memory) => {
                    if memory.shared.check_spilled() {
                        self.transition_to_spill(Vec::new())?;
                        continue;
                    }

                    let should_spill = memory.add_build_block(block.take().unwrap())?;
                    if should_spill {
                        memory.shared.set_spilled();
                        self.transition_to_spill(Vec::new())?;
                    }
                    return Ok(());
                }
                CrossHashJoin::Spill(spill) => return spill.add_block(block.take()),
            }
        }
    }

    fn finish_build(&mut self) -> Result<()> {
        loop {
            match self {
                CrossHashJoin::Memory(memory) => {
                    if memory.shared.check_spilled() || memory.memory_settings.check_spill() {
                        memory.shared.set_spilled();
                        self.transition_to_spill(Vec::new())?;
                        continue;
                    }
                    memory.finish_build()?;
                    if memory.memory_settings.check_spill() {
                        memory.shared.set_spilled();
                        self.transition_to_spill(Vec::new())?;
                        continue;
                    }
                    return Ok(());
                }
                CrossHashJoin::Spill(spill) => return spill.add_block(None),
            }
        }
    }
}

impl Join for CrossHashJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        match data {
            Some(block) => self.add_build_block(block),
            None => self.finish_build(),
        }
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        loop {
            match self {
                CrossHashJoin::Memory(memory) => {
                    if memory.shared.check_spilled() {
                        memory.shared.set_spilled();
                        self.transition_to_spill(Vec::new())?;
                        continue;
                    }
                    return memory.final_build();
                }
                CrossHashJoin::Spill(spill) => return spill.final_build(),
            }
        }
    }

    fn build_runtime_filter(&self) -> Result<JoinRuntimeFilterPacket> {
        Ok(JoinRuntimeFilterPacket::default())
    }

    fn is_spill_happened(&self) -> bool {
        match self {
            CrossHashJoin::Memory(memory) => memory.shared.has_spilled_once(),
            CrossHashJoin::Spill(spill) => spill.shared.has_spilled_once(),
        }
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        let mut data = Some(data);
        loop {
            match self {
                CrossHashJoin::Memory(memory) => {
                    if memory.shared.check_spilled() || memory.memory_settings.check_spill() {
                        memory.shared.set_spilled();
                        self.transition_to_spill(Vec::new())?;
                        continue;
                    }

                    let probe_result = memory.probe_block(data.take().unwrap())?;
                    match probe_result {
                        MemoryProbeResult::Stream(stream) => return Ok(stream),
                        MemoryProbeResult::Spill(probe_groups) => {
                            memory.shared.set_spilled();
                            self.transition_to_spill(probe_groups)?;
                            return Ok(Box::new(EmptyJoinStream));
                        }
                    }
                }
                CrossHashJoin::Spill(spill) => return spill.probe_block(data.take().unwrap()),
            }
        }
    }

    fn final_probe(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
        let mut transition_probe_groups = None;
        if let CrossHashJoin::Memory(memory) = self
            && memory.shared.check_spilled()
        {
            memory.shared.set_spilled();
            transition_probe_groups = Some(memory.finish_probe_groups()?);
        }

        if let Some(probe_groups) = transition_probe_groups {
            self.transition_to_spill(probe_groups)?;
        }

        match self {
            CrossHashJoin::Memory(memory) => memory.final_probe(),
            CrossHashJoin::Spill(spill) => spill.final_probe(),
        }
    }
}

pub struct CrossHashJoinMemory {
    ctx: Arc<QueryContext>,
    desc: Arc<HashJoinDesc>,
    shared: Arc<CrossJoinShared>,
    cache_state: Option<Arc<BasicHashJoinState>>,
    memory_settings: MemorySettings,
    max_block_size: usize,
    max_block_bytes: usize,
    build_builder: FixedBlockBuilder,
    probe_builder: FixedBlockBuilder,
    local_build_groups: Vec<DataBlock>,
    build_finished: bool,
    build_published: bool,
    cache_published: bool,
    progress_reported: bool,
}

impl CrossHashJoinMemory {
    fn add_build_block(&mut self, block: DataBlock) -> Result<bool> {
        if block.is_empty() {
            return Ok(false);
        }

        let groups = self.build_builder.add(block)?;
        self.local_build_groups.extend(groups);
        Ok(self.memory_settings.check_spill())
    }

    fn finish_build(&mut self) -> Result<()> {
        if self.build_finished {
            return Ok(());
        }

        let groups = self.build_builder.finish()?;
        self.local_build_groups.extend(groups);
        self.build_finished = true;
        self.publish_cache_build_groups();
        self.publish_memory_build_groups();
        Ok(())
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        if !self.build_finished {
            self.finish_build()?;
        }

        if self.progress_reported {
            return Ok(None);
        }
        self.progress_reported = true;

        let rows = self
            .local_build_groups
            .iter()
            .map(DataBlock::num_rows)
            .sum();
        let bytes = self
            .local_build_groups
            .iter()
            .map(DataBlock::memory_size)
            .sum();
        Ok(Some(ProgressValues { rows, bytes }))
    }

    fn probe_block(&mut self, block: DataBlock) -> Result<MemoryProbeResult> {
        if block.is_empty() {
            return Ok(MemoryProbeResult::Stream(Box::new(EmptyJoinStream)));
        }

        let probe_groups = self.probe_builder.add(block)?;
        if self.memory_settings.check_spill() || self.shared.check_spilled() {
            let mut groups = probe_groups;
            groups.extend(self.finish_probe_groups()?);
            return Ok(MemoryProbeResult::Spill(groups));
        }

        Ok(MemoryProbeResult::Stream(
            self.create_memory_stream(probe_groups),
        ))
    }

    fn final_probe(&mut self) -> Result<Option<Box<dyn JoinStream>>> {
        let probe_groups = self.finish_probe_groups()?;
        if probe_groups.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.create_memory_stream(probe_groups)))
    }

    fn finish_probe_groups(&mut self) -> Result<Vec<DataBlock>> {
        self.probe_builder.finish()
    }

    fn create_memory_stream(&self, probe_groups: Vec<DataBlock>) -> Box<dyn JoinStream> {
        if probe_groups.is_empty() {
            return Box::new(EmptyJoinStream);
        }

        let build_groups = self.shared.memory_build_groups();
        if build_groups.is_empty() {
            return Box::new(EmptyJoinStream);
        }

        Box::new(MemoryCrossJoinStream {
            desc: self.desc.clone(),
            build_groups,
            probe_groups,
            build_group_index: 0,
            probe_group_index: 0,
            current: None,
            max_block_size: self.max_block_size,
            max_block_bytes: self.max_block_bytes,
        })
    }

    fn publish_memory_build_groups(&mut self) {
        if self.build_published {
            return;
        }
        self.shared
            .publish_memory_build_groups(self.local_build_groups.clone());
        self.build_published = true;
    }

    fn publish_cache_build_groups(&mut self) {
        if self.cache_published {
            return;
        }
        self.cache_published = true;

        let Some(cache_state) = &self.cache_state else {
            return;
        };

        for group in &self.local_build_groups {
            cache_state.push_chunk(group.clone());
        }
    }

    fn create_spill(&mut self, probe_groups: Vec<DataBlock>) -> Result<CrossHashJoinSpill> {
        let mut spill = CrossHashJoinSpill::create(
            self.ctx.clone(),
            self.desc.clone(),
            self.shared.clone(),
            self.cache_state.clone(),
            self.max_block_size,
            self.max_block_bytes,
        )?;

        let groups = self.build_builder.finish()?;
        self.local_build_groups.extend(groups);
        self.publish_cache_build_groups();
        let shared_build_groups = self.shared.take_memory_build_groups_for_spill();
        spill.write_build_groups(shared_build_groups)?;

        if !self.build_published {
            let local_build_groups = std::mem::take(&mut self.local_build_groups);
            spill.write_build_groups(local_build_groups)?;
        }

        if self.build_finished {
            spill.finish_build_spill()?;
        }

        let pending_probe_groups = self.probe_builder.finish()?;
        spill.write_probe_groups(pending_probe_groups)?;
        spill.write_probe_groups(probe_groups)?;

        Ok(spill)
    }
}

enum MemoryProbeResult {
    Stream(Box<dyn JoinStream>),
    Spill(Vec<DataBlock>),
}

pub struct CrossHashJoinSpill {
    ctx: Arc<QueryContext>,
    desc: Arc<HashJoinDesc>,
    shared: Arc<CrossJoinShared>,
    cache_state: Option<Arc<BasicHashJoinState>>,
    max_block_size: usize,
    max_block_bytes: usize,
    build_builder: FixedBlockBuilder,
    probe_builder: FixedBlockBuilder,
    build_writer: Option<CrossSpillWriter>,
    probe_writer: Option<CrossSpillWriter>,
    build_published: bool,
    probe_published: bool,
    task_stream_returned: bool,
    task_stream_done_published: bool,
}

impl CrossHashJoinSpill {
    fn create(
        ctx: Arc<QueryContext>,
        desc: Arc<HashJoinDesc>,
        shared: Arc<CrossJoinShared>,
        cache_state: Option<Arc<BasicHashJoinState>>,
        max_block_size: usize,
        max_block_bytes: usize,
    ) -> Result<Self> {
        Ok(Self {
            ctx: ctx.clone(),
            desc,
            shared,
            cache_state,
            max_block_size,
            max_block_bytes,
            build_builder: FixedBlockBuilder::new(max_block_size, max_block_bytes),
            probe_builder: FixedBlockBuilder::new(max_block_size, max_block_bytes),
            build_writer: Some(CrossSpillWriter::create(ctx.clone())?),
            probe_writer: None,
            build_published: false,
            probe_published: false,
            task_stream_returned: false,
            task_stream_done_published: false,
        })
    }

    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        match data {
            Some(block) => {
                let groups = self.build_builder.add(block)?;
                self.publish_cache_build_groups(&groups);
                self.write_build_groups(groups)
            }
            None => self.finish_build_spill(),
        }
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.finish_build_spill()?;
        Ok(None)
    }

    fn probe_block(&mut self, block: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        let groups = self.probe_builder.add(block)?;
        self.write_probe_groups(groups)?;
        Ok(Box::new(EmptyJoinStream))
    }

    fn final_probe(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
        self.finish_probe_spill()?;

        if self.task_stream_returned {
            if !self.task_stream_done_published {
                self.shared.publish_task_stream_done();
                self.task_stream_done_published = true;
            }

            return match self.shared.all_task_stream_done() {
                true => Ok(None),
                false => Ok(Some(Box::new(EmptyJoinStream))),
            };
        }

        if !self.shared.try_init_spill_tasks() {
            return Ok(Some(Box::new(EmptyJoinStream)));
        }
        self.task_stream_returned = true;

        let read_settings = ReadSettings::from_settings(&self.ctx.get_settings())?;
        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let operator = data_operator.spill_operator();

        Ok(Some(Box::new(SpillCrossJoinTaskStream {
            desc: self.desc.clone(),
            shared: self.shared.clone(),
            read_settings,
            target,
            operator,
            current: None,
            max_block_size: self.max_block_size,
            max_block_bytes: self.max_block_bytes,
        })))
    }

    fn write_build_groups(&mut self, groups: Vec<DataBlock>) -> Result<()> {
        if groups.is_empty() {
            return Ok(());
        }
        let writer = self.build_writer()?;
        for group in groups {
            writer.write_group(group)?;
        }
        Ok(())
    }

    fn write_probe_groups(&mut self, groups: Vec<DataBlock>) -> Result<()> {
        if groups.is_empty() {
            return Ok(());
        }
        if self.probe_writer.is_none() {
            self.probe_writer = Some(CrossSpillWriter::create(self.ctx.clone())?);
        }
        let writer = self.probe_writer.as_mut().unwrap();
        for group in groups {
            writer.write_group(group)?;
        }
        Ok(())
    }

    fn finish_build_spill(&mut self) -> Result<()> {
        if self.build_published {
            return Ok(());
        }

        let groups = self.build_builder.finish()?;
        self.publish_cache_build_groups(&groups);
        self.write_build_groups(groups)?;

        let row_groups = match self.build_writer.take() {
            Some(writer) => writer.close()?,
            None => Vec::new(),
        };
        self.shared.publish_spill_build_row_groups(row_groups);
        self.build_published = true;
        Ok(())
    }

    fn finish_probe_spill(&mut self) -> Result<()> {
        if self.probe_published {
            return Ok(());
        }

        let groups = self.probe_builder.finish()?;
        self.write_probe_groups(groups)?;

        let row_groups = match self.probe_writer.take() {
            Some(writer) => writer.close()?,
            None => Vec::new(),
        };
        self.shared.publish_spill_probe_row_groups(row_groups);
        self.probe_published = true;
        Ok(())
    }

    fn build_writer(&mut self) -> Result<&mut CrossSpillWriter> {
        if self.build_writer.is_none() {
            self.build_writer = Some(CrossSpillWriter::create(self.ctx.clone())?);
        }
        Ok(self.build_writer.as_mut().unwrap())
    }

    fn publish_cache_build_groups(&self, groups: &[DataBlock]) {
        let Some(cache_state) = &self.cache_state else {
            return;
        };

        for group in groups {
            cache_state.push_chunk(group.clone());
        }
    }
}

pub struct CrossJoinShared {
    id: usize,
    factory: Weak<HashJoinFactory>,
    processor_count: usize,
    spilled: AtomicBool,
    ever_spilled: AtomicBool,
    spill_build_done_count: AtomicUsize,
    spill_probe_done_count: AtomicUsize,
    task_stream_done_count: AtomicUsize,
    task_initialized: AtomicBool,
    next_task_index: AtomicUsize,
    total_tasks: AtomicUsize,
    mutex: Mutex<CrossJoinSharedData>,
}

#[derive(Default)]
struct CrossJoinSharedData {
    memory_build_groups: Vec<DataBlock>,
    spill_build_row_groups: Vec<CrossSpillRowGroup>,
    spill_probe_row_groups: Vec<CrossSpillRowGroup>,
}

impl CrossJoinShared {
    pub fn create(id: usize, factory: Arc<HashJoinFactory>, processor_count: usize) -> Arc<Self> {
        Arc::new(Self {
            id,
            factory: Arc::downgrade(&factory),
            processor_count,
            spilled: AtomicBool::new(false),
            ever_spilled: AtomicBool::new(false),
            spill_build_done_count: AtomicUsize::new(0),
            spill_probe_done_count: AtomicUsize::new(0),
            task_stream_done_count: AtomicUsize::new(0),
            task_initialized: AtomicBool::new(false),
            next_task_index: AtomicUsize::new(0),
            total_tasks: AtomicUsize::new(0),
            mutex: Mutex::new(CrossJoinSharedData::default()),
        })
    }

    fn check_spilled(&self) -> bool {
        self.spilled.load(Ordering::Acquire)
    }

    fn set_spilled(&self) -> bool {
        self.ever_spilled.store(true, Ordering::Release);
        !self.spilled.swap(true, Ordering::AcqRel)
    }

    fn has_spilled_once(&self) -> bool {
        self.ever_spilled.load(Ordering::Acquire)
    }

    fn publish_memory_build_groups(&self, groups: Vec<DataBlock>) {
        if groups.is_empty() {
            return;
        }
        let locked = self.mutex.lock();
        let mut locked = locked.unwrap_or_else(PoisonError::into_inner);
        locked.memory_build_groups.extend(groups);
    }

    fn memory_build_groups(&self) -> Vec<DataBlock> {
        let locked = self.mutex.lock();
        let locked = locked.unwrap_or_else(PoisonError::into_inner);
        locked.memory_build_groups.clone()
    }

    fn take_memory_build_groups_for_spill(&self) -> Vec<DataBlock> {
        let locked = self.mutex.lock();
        let mut locked = locked.unwrap_or_else(PoisonError::into_inner);
        std::mem::take(&mut locked.memory_build_groups)
    }

    fn publish_spill_build_row_groups(&self, groups: Vec<CrossSpillRowGroup>) {
        if !groups.is_empty() {
            let locked = self.mutex.lock();
            let mut locked = locked.unwrap_or_else(PoisonError::into_inner);
            locked.spill_build_row_groups.extend(groups);
        }
        self.spill_build_done_count.fetch_add(1, Ordering::AcqRel);
    }

    fn publish_spill_probe_row_groups(&self, groups: Vec<CrossSpillRowGroup>) {
        if !groups.is_empty() {
            let locked = self.mutex.lock();
            let mut locked = locked.unwrap_or_else(PoisonError::into_inner);
            locked.spill_probe_row_groups.extend(groups);
        }
        self.spill_probe_done_count.fetch_add(1, Ordering::AcqRel);
    }

    fn try_init_spill_tasks(&self) -> bool {
        if self.task_initialized.load(Ordering::Acquire) {
            return true;
        }

        if self.processor_count == 0
            || self.spill_build_done_count.load(Ordering::Acquire) < self.processor_count
            || self.spill_probe_done_count.load(Ordering::Acquire) < self.processor_count
        {
            return false;
        }

        let locked = self.mutex.lock();
        let locked = locked.unwrap_or_else(PoisonError::into_inner);

        if !self.task_initialized.load(Ordering::Acquire) {
            let total_tasks = locked
                .spill_build_row_groups
                .len()
                .saturating_mul(locked.spill_probe_row_groups.len());
            self.next_task_index.store(0, Ordering::Release);
            self.total_tasks.store(total_tasks, Ordering::Release);
            self.task_initialized.store(true, Ordering::Release);
        }

        true
    }

    fn next_spill_task(&self) -> Option<CrossJoinTask> {
        let task_index = self.next_task_index.fetch_add(1, Ordering::AcqRel);
        if task_index >= self.total_tasks.load(Ordering::Acquire) {
            return None;
        }

        let locked = self.mutex.lock();
        let locked = locked.unwrap_or_else(PoisonError::into_inner);
        let probe_len = locked.spill_probe_row_groups.len();
        if probe_len == 0 {
            return None;
        }

        let build_index = task_index / probe_len;
        let probe_index = task_index % probe_len;
        Some(CrossJoinTask {
            build: locked.spill_build_row_groups[build_index].clone(),
            probe: locked.spill_probe_row_groups[probe_index].clone(),
        })
    }

    fn publish_task_stream_done(&self) {
        self.task_stream_done_count.fetch_add(1, Ordering::AcqRel);
    }

    fn all_task_stream_done(&self) -> bool {
        self.task_stream_done_count.load(Ordering::Acquire) >= self.processor_count
    }
}

impl Drop for CrossJoinShared {
    fn drop(&mut self) {
        if let Some(factory) = self.factory.upgrade() {
            factory.remove_cross_state(self.id);
        }
    }
}

struct FixedBlockBuilder {
    max_rows: usize,
    max_bytes: usize,
    pending_blocks: Vec<DataBlock>,
    pending_rows: usize,
    pending_bytes: usize,
}

impl FixedBlockBuilder {
    fn new(max_rows: usize, max_bytes: usize) -> Self {
        Self {
            max_rows: max(1, max_rows),
            max_bytes: max(1, max_bytes),
            pending_blocks: Vec::new(),
            pending_rows: 0,
            pending_bytes: 0,
        }
    }

    fn add(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        let mut groups = Vec::new();
        if block.is_empty() {
            return Ok(groups);
        }

        let split_rows = self.split_rows(&block);
        let total_rows = block.num_rows();
        let mut start = 0;
        while start < total_rows {
            let rows = (total_rows - start).min(split_rows);
            let part = block.slice(start..start + rows).maybe_gc();
            self.add_part(part, &mut groups)?;
            start += rows;
        }

        Ok(groups)
    }

    fn finish(&mut self) -> Result<Vec<DataBlock>> {
        if self.pending_blocks.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![self.take_pending()?])
    }

    fn split_rows(&self, block: &DataBlock) -> usize {
        let avg_bytes = block.memory_size().div_ceil(max(1, block.num_rows()));
        let rows_by_bytes = match avg_bytes {
            0 => self.max_rows,
            _ => max(1, self.max_bytes / avg_bytes),
        };
        max(1, self.max_rows.min(rows_by_bytes))
    }

    fn add_part(&mut self, block: DataBlock, groups: &mut Vec<DataBlock>) -> Result<()> {
        let block_rows = block.num_rows();
        let block_bytes = block.memory_size();

        if !self.pending_blocks.is_empty()
            && (self.pending_rows + block_rows > self.max_rows
                || self.pending_bytes + block_bytes > self.max_bytes)
        {
            groups.push(self.take_pending()?);
        }

        if self.pending_blocks.is_empty()
            && (block_rows >= self.max_rows || block_bytes >= self.max_bytes)
        {
            groups.push(block);
            return Ok(());
        }

        self.pending_rows += block_rows;
        self.pending_bytes += block_bytes;
        self.pending_blocks.push(block);

        if self.pending_rows >= self.max_rows || self.pending_bytes >= self.max_bytes {
            groups.push(self.take_pending()?);
        }

        Ok(())
    }

    fn take_pending(&mut self) -> Result<DataBlock> {
        let blocks = std::mem::take(&mut self.pending_blocks);
        self.pending_rows = 0;
        self.pending_bytes = 0;
        if blocks.len() == 1 {
            Ok(blocks.into_iter().next().unwrap())
        } else {
            DataBlock::concat(&blocks)
        }
    }
}

struct CrossSpillWriter {
    ctx: Arc<QueryContext>,
    path: String,
    writer: SpillsDataWriter,
}

impl CrossSpillWriter {
    fn create(ctx: Arc<QueryContext>) -> Result<Self> {
        let operator = DataOperator::instance().spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let path = format!("{}/{}", ctx.query_id_spill_prefix(), GlobalUniq::unique());
        let writer = buffer_pool.writer(operator, path.clone())?;

        Ok(Self { ctx, path, writer })
    }

    fn write_group(&mut self, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }
        self.writer.write(block)?;
        self.writer.flush()
    }

    fn close(self) -> Result<Vec<CrossSpillRowGroup>> {
        let Self { ctx, path, writer } = self;
        let (_written, row_groups) = writer.close()?;
        ctx.add_spill_file(Location::Remote(path.clone()), Layout::Parquet);

        Ok(row_groups
            .into_iter()
            .map(|row_group| CrossSpillRowGroup {
                path: path.clone(),
                row_group,
            })
            .collect())
    }
}

#[derive(Clone)]
struct CrossSpillRowGroup {
    path: String,
    row_group: RowGroupMetaData,
}

#[cfg_attr(test, derive(Clone))]
struct CrossJoinTask {
    build: CrossSpillRowGroup,
    probe: CrossSpillRowGroup,
}

struct MemoryCrossJoinStream {
    desc: Arc<HashJoinDesc>,
    build_groups: Vec<DataBlock>,
    probe_groups: Vec<DataBlock>,
    probe_group_index: usize,
    build_group_index: usize,
    current: Option<CrossBlockOutputStream>,
    max_block_size: usize,
    max_block_bytes: usize,
}

impl JoinStream for MemoryCrossJoinStream {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if let Some(current) = self.current.as_mut() {
                if let Some(block) = current.next()? {
                    return Ok(Some(block));
                }
                self.current = None;
            }

            if self.probe_group_index >= self.probe_groups.len() {
                return Ok(None);
            }

            if self.build_group_index >= self.build_groups.len() {
                self.build_group_index = 0;
                self.probe_group_index += 1;
                continue;
            }

            let build_block = self.build_groups[self.build_group_index].clone();
            let probe_block = self.probe_groups[self.probe_group_index].clone();
            self.build_group_index += 1;
            self.current = Some(CrossBlockOutputStream::create(
                self.desc.clone(),
                build_block,
                probe_block,
                self.max_block_size,
                self.max_block_bytes,
            )?);
        }
    }
}

struct SpillCrossJoinTaskStream {
    desc: Arc<HashJoinDesc>,
    shared: Arc<CrossJoinShared>,
    read_settings: ReadSettings,
    target: SpillTarget,
    operator: Operator,
    current: Option<CrossBlockOutputStream>,
    max_block_size: usize,
    max_block_bytes: usize,
}

impl JoinStream for SpillCrossJoinTaskStream {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if let Some(current) = self.current.as_mut() {
                if let Some(block) = current.next()? {
                    return Ok(Some(block));
                }
                self.current = None;
            }

            let Some(task) = self.shared.next_spill_task() else {
                return Ok(None);
            };

            let build_block = read_spill_row_group(
                &task.build,
                self.desc.build_schema.clone(),
                self.read_settings,
                self.operator.clone(),
                self.target,
            )?;
            let probe_block = read_spill_row_group(
                &task.probe,
                self.desc.probe_schema.clone(),
                self.read_settings,
                self.operator.clone(),
                self.target,
            )?;

            self.current = Some(CrossBlockOutputStream::create(
                self.desc.clone(),
                build_block,
                probe_block,
                self.max_block_size,
                self.max_block_bytes,
            )?);
        }
    }
}

fn read_spill_row_group(
    row_group: &CrossSpillRowGroup,
    schema: DataSchemaRef,
    read_settings: ReadSettings,
    operator: Operator,
    target: SpillTarget,
) -> Result<DataBlock> {
    let buffer_pool = SpillsBufferPool::instance();
    let mut reader = buffer_pool.reader(
        operator,
        row_group.path.clone(),
        schema.clone(),
        vec![row_group.row_group.clone()],
        target,
        read_settings,
    )?;

    let mut blocks = Vec::new();
    while let Some(block) = reader.read()? {
        if !block.is_empty() {
            blocks.push(block);
        }
    }

    match blocks.len() {
        0 => Ok(DataBlock::empty_with_schema(&schema)),
        1 => Ok(blocks.pop().unwrap()),
        _ => DataBlock::concat(&blocks),
    }
}

struct CrossBlockOutputStream {
    build_block: DataBlock,
    probe_block: DataBlock,
    probe_row: usize,
    build_row: usize,
    max_block_size: usize,
    max_block_bytes: usize,
    build_avg_row_bytes: usize,
    probe_avg_row_bytes: usize,
}

impl CrossBlockOutputStream {
    fn create(
        desc: Arc<HashJoinDesc>,
        build_block: DataBlock,
        probe_block: DataBlock,
        max_block_size: usize,
        max_block_bytes: usize,
    ) -> Result<Self> {
        Self::create_projected(
            build_block.project(&desc.build_projection),
            probe_block.project(&desc.probe_projection),
            max_block_size,
            max_block_bytes,
        )
    }

    fn create_projected(
        build_block: DataBlock,
        probe_block: DataBlock,
        max_block_size: usize,
        max_block_bytes: usize,
    ) -> Result<Self> {
        let build_avg_row_bytes = build_block
            .memory_size()
            .div_ceil(max(1, build_block.num_rows()));
        let probe_avg_row_bytes = probe_block
            .memory_size()
            .div_ceil(max(1, probe_block.num_rows()));

        Ok(Self {
            build_block,
            probe_block,
            probe_row: 0,
            build_row: 0,
            max_block_size: max(1, max_block_size),
            max_block_bytes: max(1, max_block_bytes),
            build_avg_row_bytes,
            probe_avg_row_bytes,
        })
    }
}

impl JoinStream for CrossBlockOutputStream {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        let build_rows = self.build_block.num_rows();
        let probe_rows = self.probe_block.num_rows();
        if build_rows == 0 || self.probe_row >= probe_rows {
            return Ok(None);
        }

        if build_rows == 1 && self.build_row == 0 {
            let remaining_probe_rows = probe_rows - self.probe_row;
            let bytes_per_row = max(1, self.build_avg_row_bytes + self.probe_avg_row_bytes);
            let rows_by_bytes = max(1, self.max_block_bytes / bytes_per_row);
            let take_rows = remaining_probe_rows
                .min(self.max_block_size)
                .min(rows_by_bytes);

            let mut probe_block = self
                .probe_block
                .slice(self.probe_row..self.probe_row + take_rows)
                .maybe_gc();
            for col in self.build_block.columns() {
                let scalar = unsafe { col.index_unchecked(0) };
                probe_block.add_const_column(scalar.to_owned(), col.data_type());
            }
            self.probe_row += take_rows;
            return Ok(Some(probe_block));
        }

        let remaining_build_rows = build_rows - self.build_row;
        let bytes_per_row = max(1, self.build_avg_row_bytes + self.probe_avg_row_bytes);
        let rows_by_bytes = max(1, self.max_block_bytes / bytes_per_row);
        let take_rows = remaining_build_rows
            .min(self.max_block_size)
            .min(rows_by_bytes);

        let columns =
            Vec::with_capacity(self.probe_block.num_columns() + self.build_block.num_columns());
        let mut replicated_probe_block = DataBlock::new(columns, take_rows);
        for col in self.probe_block.columns() {
            let scalar = unsafe { col.index_unchecked(self.probe_row) };
            replicated_probe_block.add_const_column(scalar.to_owned(), col.data_type());
        }

        let build_block = self
            .build_block
            .slice(self.build_row..self.build_row + take_rows)
            .maybe_gc();
        replicated_probe_block.merge_block(build_block);

        self.build_row += take_rows;
        if self.build_row >= build_rows {
            self.build_row = 0;
            self.probe_row += 1;
        }

        Ok(Some(replicated_probe_block))
    }
}

pub type CrossStateMap = HashMap<usize, Weak<CrossJoinShared>>;

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::types::number::Int32Type;
    use databend_common_expression::types::number::NumberScalar;

    use super::*;

    fn int_block(values: Vec<i32>) -> DataBlock {
        DataBlock::new_from_columns(vec![Int32Type::from_data(values)])
    }

    fn int_value(block: &DataBlock, column: usize, row: usize) -> i32 {
        match block.get_by_offset(column).index(row).unwrap() {
            ScalarRef::Number(NumberScalar::Int32(value)) => value,
            _ => panic!("unexpected scalar"),
        }
    }

    #[test]
    fn test_fixed_block_builder_splits_rows() -> Result<()> {
        let mut builder = FixedBlockBuilder::new(2, usize::MAX / 2);
        let groups = builder.add(int_block(vec![1, 2, 3, 4, 5]))?;
        let rest = builder.finish()?;
        let rows = groups
            .into_iter()
            .chain(rest)
            .map(|block| block.num_rows())
            .collect::<Vec<_>>();
        assert_eq!(rows, vec![2, 2, 1]);
        Ok(())
    }

    #[test]
    fn test_cross_block_output_stream_splits_rows() -> Result<()> {
        let mut stream = CrossBlockOutputStream::create_projected(
            int_block(vec![10, 20, 30]),
            int_block(vec![1, 2]),
            2,
            usize::MAX / 2,
        )?;

        let mut rows = Vec::new();
        while let Some(block) = stream.next()? {
            assert!(block.num_rows() <= 2);
            rows.push(block.num_rows());
        }
        assert_eq!(rows, vec![2, 1, 2, 1]);
        Ok(())
    }

    #[test]
    fn test_cross_block_output_stream_batches_single_build_row() -> Result<()> {
        let mut stream = CrossBlockOutputStream::create_projected(
            int_block(vec![10]),
            int_block(vec![1, 2, 3, 4, 5]),
            3,
            usize::MAX / 2,
        )?;

        let mut rows = Vec::new();
        while let Some(block) = stream.next()? {
            rows.push(block.num_rows());
        }
        assert_eq!(rows, vec![3, 2]);
        Ok(())
    }

    #[test]
    fn test_cross_block_output_stream_outputs_cartesian_product() -> Result<()> {
        let mut stream = CrossBlockOutputStream::create_projected(
            int_block(vec![10, 20, 30]),
            int_block(vec![1, 2]),
            10,
            usize::MAX / 2,
        )?;

        let mut rows = Vec::new();
        while let Some(block) = stream.next()? {
            for row in 0..block.num_rows() {
                rows.push((int_value(&block, 0, row), int_value(&block, 1, row)));
            }
        }

        assert_eq!(rows, vec![
            (1, 10),
            (1, 20),
            (1, 30),
            (2, 10),
            (2, 20),
            (2, 30)
        ]);
        Ok(())
    }

    #[test]
    fn test_spill_task_initialization_publishes_tasks_before_ready() {
        let shared = Arc::new(CrossJoinShared {
            id: 0,
            factory: Weak::new(),
            processor_count: 1,
            spilled: AtomicBool::new(false),
            ever_spilled: AtomicBool::new(false),
            spill_build_done_count: AtomicUsize::new(1),
            spill_probe_done_count: AtomicUsize::new(1),
            task_stream_done_count: AtomicUsize::new(0),
            task_initialized: AtomicBool::new(false),
            next_task_index: AtomicUsize::new(usize::MAX),
            total_tasks: AtomicUsize::new(0),
            mutex: Mutex::new(CrossJoinSharedData {
                memory_build_groups: Vec::new(),
                spill_build_row_groups: vec![CrossSpillRowGroup {
                    path: "build".to_string(),
                    row_group: empty_row_group_meta(),
                }],
                spill_probe_row_groups: vec![CrossSpillRowGroup {
                    path: "probe".to_string(),
                    row_group: empty_row_group_meta(),
                }],
            }),
        });

        assert!(shared.try_init_spill_tasks());
        assert!(shared.task_initialized.load(Ordering::Acquire));
        assert_eq!(shared.next_task_index.load(Ordering::Acquire), 0);
        assert_eq!(shared.total_tasks.load(Ordering::Acquire), 1);
        assert!(shared.next_spill_task().is_some());
    }

    fn empty_row_group_meta() -> RowGroupMetaData {
        use std::sync::Arc;

        use parquet::basic::Repetition;
        use parquet::file::metadata::RowGroupMetaData;
        use parquet::schema::types::SchemaDescriptor;
        use parquet::schema::types::Type;

        let schema = Type::group_type_builder("schema")
            .with_repetition(Repetition::REPEATED)
            .build()
            .unwrap();
        let descriptor = SchemaDescriptor::new(Arc::new(schema));
        RowGroupMetaData::builder(Arc::new(descriptor))
            .build()
            .unwrap()
    }
}
