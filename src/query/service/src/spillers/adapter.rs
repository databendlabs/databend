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

use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::RwLock;

use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::traits::DataBlockSpill;
use databend_common_pipeline_transforms::traits::SortSpiller;
use databend_storages_common_cache::TempPath;
use opendal::Operator;

use super::Location;
use super::block_reader::BlocksReader;
use super::block_writer::BlocksWriter;
use super::inner::*;
use super::serialize::Layout;
use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSpillProgress;

#[derive(Clone)]
pub struct PartitionAdapter {
    ctx: Arc<QueryContext>,
    // Stores the spilled files that controlled by current spiller
    private_spilled_files: Arc<RwLock<HashSet<Location>>>,
    /// 1 partition -> N partition files
    partition_location: HashMap<usize, Vec<(Location, usize, usize)>>,
}

impl SpillAdapter for PartitionAdapter {
    fn add_spill_file(&self, location: Location, layout: Layout, size: usize) {
        self.private_spilled_files
            .write()
            .unwrap()
            .insert(location.clone());

        // Progress (SpillTotalStats) should reflect total spill volume,
        // including both local and remote spill files.
        self.ctx.as_ref().incr_spill_progress(1, size);
        self.ctx.as_ref().add_spill_file(location, layout);
    }

    fn get_spill_layout(&self, location: &Location) -> Option<Layout> {
        self.ctx.as_ref().get_spill_layout(location)
    }
}

pub type Spiller = SpillerInner<PartitionAdapter>;

impl Spiller {
    pub fn create(
        ctx: Arc<QueryContext>,
        operator: Operator,
        config: SpillerConfig,
    ) -> Result<Self> {
        Self::new(
            PartitionAdapter {
                ctx,
                private_spilled_files: Default::default(),
                partition_location: Default::default(),
            },
            operator,
            config,
        )
    }

    pub fn spilled_partitions(&self) -> HashSet<usize> {
        self.adapter.partition_location.keys().copied().collect()
    }

    pub fn partition_blocks_reader(&mut self, id: &usize) -> BlocksReader<'_> {
        let locations: &[_] = match self.adapter.partition_location.get(id) {
            None => &[],
            Some(locations) => locations,
        };

        BlocksReader::new(locations, self.operator.clone())
    }

    #[async_backtrace::framed]
    /// Read spilled data with partition id
    pub async fn read_spilled_partition(&mut self, partition_id: &usize) -> Result<Vec<DataBlock>> {
        if let Some(locs) = self.adapter.partition_location.get(partition_id) {
            let mut spilled_data = Vec::with_capacity(locs.len());
            for (loc, _data_size, _blocks_num) in locs.iter() {
                let block = self.read_spilled_file(loc).await?;

                if block.num_rows() != 0 {
                    spilled_data.push(block);
                }
            }
            Ok(spilled_data)
        } else {
            Ok(vec![])
        }
    }

    pub fn get_partition_locations(&self, partition_id: &usize) -> Option<Vec<Location>> {
        let locations = self.adapter.partition_location.get(partition_id)?;
        Some(locations.iter().map(|(loc, _, _)| loc.clone()).collect())
    }

    pub async fn block_stream_writer(&mut self) -> Result<BlocksWriter> {
        let location = self.create_unique_location();
        let writer = self.operator.writer_with(&location).await?;
        Ok(BlocksWriter::create(writer, Location::Remote(location)))
    }

    pub fn inc_progress(&self, progress_val: ProgressValues) {
        self.adapter
            .ctx
            .get_join_spill_progress()
            .incr(&progress_val);
    }

    pub fn add_hash_join_location(
        &mut self,
        partition: usize,
        location: Location,
        block_num: usize,
        data_size: usize,
    ) {
        self.adapter
            .add_spill_file(location.clone(), Layout::Parquet, data_size);

        self.adapter
            .partition_location
            .entry(partition)
            .or_default()
            .push((location, data_size, block_num));
    }

    #[async_backtrace::framed]
    /// Spill data block with partition
    pub async fn spill_with_partition(
        &mut self,
        partition_id: usize,
        data: Vec<DataBlock>,
    ) -> Result<()> {
        let (num_rows, memory_size) = data
            .iter()
            .map(|b| (b.num_rows(), b.memory_size()))
            .reduce(|acc, x| (acc.0 + x.0, acc.1 + x.1))
            .unwrap();

        let progress_val = ProgressValues {
            rows: num_rows,
            bytes: memory_size,
        };

        let location = self.spill(data).await?;

        self.adapter
            .partition_location
            .entry(partition_id)
            .or_default()
            .push((location, 0, 0));

        self.adapter
            .ctx
            .get_join_spill_progress()
            .incr(&progress_val);
        Ok(())
    }

    pub(crate) fn private_spilled_files(&self) -> Vec<Location> {
        self.adapter
            .private_spilled_files
            .read()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }
}

impl SpillAdapter for Arc<QueryContext> {
    fn add_spill_file(&self, location: Location, layout: Layout, size: usize) {
        // Count both local and remote spills in SpillTotalStats.
        self.incr_spill_progress(1, size);
        self.as_ref().add_spill_file(location, layout);
    }

    fn get_spill_layout(&self, location: &Location) -> Option<Layout> {
        self.as_ref().get_spill_layout(location)
    }
}

pub struct SortAdapter {
    ctx: Arc<QueryContext>,
    local_files: Arc<RwLock<HashMap<TempPath, Layout>>>,
    memory_settings: MemorySettings,
}

impl SpillAdapter for SortAdapter {
    fn add_spill_file(&self, location: Location, layout: Layout, size: usize) {
        match location {
            Location::Remote(_) => {
                // Remote spill files are tracked in QueryContext for cleanup
                // and contribute to the total spill progress.
                self.ctx.as_ref().incr_spill_progress(1, size);
                self.ctx.as_ref().add_spill_file(location, layout);
            }
            Location::Local(temp_path) => {
                // Local spill files are tracked only in-memory for sort, but
                // should still be counted in SpillTotalStats so that progress
                // reflects total (local + remote) spilled bytes/files.
                self.ctx.as_ref().incr_spill_progress(1, size);
                self.local_files.write().unwrap().insert(temp_path, layout);
            }
        }
    }

    fn get_spill_layout(&self, location: &Location) -> Option<Layout> {
        match location {
            Location::Remote(_) => self.ctx.as_ref().get_spill_layout(location),
            Location::Local(temp_path) => self.local_files.read().unwrap().get(temp_path).cloned(),
        }
    }
}

#[derive(Clone)]
pub struct SortSpillerImpl(Arc<SpillerInner<SortAdapter>>);

#[async_trait::async_trait]
impl SortSpiller for SortSpillerImpl {
    async fn spill(&self, data_block: DataBlock) -> Result<Location> {
        self.0.spill(vec![data_block]).await
    }

    async fn restore(&self, location: &Location) -> Result<DataBlock> {
        self.0.read_spilled_file(location).await
    }

    fn remove_local_file(&self, local: &TempPath) {
        SortSpillerImpl::remove_local_file(self, local);
    }

    fn memory_settings(&self) -> &MemorySettings {
        &self.0.adapter.memory_settings
    }
}

impl SortSpillerImpl {
    pub fn new(ctx: Arc<QueryContext>, operator: Operator, config: SpillerConfig) -> Result<Self> {
        Ok(SortSpillerImpl(Arc::new(SpillerInner::new(
            SortAdapter {
                memory_settings: MemorySettings::from_sort_settings(&ctx)?,
                ctx,
                local_files: Default::default(),
            },
            operator,
            config,
        )?)))
    }

    pub fn remove_local_file(&self, local: &TempPath) -> Option<Layout> {
        self.0.adapter.local_files.write().unwrap().remove(local)
    }
}

#[derive(Clone, Default)]
pub struct LiteAdapter {
    files: Arc<RwLock<HashMap<Location, Layout>>>,
}

impl SpillAdapter for LiteAdapter {
    fn add_spill_file(&self, location: Location, layout: Layout, _: usize) {
        self.files.write().unwrap().insert(location, layout);
    }

    fn get_spill_layout(&self, location: &Location) -> Option<Layout> {
        self.files.read().unwrap().get(location).cloned()
    }
}

#[derive(Clone)]
pub struct LiteSpiller(Arc<SpillerInner<LiteAdapter>>);

impl LiteSpiller {
    pub fn new(operator: Operator, config: SpillerConfig) -> Result<LiteSpiller> {
        Ok(LiteSpiller(Arc::new(SpillerInner::new(
            Default::default(),
            operator,
            config,
        )?)))
    }

    pub async fn cleanup(self) -> Result<()> {
        let files = std::mem::take(self.0.adapter.files.write().unwrap().deref_mut());
        let files: Vec<_> = files
            .into_keys()
            .filter_map(|location| match location {
                Location::Remote(path) => Some(path),
                Location::Local(_) => None,
            })
            .collect();
        self.0.operator.delete_iter(files).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl DataBlockSpill for LiteSpiller {
    async fn merge_and_spill(&self, data_block: Vec<DataBlock>) -> Result<Location> {
        self.0.spill(data_block).await
    }

    async fn restore(&self, location: &Location) -> Result<DataBlock> {
        self.0.read_spilled_file(location).await
    }
}
