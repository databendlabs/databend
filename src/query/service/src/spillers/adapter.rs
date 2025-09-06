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
use std::ops::Range;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Instant;

use databend_common_base::base::dma_buffer_to_bytes;
use databend_common_base::base::dma_read_file_range;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::traits::DataBlockSpill;
use databend_storages_common_cache::TempPath;
use opendal::Buffer;
use opendal::Operator;

use super::inner::*;
use super::serialize::*;
use super::Location;
use crate::sessions::QueryContext;

pub struct PartitionAdapter {
    ctx: Arc<QueryContext>,
    // Stores the spilled files that controlled by current spiller
    private_spilled_files: Arc<RwLock<HashMap<Location, Layout>>>,
    /// 1 partition -> N partition files
    partition_location: HashMap<usize, Vec<Location>>,
}

impl SpillAdapter for PartitionAdapter {
    fn add_spill_file(&self, location: Location, layout: Layout, size: usize) {
        self.private_spilled_files
            .write()
            .unwrap()
            .insert(location.clone(), layout.clone());
        self.ctx.as_ref().add_spill_file(location, layout, size);
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

    #[async_backtrace::framed]
    /// Read spilled data with partition id
    pub async fn read_spilled_partition(&mut self, p_id: &usize) -> Result<Vec<DataBlock>> {
        if let Some(locs) = self.adapter.partition_location.get(p_id) {
            let mut spilled_data = Vec::with_capacity(locs.len());
            for loc in locs.iter() {
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

    pub fn get_partition_locations(&self, partition_id: &usize) -> Option<&Vec<Location>> {
        self.adapter.partition_location.get(partition_id)
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
            .and_modify(|locs| {
                locs.push(location.clone());
            })
            .or_insert(vec![location.clone()]);

        self.adapter
            .ctx
            .get_join_spill_progress()
            .incr(&progress_val);
        Ok(())
    }

    pub async fn spill_with_merged_partitions(
        &mut self,
        partitioned_data: Vec<(usize, Vec<DataBlock>)>,
    ) -> Result<MergedPartition> {
        // Serialize data block.
        let mut encoder = self.block_encoder();
        let mut partition_ids = Vec::new();
        for (partition_id, data_blocks) in partitioned_data.into_iter() {
            partition_ids.push(partition_id);
            encoder.add_blocks(data_blocks);
        }

        let write_bytes = encoder.size();
        let BlocksEncoder {
            buf,
            offsets,
            columns_layout,
            ..
        } = encoder;

        let layout = columns_layout.last().unwrap().clone();
        let partitions = partition_ids
            .into_iter()
            .zip(
                offsets
                    .windows(2)
                    .map(|x| x[0]..x[1])
                    .zip(columns_layout.into_iter()),
            )
            .map(|(id, (range, layout))| (id, Chunk { range, layout }))
            .collect();

        // Spill data to storage.
        let instant = Instant::now();
        let location = self.write_encodes(write_bytes, buf).await?;
        // Record statistics.
        record_write_profile(&location, &instant, write_bytes);

        self.adapter
            .add_spill_file(location.clone(), layout, write_bytes);
        Ok(MergedPartition {
            location,
            partitions,
        })
    }

    pub async fn read_merged_partitions(
        &self,
        MergedPartition {
            location,
            partitions,
        }: &MergedPartition,
    ) -> Result<Vec<(usize, DataBlock)>> {
        // Read spilled data from storage.
        let instant = Instant::now();

        let data = match (location, &self.local_operator) {
            (Location::Local(path), None) => {
                let file_size = path.size();
                debug_assert_eq!(
                    file_size,
                    if let Some((_, Chunk { range, .. })) = partitions.last() {
                        range.end
                    } else {
                        0
                    }
                );

                let (buf, range) = dma_read_file_range(path, 0..file_size as u64).await?;
                Buffer::from(dma_buffer_to_bytes(buf)).slice(range)
            }
            (Location::Local(path), Some(ref local)) => {
                local
                    .read(path.file_name().unwrap().to_str().unwrap())
                    .await?
            }
            (Location::Remote(loc), _) => self.operator.read(loc).await?,
        };

        // Record statistics.
        record_read_profile(location, &instant, data.len());

        // Deserialize partitioned data block.
        let mut partitioned_data = Vec::with_capacity(partitions.len());
        for (partition_id, Chunk { range, layout }) in partitions {
            let block = deserialize_block(layout, data.slice(range.clone()))?;
            partitioned_data.push((*partition_id, block));
        }

        Ok(partitioned_data)
    }

    pub async fn read_chunk(&self, location: &Location, chunk: &Chunk) -> Result<DataBlock> {
        // Read spilled data from storage.
        let instant = Instant::now();
        let Chunk { range, layout } = chunk;
        let data_range = range.start as u64..range.end as u64;

        let data = match location {
            Location::Local(path) => match &self.local_operator {
                Some(ref local) => {
                    local
                        .read_with(path.file_name().unwrap().to_str().unwrap())
                        .range(data_range)
                        .await?
                }
                None => {
                    let (buf, range) = dma_read_file_range(path, data_range).await?;
                    Buffer::from(dma_buffer_to_bytes(buf)).slice(range)
                }
            },
            Location::Remote(loc) => self.operator.read_with(loc).range(data_range).await?,
        };

        record_read_profile(location, &instant, data.len());

        deserialize_block(layout, data)
    }

    pub async fn spill_stream_aggregate_buffer(
        &self,
        location: Option<String>,
        write_data: Vec<Vec<Vec<u8>>>,
    ) -> Result<(String, usize)> {
        let mut write_bytes = 0;
        let location = location.unwrap_or_else(|| self.create_unique_location());

        let mut writer = self
            .operator
            .writer_with(&location)
            .chunk(8 * 1024 * 1024)
            .await?;
        for write_bucket_data in write_data.into_iter() {
            for data in write_bucket_data.into_iter() {
                write_bytes += data.len();
                writer.write(data).await?;
            }
        }

        writer.close().await?;
        self.adapter.add_spill_file(
            Location::Remote(location.clone()),
            Layout::Aggregate,
            write_bytes,
        );
        Ok((location, write_bytes))
    }

    pub(crate) fn private_spilled_files(&self) -> Vec<Location> {
        self.adapter
            .private_spilled_files
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }
}

pub struct MergedPartition {
    pub location: Location,
    pub partitions: Vec<(usize, Chunk)>,
}

pub struct Chunk {
    pub range: Range<usize>,
    pub layout: Layout,
}

impl SpillAdapter for Arc<QueryContext> {
    fn add_spill_file(&self, location: Location, layout: Layout, size: usize) {
        self.as_ref().add_spill_file(location, layout, size);
    }

    fn get_spill_layout(&self, location: &Location) -> Option<Layout> {
        self.as_ref().get_spill_layout(location)
    }
}

pub struct SortAdapter {
    ctx: Arc<QueryContext>,
    local_files: Arc<RwLock<HashMap<TempPath, Layout>>>,
}

impl SpillAdapter for SortAdapter {
    fn add_spill_file(&self, location: Location, layout: Layout, size: usize) {
        match location {
            Location::Remote(_) => self.ctx.as_ref().add_spill_file(location, layout, size),
            Location::Local(temp_path) => {
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
pub struct SortSpiller(Arc<SpillerInner<SortAdapter>>);

#[async_trait::async_trait]
impl DataBlockSpill for SortSpiller {
    async fn merge_and_spill(&self, data_block: Vec<DataBlock>) -> Result<Location> {
        self.0.spill(data_block).await
    }

    async fn restore(&self, location: &Location) -> Result<DataBlock> {
        self.0.read_spilled_file(location).await
    }
}

impl SortSpiller {
    pub fn new(ctx: Arc<QueryContext>, operator: Operator, config: SpillerConfig) -> Result<Self> {
        Ok(SortSpiller(Arc::new(SpillerInner::new(
            SortAdapter {
                ctx,
                local_files: Default::default(),
            },
            operator,
            config,
        )?)))
    }

    // todo: We need to drop [TempPath] earlier
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

pub type LiteSpiller = Arc<SpillerInner<LiteAdapter>>;

pub fn new_lite_spiller(operator: Operator, config: SpillerConfig) -> Result<LiteSpiller> {
    Ok(Arc::new(SpillerInner::new(
        Default::default(),
        operator,
        config,
    )?))
}
