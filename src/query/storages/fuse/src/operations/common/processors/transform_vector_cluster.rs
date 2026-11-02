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

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VectorColumn;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_common_pipeline_transforms::BlockCompactMeta;
use databend_common_pipeline_transforms::Transform;
use databend_storages_common_index::KMeans;
use databend_storages_common_index::KMeansInit;
use databend_storages_common_index::KMeansModel;
use databend_storages_common_table_meta::meta::VectorColumnStatistics;
use databend_storages_common_table_meta::meta::VectorDistanceType;
use databend_storages_common_table_meta::table::VECTOR_REFINE_PAGE_ROWS;

// Kmeans is computed per bounded batch so the transform can stream instead of
// buffering the whole INSERT/RECLUSTER input.
/// Upper bound of a batch in blocks, and of the cluster count (about one cluster per
/// block) of a batch or a recluster task. This bounds the training cost, which grows
/// with the square of the cluster count.
const KMEANS_MAX_BATCH_BLOCKS: usize = 64;
/// A cluster takes at most this many times the average number of rows, so that a
/// cluster is about one block (knowhere's balanced assignment uses 2).
const KMEANS_BALANCE_FACTOR: usize = 2;
/// Share (in percent) of the free memory used by the batches of all lanes.
const KMEANS_MEMORY_BUDGET_PERCENT: usize = 10;
// Sort key: cluster rank | page leaf | distance to the cluster centroid. Rows are
// first sorted by cluster (the leaf is 0), cut into blocks, and then ordered within
// each block by page leaf (`order_block_pages`). The low distance bits are quantized
// because they only need to provide a stable order within a leaf.
const VECTOR_CLUSTER_ID_BITS: u32 = 7;
const VECTOR_CLUSTER_LEAF_BITS: u32 = 12;
const VECTOR_CLUSTER_SHIFT: u32 = 64 - VECTOR_CLUSTER_ID_BITS;
const VECTOR_CLUSTER_DISTANCE_BITS: u32 = VECTOR_CLUSTER_SHIFT - VECTOR_CLUSTER_LEAF_BITS;
const VECTOR_CLUSTER_DISTANCE_MASK: u64 = (1_u64 << VECTOR_CLUSTER_DISTANCE_BITS) - 1;
const VECTOR_CLUSTER_DISTANCE_SCALE: f64 = 1_000_000.0;
/// Leaves of one cluster in a block; more rows make leaves larger than a page.
const MAX_PAGE_LEAVES: usize = 1 << VECTOR_CLUSTER_LEAF_BITS;
/// Cluster ids stay below the all-ones id, so that `INVALID_SORT_KEY` sorts last.
const MAX_CLUSTERS: usize = (1 << VECTOR_CLUSTER_ID_BITS) - 1;
/// Sort key of NULL and unclusterable vectors, after all clusters.
const INVALID_SORT_KEY: u64 = u64::MAX;

/// Where the k-means model of a batch comes from.
enum ClusterModel {
    /// Each batch trains its own model. Cluster ids are batch-local: the batch is
    /// emitted as one block, which is sorted on its own (INSERT).
    PerBatch,
    /// One model for all rows of a recluster task, which are sorted together. It is
    /// trained on the first batch with vectors, seeded by the centroids of the
    /// task's input blocks so that it also covers the rows of later batches.
    Task {
        clusters: usize,
        total_rows: usize,
        seed_centroids: Vec<f32>,
        seed_weights: Vec<u64>,
        assigner: Option<ClusterAssigner>,
    },
}

/// A trained model with the state of its balanced assignment.
struct ClusterAssigner {
    model: KMeansModel,
    /// Sort rank of each cluster; consecutive ranks are close clusters.
    ranks: Vec<usize>,
    counts: Vec<u64>,
    capacity: u64,
}

impl ClusterAssigner {
    /// `rows` is the number of rows that will be assigned with this model.
    fn new(model: KMeansModel, rows: usize) -> Self {
        let k = model.k();
        Self {
            ranks: model.neighbor_ranks(),
            counts: vec![0; k],
            capacity: rows.div_ceil(k).saturating_mul(KMEANS_BALANCE_FACTOR) as u64,
            model,
        }
    }

    /// Sort key of a prepared point: the rank of its (balanced) cluster, then its
    /// distance to the centroid.
    fn key(&mut self, point: &[f32]) -> u64 {
        let (cluster, distance) =
            self.model
                .assign_balanced(point, &mut self.counts, self.capacity);
        pack_vector_cluster_sort_key(self.ranks[cluster], distance)
    }
}

/// Rows of the blocks that will be written.
#[derive(Clone, Copy)]
enum BlockRows {
    /// Computed by the caller from the input size (RECLUSTER).
    Exact(usize),
    /// Estimated from the row size of the input (INSERT): wide vectors fill blocks
    /// by bytes long before `max_rows_per_block`.
    Estimate(BlockThresholds),
}

/// Batch sizes, determined from the row size of the first input block.
#[derive(Clone, Copy)]
struct BatchLayout {
    rows_per_block: usize,
    batch_rows: usize,
}

pub struct TransformVectorCluster {
    vector_column_input_offset: usize,
    dimension: usize,
    block_rows: BlockRows,
    /// Memory budget of this lane's batches.
    budget_bytes: usize,
    layout: Option<BatchLayout>,
    /// Order the rows of each INSERT block by FP16 page (`order_block_pages`).
    page_order: bool,
    kmeans: KMeans,
    model: ClusterModel,
    pending_blocks: Vec<DataBlock>,
    pending_rows: usize,
}

impl TransformVectorCluster {
    /// Clusters each batch on its own, for INSERT. `budget_bytes` comes from
    /// `vector_cluster_memory_budget`.
    pub fn new(
        vector_column_input_offset: usize,
        dimension: usize,
        distance_type: VectorDistanceType,
        block_thresholds: BlockThresholds,
        budget_bytes: usize,
    ) -> Self {
        Self::create(
            vector_column_input_offset,
            dimension,
            distance_type,
            BlockRows::Estimate(block_thresholds),
            budget_bytes,
            ClusterModel::PerBatch,
        )
    }

    /// Clusters all `total_rows` rows of a recluster task with one model, seeded by
    /// the vector statistics of the task's input blocks.
    pub fn for_recluster(
        vector_column_input_offset: usize,
        dimension: usize,
        distance_type: VectorDistanceType,
        rows_per_block: usize,
        total_rows: usize,
        seeds: &[VectorColumnStatistics],
        budget_bytes: usize,
    ) -> Self {
        let rows_per_block = rows_per_block.max(1);
        let mut seed_centroids = Vec::with_capacity(seeds.len() * dimension);
        let mut seed_weights = Vec::with_capacity(seeds.len());
        for seed in seeds.iter().filter(|seed| seed.centroid.len() == dimension) {
            seed_centroids.extend(seed.centroid.iter().map(|value| value.0));
            seed_weights.push(seed.row_count);
        }
        let model = ClusterModel::Task {
            clusters: cluster_count(total_rows, rows_per_block),
            total_rows,
            seed_centroids,
            seed_weights,
            assigner: None,
        };
        Self::create(
            vector_column_input_offset,
            dimension,
            distance_type,
            BlockRows::Exact(rows_per_block),
            budget_bytes,
            model,
        )
    }

    fn create(
        vector_column_input_offset: usize,
        dimension: usize,
        distance_type: VectorDistanceType,
        block_rows: BlockRows,
        budget_bytes: usize,
        model: ClusterModel,
    ) -> Self {
        Self {
            vector_column_input_offset,
            dimension,
            block_rows,
            budget_bytes,
            layout: None,
            page_order: false,
            kmeans: KMeans::new(dimension, distance_type),
            model,
            pending_blocks: vec![],
            pending_rows: 0,
        }
    }

    /// Orders the rows of each INSERT block by FP16 page; only useful when the vector
    /// index stores FP16 refine data, so it is off by default.
    pub fn with_page_order(mut self, page_order: bool) -> Self {
        self.page_order = page_order;
        self
    }

    /// The batch layout, determined from the row size of the first nonempty block.
    fn layout(&mut self, block: &DataBlock) -> BatchLayout {
        let (block_rows, budget_bytes) = (self.block_rows, self.budget_bytes);
        *self.layout.get_or_insert_with(|| {
            let row_bytes = block.memory_size().div_ceil(block.num_rows().max(1));
            let rows_per_block = match block_rows {
                BlockRows::Exact(rows) => rows,
                BlockRows::Estimate(thresholds) => thresholds.estimate_rows_per_block(row_bytes),
            };
            BatchLayout {
                rows_per_block,
                batch_rows: kmeans_batch_rows(rows_per_block, row_bytes, budget_bytes),
            }
        })
    }

    fn cluster_blocks(&mut self) -> Result<Vec<DataBlock>> {
        if self.pending_blocks.is_empty() {
            return Ok(vec![]);
        }

        let mut block = if self.pending_blocks.len() == 1 {
            self.pending_rows = 0;
            self.pending_blocks.pop().unwrap()
        } else {
            let blocks = std::mem::take(&mut self.pending_blocks);
            self.pending_rows = 0;
            DataBlock::concat(&blocks)?
        };
        let keys = self.sort_keys(&block)?;
        if !matches!(self.model, ClusterModel::PerBatch) {
            // RECLUSTER sorts the whole task by these keys afterwards, which also cuts
            // the blocks, so the batch is passed on as is.
            block.add_column(UInt64Type::from_data(keys));
            return Ok(vec![block]);
        }
        // INSERT writes each output block as one file (the following sort is per
        // block), so sort the batch by cluster here and cut it into blocks along
        // cluster boundaries; otherwise a batch of up to KMEANS_MAX_BATCH_BLOCKS
        // blocks would be written as a single block.
        let rows_per_block = self.layout(&block).rows_per_block;
        let mut order: Vec<u32> = (0..block.num_rows() as u32).collect();
        order.sort_by_key(|&row| keys[row as usize]);
        let sorted_keys: Vec<u64> = order.iter().map(|&row| keys[row as usize]).collect();
        let mut cutter = ClusterBlockCutter::new(rows_per_block);
        cutter.push_keys(&sorted_keys);
        cutter.finish();
        let mut block = block.take(order.as_slice())?;
        block.add_column(UInt64Type::from_data(sorted_keys));
        let key_offset = block.num_columns() - 1;
        let mut start = 0;
        let mut blocks = Vec::new();
        for rows in cutter.take_cuts() {
            let cut = block.slice(start..start + rows);
            // INSERT lanes run in parallel; the following per-block sort keeps the
            // page order, which the key encodes.
            blocks.push(if self.page_order {
                order_block_pages(
                    &self.kmeans,
                    cut,
                    self.vector_column_input_offset,
                    key_offset,
                    VECTOR_REFINE_PAGE_ROWS,
                )?
            } else {
                cut
            });
            start += rows;
        }
        debug_assert_eq!(start, block.num_rows());
        Ok(blocks)
    }

    /// Sort keys of a batch: cluster id and distance to its centroid, with NULL and
    /// unclusterable vectors last.
    fn sort_keys(&mut self, block: &DataBlock) -> Result<Vec<u64>> {
        let column = block
            .get_by_offset(self.vector_column_input_offset)
            .to_column();
        let validity = column.as_nullable().map(|column| column.validity.clone());
        let Column::Vector(VectorColumn::Float32((values, dimension))) = column.remove_nullable()
        else {
            return Err(ErrorCode::InvalidClusterKeys(
                "Vector cluster key only supports float32 vector column",
            ));
        };
        let num_rows = block.num_rows();
        if dimension != self.dimension || values.len() != num_rows * dimension {
            return Err(ErrorCode::InvalidClusterKeys(
                "Vector cluster key has invalid vector dimension",
            ));
        }

        let kmeans = self.kmeans;
        // Writes the prepared vector of `row`; false for NULL or unclusterable rows.
        let prepare = |row: usize, out: &mut [f32]| {
            if validity
                .as_ref()
                .is_some_and(|validity| !validity.get_bit(row))
            {
                return false;
            }
            let vector = &values[row * dimension..(row + 1) * dimension];
            for (out, value) in out.iter_mut().zip(vector) {
                *out = value.0;
            }
            kmeans.prepare(out)
        };
        let mut point = vec![0.0; dimension];
        let valid: Vec<usize> = (0..num_rows)
            .filter(|&row| prepare(row, &mut point))
            .collect();
        let training_point = |i: usize, out: &mut [f32]| {
            prepare(valid[i], out);
        };

        let rows_per_block = self.layout(block).rows_per_block;
        let mut batch_assigner;
        let assigner = match &mut self.model {
            ClusterModel::PerBatch => {
                batch_assigner = match valid.len() {
                    0 => None,
                    n => {
                        let k = cluster_count(n, rows_per_block);
                        let model = kmeans.train(n, k, KMeansInit::Random, training_point)?;
                        Some(ClusterAssigner::new(model, n))
                    }
                };
                batch_assigner.as_mut()
            }
            ClusterModel::Task {
                clusters,
                total_rows,
                seed_centroids,
                seed_weights,
                assigner,
            } => {
                if assigner.is_none() && !valid.is_empty() {
                    let init = KMeansInit::Seeds {
                        centroids: seed_centroids,
                        weights: seed_weights,
                    };
                    let model = kmeans.train(valid.len(), *clusters, init, training_point)?;
                    // The balance covers all rows of the task, across batches.
                    *assigner = Some(ClusterAssigner::new(model, *total_rows));
                }
                assigner.as_mut()
            }
        };

        let mut keys = vec![INVALID_SORT_KEY; num_rows];
        if let Some(assigner) = assigner {
            for &row in &valid {
                prepare(row, &mut point);
                keys[row] = assigner.key(&point);
            }
        }
        Ok(keys)
    }

    fn push_block(&mut self, data: DataBlock, output: &mut Vec<DataBlock>) -> Result<()> {
        let rows = data.num_rows();
        if rows == 0 {
            let mut data = data;
            data.add_column(UInt64Type::from_data(Vec::<u64>::new()));
            output.push(data);
            return Ok(());
        }

        let batch_rows = self.layout(&data).batch_rows;
        if rows > batch_rows {
            output.extend(self.cluster_blocks()?);
            let (blocks, remain) = data.split_by_rows(batch_rows);
            for block in blocks {
                self.pending_rows = block.num_rows();
                self.pending_blocks.push(block);
                output.extend(self.cluster_blocks()?);
            }
            if let Some(remain) = remain {
                self.pending_rows = remain.num_rows();
                self.pending_blocks.push(remain);
            }
            return Ok(());
        }

        if self.pending_rows > 0 && self.pending_rows.saturating_add(rows) > batch_rows {
            output.extend(self.cluster_blocks()?);
        }

        self.pending_rows += rows;
        self.pending_blocks.push(data);
        if self.pending_rows >= batch_rows {
            output.extend(self.cluster_blocks()?);
        }

        Ok(())
    }
}

impl AccumulatingTransform for TransformVectorCluster {
    const NAME: &'static str = "TransformVectorCluster";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let mut output = Vec::new();
        self.push_block(data, &mut output)?;
        Ok(output)
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if output {
            self.cluster_blocks()
        } else {
            self.pending_blocks.clear();
            self.pending_rows = 0;
            Ok(vec![])
        }
    }
}

/// Memory budget of the batches of one of `lanes` vector clustering transforms:
/// `KMEANS_MEMORY_BUDGET_PERCENT` of the memory left under `max_memory_usage`,
/// taking the larger of the node-global and query usages as recluster does.
/// Unlimited when `max_memory_usage` is 0.
pub fn vector_cluster_memory_budget(ctx: &dyn TableContext, lanes: usize) -> Result<usize> {
    let max_memory_usage = ctx.get_settings().get_max_memory_usage()? as usize;
    if max_memory_usage == 0 {
        return Ok(usize::MAX);
    }
    let memory_usage = GLOBAL_MEM_STAT
        .get_memory_usage()
        .max(ctx.get_nodes_memory_usage());
    let free = max_memory_usage.saturating_sub(memory_usage);
    Ok(free / 100 * KMEANS_MEMORY_BUDGET_PERCENT / lanes.max(1))
}

/// Rows of a batch: as many as fit `budget_bytes`, up to `KMEANS_MAX_BATCH_BLOCKS`
/// blocks. Under memory pressure a batch may be smaller than a block (at least one
/// row), which clusters fewer rows together but keeps within the budget.
fn kmeans_batch_rows(rows_per_block: usize, row_bytes: usize, budget_bytes: usize) -> usize {
    // Concatenating a batch briefly holds it twice.
    let budget_rows = budget_bytes / 2 / row_bytes.max(1);
    budget_rows.clamp(
        1,
        rows_per_block
            .max(1)
            .saturating_mul(KMEANS_MAX_BATCH_BLOCKS),
    )
}

/// About one cluster per block, at most `KMEANS_MAX_BATCH_BLOCKS`.
fn cluster_count(rows: usize, rows_per_block: usize) -> usize {
    const _: () = assert!(KMEANS_MAX_BATCH_BLOCKS <= MAX_CLUSTERS);
    rows.div_ceil(rows_per_block.max(1))
        .clamp(1, KMEANS_MAX_BATCH_BLOCKS)
}

fn pack_vector_cluster_sort_key(cluster_id: usize, distance_to_centroid: f32) -> u64 {
    debug_assert!(cluster_id < MAX_CLUSTERS);
    let cluster_part = (cluster_id as u64) << VECTOR_CLUSTER_SHIFT;
    let distance = if distance_to_centroid.is_finite() {
        distance_to_centroid.max(0.0) as f64
    } else {
        f64::INFINITY
    };
    let distance_part = (distance * VECTOR_CLUSTER_DISTANCE_SCALE)
        .min(VECTOR_CLUSTER_DISTANCE_MASK as f64)
        .max(0.0) as u64;
    cluster_part | distance_part
}

/// Orders the rows of a block, sorted by vector cluster sort key, so that the rows of
/// each page are close: the rows of each cluster are split into page leaves by
/// `KMeans::page_order`, and their keys get the leaf. Leaves start at the first row
/// of the cluster, so they align with the pages of a block holding one cluster.
/// Rows that cannot be clustered (`INVALID_SORT_KEY`, last) keep their order.
fn order_block_pages(
    kmeans: &KMeans,
    block: DataBlock,
    vector_offset: usize,
    key_offset: usize,
    page_rows: usize,
) -> Result<DataBlock> {
    let num_rows = block.num_rows();
    if num_rows == 0 {
        return Ok(block);
    }
    let key_column = block.get_by_offset(key_offset).to_column();
    let mut keys = UInt64Type::try_downcast_column(&key_column)
        .map_err(|_| ErrorCode::Internal("Vector cluster sort key is not a UInt64 column"))?
        .to_vec();
    let column = block
        .get_by_offset(vector_offset)
        .to_column()
        .remove_nullable();
    let Column::Vector(VectorColumn::Float32((values, dimension))) = column else {
        return Err(ErrorCode::InvalidClusterKeys(
            "Vector cluster key only supports float32 vector column",
        ));
    };
    if values.len() != num_rows * dimension {
        return Err(ErrorCode::InvalidClusterKeys(
            "Vector cluster key has invalid vector dimension",
        ));
    }

    let mut order: Vec<u32> = (0..num_rows as u32).collect();
    let mut start = 0;
    while start < num_rows && keys[start] != INVALID_SORT_KEY {
        let cluster = keys[start] >> VECTOR_CLUSTER_SHIFT;
        let mut end = start + 1;
        while end < num_rows
            && keys[end] != INVALID_SORT_KEY
            && keys[end] >> VECTOR_CLUSTER_SHIFT == cluster
        {
            end += 1;
        }
        let rows = end - start;
        let leaf_rows = page_rows.max(rows.div_ceil(MAX_PAGE_LEAVES)).max(1);
        if rows > leaf_rows {
            // Only valid rows have a cluster key, so they all prepare.
            let leaves = kmeans.page_order(rows, leaf_rows, |i, out| {
                let row = start + i;
                for (out, value) in out
                    .iter_mut()
                    .zip(&values[row * dimension..(row + 1) * dimension])
                {
                    *out = value.0;
                }
                kmeans.prepare(out);
            })?;
            let run_keys = keys[start..end].to_vec();
            for (pos, &i) in leaves.iter().enumerate() {
                let key = run_keys[i];
                let leaf = (pos / leaf_rows) as u64;
                order[start + pos] = (start + i) as u32;
                keys[start + pos] = (key >> VECTOR_CLUSTER_SHIFT << VECTOR_CLUSTER_SHIFT)
                    | (leaf << VECTOR_CLUSTER_DISTANCE_BITS)
                    | (key & VECTOR_CLUSTER_DISTANCE_MASK);
            }
        }
        start = end;
    }

    let block = block.take(order.as_slice())?;
    let mut entries = block.take_columns();
    entries[key_offset] = UInt64Type::from_data(keys).into();
    Ok(DataBlock::new(entries, num_rows))
}

/// Orders the rows of each block written by a vector recluster task by page leaf
/// (see `order_block_pages`). Runs after the blocks are cut, in parallel lanes.
pub struct TransformVectorPageOrder {
    kmeans: KMeans,
    vector_offset: usize,
    key_offset: usize,
}

impl TransformVectorPageOrder {
    /// `vector_offset` and `key_offset` are the offsets of the vector column and of
    /// the vector cluster sort key column.
    pub fn new(
        vector_offset: usize,
        key_offset: usize,
        dimension: usize,
        distance_type: VectorDistanceType,
    ) -> Self {
        Self {
            kmeans: KMeans::new(dimension, distance_type),
            vector_offset,
            key_offset,
        }
    }
}

impl Transform for TransformVectorPageOrder {
    const NAME: &'static str = "TransformVectorPageOrder";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        order_block_pages(
            &self.kmeans,
            data,
            self.vector_offset,
            self.key_offset,
            VECTOR_REFINE_PAGE_ROWS,
        )
    }
}

/// Cuts rows sorted by cluster sort key into blocks along cluster boundaries. A
/// cluster of at least `min_rows` rows is cut into `ceil(rows / rows_per_block)`
/// blocks of equal size; consecutive smaller clusters, which have close ranks, are
/// packed into blocks of up to `rows_per_block` rows. Cuts are produced in order and
/// sum to the pushed rows.
struct ClusterBlockCutter {
    rows_per_block: usize,
    min_rows: usize,
    /// Rows of packed small clusters, which precede the current cluster.
    packed: usize,
    cluster: Option<u64>,
    /// Rows of the current cluster not cut yet.
    run: usize,
    cuts: Vec<usize>,
}

impl ClusterBlockCutter {
    fn new(rows_per_block: usize) -> Self {
        let rows_per_block = rows_per_block.max(1);
        Self {
            rows_per_block,
            min_rows: BlockThresholds::min_block_threshold(rows_per_block).max(1),
            packed: 0,
            cluster: None,
            run: 0,
            cuts: Vec::new(),
        }
    }

    /// Pushes sort keys in sorted order.
    fn push_keys(&mut self, keys: &[u64]) {
        let mut start = 0;
        while start < keys.len() {
            let cluster = keys[start] >> VECTOR_CLUSTER_SHIFT;
            let mut end = start + 1;
            while end < keys.len() && keys[end] >> VECTOR_CLUSTER_SHIFT == cluster {
                end += 1;
            }
            self.push(cluster, end - start);
            start = end;
        }
    }

    fn push(&mut self, cluster: u64, rows: usize) {
        if self.cluster != Some(cluster) {
            self.end_cluster();
            self.cluster = Some(cluster);
        }
        self.run += rows;
        // Bounds the rows kept of a large cluster; the rest stays above one block.
        while self.run > 2 * self.rows_per_block {
            self.flush_packed();
            self.cuts.push(self.rows_per_block);
            self.run -= self.rows_per_block;
        }
    }

    fn end_cluster(&mut self) {
        let run = std::mem::take(&mut self.run);
        if run == 0 {
            return;
        }
        if run >= self.min_rows {
            self.flush_packed();
            let blocks = run.div_ceil(self.rows_per_block);
            for i in 0..blocks {
                self.cuts.push(run * (i + 1) / blocks - run * i / blocks);
            }
        } else {
            if self.packed + run > self.rows_per_block {
                self.flush_packed();
            }
            self.packed += run;
            if self.packed >= self.min_rows {
                self.flush_packed();
            }
        }
    }

    fn flush_packed(&mut self) {
        if self.packed > 0 {
            self.cuts.push(std::mem::take(&mut self.packed));
        }
    }

    fn finish(&mut self) {
        self.end_cluster();
        self.flush_packed();
        self.cluster = None;
    }

    fn take_cuts(&mut self) -> Vec<usize> {
        std::mem::take(&mut self.cuts)
    }
}

/// Cuts the globally sorted rows of a vector recluster task into blocks along cluster
/// boundaries (see `ClusterBlockCutter`), in place of the ordered compaction, which
/// cuts by size only and so mixes clusters in a block.
pub struct TransformVectorClusterBlocks {
    key_offset: usize,
    cutter: ClusterBlockCutter,
    buffered: VecDeque<DataBlock>,
}

impl TransformVectorClusterBlocks {
    /// `key_offset` is the offset of the vector cluster sort key column.
    pub fn new(rows_per_block: usize, key_offset: usize) -> Self {
        Self {
            key_offset,
            cutter: ClusterBlockCutter::new(rows_per_block),
            buffered: VecDeque::new(),
        }
    }

    /// Takes the cut blocks from the buffered rows, for `TransformCompactBlock`.
    fn take_blocks(&mut self) -> Vec<DataBlock> {
        let mut output = Vec::new();
        for mut rows in self.cutter.take_cuts() {
            let mut blocks = Vec::new();
            while rows > 0 {
                let Some(front) = self.buffered.pop_front() else {
                    break;
                };
                if front.num_rows() <= rows {
                    rows -= front.num_rows();
                    blocks.push(front);
                } else {
                    blocks.push(front.slice(0..rows));
                    self.buffered
                        .push_front(front.slice(rows..front.num_rows()));
                    rows = 0;
                }
            }
            debug_assert_eq!(rows, 0, "cut rows are buffered");
            let meta = if blocks.len() == 1 {
                BlockCompactMeta::NoChange(blocks)
            } else {
                BlockCompactMeta::Concat(blocks)
            };
            output.push(DataBlock::empty_with_meta(Box::new(meta)));
        }
        output
    }
}

impl AccumulatingTransform for TransformVectorClusterBlocks {
    const NAME: &'static str = "TransformVectorClusterBlocks";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        if data.num_rows() == 0 {
            return Ok(vec![]);
        }
        let column = data.get_by_offset(self.key_offset).to_column();
        let keys = UInt64Type::try_downcast_column(&column)
            .map_err(|_| ErrorCode::Internal("Vector cluster sort key is not a UInt64 column"))?;
        self.cutter.push_keys(&keys);
        self.buffered.push_back(data);
        Ok(self.take_blocks())
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if !output {
            self.buffered.clear();
            return Ok(vec![]);
        }
        self.cutter.finish();
        Ok(self.take_blocks())
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::BlockMetaInfoDowncast;
    use databend_common_expression::types::Bitmap;
    use databend_common_expression::types::F32;
    use databend_common_expression::types::NullableColumn;

    use super::*;

    fn vector_block(vectors: &[[f32; 2]], validity: Option<&[bool]>) -> DataBlock {
        let values: Vec<F32> = vectors.iter().flatten().map(|v| F32::from(*v)).collect();
        let column = Column::Vector(VectorColumn::Float32((values.into(), 2)));
        let column = match validity {
            Some(validity) => {
                NullableColumn::new_column(column, Bitmap::from_iter(validity.iter().copied()))
            }
            None => column,
        };
        DataBlock::new_from_columns(vec![column])
    }

    #[test]
    fn test_insert_batch_cut_into_blocks() -> Result<()> {
        // Three rows per block, one batch of 10 rows in two groups; page ordering
        // (FP16 refine data) on and off.
        let thresholds = BlockThresholds::new(3, 1 << 30, 1 << 30, 1000);
        for page_order in [false, true] {
            let mut transform =
                TransformVectorCluster::new(0, 2, VectorDistanceType::L2, thresholds, usize::MAX)
                    .with_page_order(page_order);
            let vectors: Vec<[f32; 2]> = (0..10)
                .map(|i| {
                    if i % 2 == 0 {
                        [0.0, i as f32 * 0.01]
                    } else {
                        [10.0, 10.0]
                    }
                })
                .collect();
            let mut output = transform.transform(vector_block(&vectors, None))?;
            output.extend(transform.on_finish(true)?);

            // Blocks of at most rows_per_block rows, sorted by key across the batch.
            assert!(output.iter().all(|block| block.num_rows() <= 3));
            assert_eq!(output.iter().map(DataBlock::num_rows).sum::<usize>(), 10);
            let keys: Vec<u64> = output
                .iter()
                .flat_map(|block| {
                    UInt64Type::try_downcast_column(block.get_last_column())
                        .unwrap()
                        .to_vec()
                })
                .collect();
            // Page ordering only reorders rows within a cluster of a block, so the
            // keys, which then carry the page leaf, stay sorted.
            assert!(keys.is_sorted());
        }
        Ok(())
    }

    #[test]
    fn test_order_block_pages() -> Result<()> {
        // One cluster of two interleaved groups, an id column, and a NULL row last.
        let vectors: Vec<[f32; 2]> = (0..7)
            .map(|i| {
                if i % 2 == 0 {
                    [0.0, i as f32 * 0.1]
                } else {
                    [100.0, i as f32 * 0.1]
                }
            })
            .collect();
        let keys: Vec<u64> = (0..6)
            .map(|i| pack_vector_cluster_sort_key(0, i as f32))
            .chain([INVALID_SORT_KEY])
            .collect();
        let mut validity = vec![true; 7];
        validity[6] = false;
        let mut block = vector_block(&vectors, Some(&validity));
        block.add_column(UInt64Type::from_data((0..7_u64).collect::<Vec<_>>()));
        block.add_column(UInt64Type::from_data(keys));
        let kmeans = KMeans::new(2, VectorDistanceType::L2);
        let block = order_block_pages(&kmeans, block, 0, 2, 3)?;

        let ids = UInt64Type::try_downcast_column(&block.get_by_offset(1).to_column())
            .unwrap()
            .to_vec();
        let keys = UInt64Type::try_downcast_column(block.get_last_column())
            .unwrap()
            .to_vec();
        // Each leaf of 3 rows (a page) holds one group; the NULL row stays last.
        assert!(ids[..3].iter().all(|id| id % 2 == ids[0] % 2));
        assert!(ids[3..6].iter().all(|id| id % 2 == ids[3] % 2));
        assert_ne!(ids[0] % 2, ids[3] % 2);
        assert_eq!((ids[6], keys[6]), (6, INVALID_SORT_KEY));
        // Keys keep the cluster and carry the leaf, so sorting keeps the order.
        let leaf = |key: u64| {
            (key >> VECTOR_CLUSTER_DISTANCE_BITS) & ((1 << VECTOR_CLUSTER_LEAF_BITS) - 1)
        };
        assert!(keys[..6].iter().all(|k| k >> VECTOR_CLUSTER_SHIFT == 0));
        assert_eq!(keys[..6].iter().map(|k| leaf(*k)).collect::<Vec<_>>(), [
            0, 0, 0, 1, 1, 1
        ]);
        Ok(())
    }

    #[test]
    fn test_cluster_block_cutter() {
        let cuts = |runs: &[(u64, usize)]| {
            let mut cutter = ClusterBlockCutter::new(10);
            for &(cluster, rows) in runs {
                cutter.push(cluster, rows);
            }
            cutter.finish();
            cutter.take_cuts()
        };
        // A cluster of at least min_rows (8) is cut into equal blocks at its boundary.
        assert_eq!(cuts(&[(0, 9), (1, 15)]), [9, 7, 8]);
        // Consecutive small clusters are packed up to rows_per_block; a cluster is
        // not split to fill a block.
        assert_eq!(cuts(&[(0, 3), (1, 4), (2, 5), (3, 9)]), [7, 5, 9]);
        // A large cluster pushed in pieces is cut as it streams, in equal blocks at
        // the end.
        assert_eq!(cuts(&[(0, 3), (1, 20), (1, 15)]), [3, 10, 10, 7, 8]);
    }

    #[test]
    fn test_recluster_blocks_follow_clusters() -> Result<()> {
        let key = |cluster: u64| cluster << VECTOR_CLUSTER_SHIFT;
        let block =
            |keys: &[u64]| DataBlock::new_from_columns(vec![UInt64Type::from_data(keys.to_vec())]);
        let mut transform = TransformVectorClusterBlocks::new(4, 0);
        // Input blocks do not follow cluster boundaries: clusters 0 (5 rows), 1 (4 rows).
        let mut output = transform.transform(block(&[key(0), key(0), key(0)]))?;
        output.extend(transform.transform(block(&[key(0), key(0), key(1), key(1)]))?);
        output.extend(transform.transform(block(&[key(1), key(1)]))?);
        output.extend(transform.on_finish(true)?);
        let rows: Vec<usize> = output
            .into_iter()
            .map(
                |mut block| match BlockCompactMeta::downcast_from(block.take_meta().unwrap()) {
                    Some(BlockCompactMeta::Concat(blocks) | BlockCompactMeta::NoChange(blocks)) => {
                        blocks.iter().map(DataBlock::num_rows).sum()
                    }
                    _ => unreachable!(),
                },
            )
            .collect();
        // Cluster 0 is cut into 2 + 3 rows, and cluster 1 is one block.
        assert_eq!(rows, [2, 3, 4]);
        Ok(())
    }

    #[test]
    fn test_vector_cluster_sort_keys() -> Result<()> {
        let thresholds = BlockThresholds::new(2, 1 << 30, 1 << 30, 1000);
        let mut transform =
            TransformVectorCluster::new(0, 2, VectorDistanceType::L2, thresholds, usize::MAX);
        let vectors = [[0.0, 0.0], [5.0, 5.0], [0.1, 0.0], [10.0, 10.0], [
            10.1, 10.0,
        ]];
        let validity = [true, false, true, true, true];
        let block = vector_block(&vectors, Some(&validity));
        let keys = transform.sort_keys(&block)?;
        let cluster = |key: u64| key >> VECTOR_CLUSTER_SHIFT;
        // NULL rows sort last; nearby vectors share a cluster.
        assert_eq!(keys[1], INVALID_SORT_KEY);
        assert_eq!(cluster(keys[0]), cluster(keys[2]));
        assert_eq!(cluster(keys[3]), cluster(keys[4]));
        assert_ne!(cluster(keys[0]), cluster(keys[3]));
        Ok(())
    }

    #[test]
    fn test_recluster_model_spans_batches() -> Result<()> {
        let seed = |x: f32| VectorColumnStatistics {
            centroid: vec![F32::from(x), F32::from(x)],
            radius: F32::from(1.0),
            row_count: 2,
        };
        let seeds = [seed(0.0), seed(10.0)];
        let mut transform = TransformVectorCluster::for_recluster(
            0,
            2,
            VectorDistanceType::L2,
            2,
            4,
            &seeds,
            usize::MAX,
        );
        // The first batch only covers one seed, which trains the task's model.
        let first = transform.sort_keys(&vector_block(&[[0.0, 0.0], [0.1, 0.0]], None))?;
        let second = transform.sort_keys(&vector_block(&[[10.0, 10.0], [0.1, 0.0]], None))?;
        // Later batches use the same model, and the unsampled seed keeps its cluster.
        assert_eq!(first[1], second[1]);
        let cluster = |key: u64| key >> VECTOR_CLUSTER_SHIFT;
        assert_ne!(cluster(first[0]), cluster(second[0]));
        Ok(())
    }

    #[test]
    fn test_vector_cluster_batch_rows() {
        // 1536-dim vectors fill a default block by compressed size, not by rows.
        let thresholds = BlockThresholds::new(1_000_000, 125 << 20, 16 << 20, 1000);
        let rows_per_block = thresholds.estimate_rows_per_block(1536 * 4);
        assert_eq!(rows_per_block, (16 << 20) / (1536 * 4));
        // The budget sizes a batch up to KMEANS_MAX_BATCH_BLOCKS blocks, with no
        // lower bound of one block.
        let row_bytes = 1536 * 4;
        let rows = |budget| kmeans_batch_rows(rows_per_block, row_bytes, budget);
        assert_eq!(rows(0), 1);
        assert_eq!(rows(10 * row_bytes * 2), 10);
        assert_eq!(
            rows(10 * rows_per_block * row_bytes * 2),
            10 * rows_per_block
        );
        assert_eq!(rows(usize::MAX), KMEANS_MAX_BATCH_BLOCKS * rows_per_block);
        // A recluster task of more than KMEANS_MAX_BATCH_BLOCKS blocks keeps the
        // cluster count bounded.
        assert_eq!(
            cluster_count(1000 * rows_per_block, rows_per_block),
            KMEANS_MAX_BATCH_BLOCKS
        );
        assert_eq!(cluster_count(3 * rows_per_block, rows_per_block), 3);
    }
}
