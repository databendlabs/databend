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

use std::any::Any;
use std::ops::BitAnd;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::InternalColumnMeta;
use databend_common_catalog::runtime_filter_info::RuntimeBloomFilter;
use databend_common_catalog::runtime_filter_info::RuntimeFilterEntry;
use databend_common_catalog::runtime_filter_info::RuntimeFilterStats;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::FieldIndex;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::MutableBitmap;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use roaring::RoaringTreemap;

use crate::pruning::ExprBloomFilter;

pub struct TransformRuntimeFilter {
    ctx: Arc<dyn TableContext>,
    scan_id: usize,
    schema: DataSchema,
    scan_progress: Arc<Progress>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,

    cached_bloom_filters: Option<Vec<BloomRuntimeFilterRef>>,
}

#[derive(Clone)]
struct BloomRuntimeFilterRef {
    column_index: FieldIndex,
    filter_id: usize,
    filter: RuntimeBloomFilter,
    stats: Arc<RuntimeFilterStats>,
}

impl TransformRuntimeFilter {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        scan_id: usize,
        schema: DataSchema,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> ProcessorPtr {
        let scan_progress = ctx.get_scan_progress();
        ProcessorPtr::create(Box::new(Self {
            ctx,
            scan_id,
            schema,
            scan_progress,
            input,
            output,
            output_data: None,
            cached_bloom_filters: None,
        }))
    }

    fn try_init_bloom_filters(&mut self) {
        if self.cached_bloom_filters.is_some() {
            return;
        }

        let bloom_filters = self
            .ctx
            .get_runtime_filters(self.scan_id)
            .into_iter()
            .filter_map(|entry| {
                let filter_id = entry.id;
                let RuntimeFilterEntry { bloom, stats, .. } = entry;
                let bloom = bloom?;
                let column_index = self.schema.index_of(bloom.column_name.as_str()).ok()?;
                Some(BloomRuntimeFilterRef {
                    column_index,
                    filter_id,
                    filter: bloom.filter.clone(),
                    stats,
                })
            })
            .collect::<Vec<_>>();

        if bloom_filters.is_empty() {
            return;
        }

        let mut filter_ids = bloom_filters
            .iter()
            .map(|f| f.filter_id)
            .collect::<Vec<_>>();
        filter_ids.sort_unstable();
        log::info!(
            "RUNTIME-FILTER: scan_id={} bloom_filters={} filter_ids={:?}",
            self.scan_id,
            bloom_filters.len(),
            filter_ids
        );
        self.cached_bloom_filters = Some(bloom_filters);
    }

    fn apply_bloom_runtime_filters(&mut self, data_block: &DataBlock) -> Result<Option<Bitmap>> {
        self.try_init_bloom_filters();
        let Some(bloom_filters) = self.cached_bloom_filters.as_ref() else {
            return Ok(None);
        };

        let mut bitmaps = Vec::with_capacity(bloom_filters.len());
        for runtime_filter in bloom_filters.iter() {
            let mut bitmap = MutableBitmap::from_len_zeroed(data_block.num_rows());
            let probe_block_entry = data_block.get_by_offset(runtime_filter.column_index);
            let probe_column = probe_block_entry.to_column();

            let start = Instant::now();
            ExprBloomFilter::new(&runtime_filter.filter).apply(probe_column, &mut bitmap)?;
            let elapsed = start.elapsed();

            let unset_bits = bitmap.null_count();
            runtime_filter
                .stats
                .record_bloom(elapsed.as_nanos() as u64, unset_bits as u64);
            bitmaps.push(bitmap);
        }

        if bitmaps.is_empty() {
            return Ok(None);
        }

        let rf_bitmap = bitmaps
            .into_iter()
            .reduce(|acc, rf_filter| acc.bitand(&rf_filter.into()))
            .unwrap();

        Ok(Some(rf_bitmap.into()))
    }

    fn maybe_update_internal_column_meta(
        mut block: DataBlock,
        bitmap: &Bitmap,
    ) -> Result<DataBlock> {
        let Some(meta) = block.take_meta() else {
            return Ok(block);
        };

        let mut internal = match InternalColumnMeta::downcast_from_err(meta) {
            Ok(internal) => internal,
            Err(meta) => return block.add_meta(Some(meta)),
        };

        let num_rows_before = bitmap.len();
        if num_rows_before == 0 {
            return block.add_meta(Some(internal.boxed()));
        }

        let kept_positions = RoaringTreemap::from_sorted_iter(
            (0..num_rows_before)
                .filter(|i| unsafe { bitmap.get_bit_unchecked(*i) })
                .map(|i| i as u64),
        )
        .unwrap();

        let new_offsets = match &internal.offsets {
            None => Some(RoaringTreemap::from_sorted_iter(kept_positions.iter()).unwrap()),
            Some(offsets) => {
                let mut selected = Vec::with_capacity(kept_positions.len() as usize);
                for (idx, origin_row) in offsets.iter().take(num_rows_before).enumerate() {
                    if unsafe { bitmap.get_bit_unchecked(idx) } {
                        selected.push(origin_row);
                    }
                }
                Some(RoaringTreemap::from_sorted_iter(selected).unwrap())
            }
        };
        internal.offsets = new_offsets;

        match (internal.matched_rows.take(), internal.matched_scores.take()) {
            (Some(rows), Some(scores)) => {
                debug_assert_eq!(rows.len(), scores.len());
                let mut new_rows = Vec::with_capacity(rows.len());
                let mut new_scores = Vec::with_capacity(scores.len());
                for (idx, score) in rows.into_iter().zip(scores.into_iter()) {
                    if kept_positions.contains(idx as u64) {
                        new_rows.push((kept_positions.rank(idx as u64) - 1) as usize);
                        new_scores.push(score);
                    }
                }
                internal.matched_rows = Some(new_rows);
                internal.matched_scores = Some(new_scores);
            }
            (Some(rows), None) => {
                internal.matched_rows = Some(
                    rows.into_iter()
                        .filter(|idx| kept_positions.contains(*idx as u64))
                        .map(|idx| (kept_positions.rank(idx as u64) - 1) as usize)
                        .collect(),
                );
                internal.matched_scores = None;
            }
            (None, other) => {
                internal.matched_rows = None;
                internal.matched_scores = other;
            }
        }

        if let Some(vector_scores) = internal.vector_scores.take() {
            internal.vector_scores = Some(
                vector_scores
                    .into_iter()
                    .filter(|(idx, _score)| kept_positions.contains(*idx as u64))
                    .map(|(idx, score)| ((kept_positions.rank(idx as u64) - 1) as usize, score))
                    .collect(),
            );
        }

        block.add_meta(Some(internal.boxed()))
    }

    fn add_output_block(&mut self, data_block: &DataBlock) {
        let progress_values = ProgressValues {
            rows: data_block.num_rows(),
            bytes: data_block.memory_size(),
        };
        self.scan_progress.incr(&progress_values);
        Profile::record_usize_profile(ProfileStatisticsName::ScanBytes, data_block.memory_size());
    }
}

#[async_trait::async_trait]
impl Processor for TransformRuntimeFilter {
    fn name(&self) -> String {
        String::from("TransformRuntimeFilter")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<databend_common_pipeline::core::Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.add_output_block(&data_block);
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.has_data() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        Ok(())
    }

    fn process(&mut self) -> Result<()> {
        let Some(data) = self.input.pull_data() else {
            return Ok(());
        };

        let data_block = data?;
        if data_block.num_rows() == 0 {
            self.output_data = Some(data_block);
            return Ok(());
        }

        let bloom_start = Instant::now();
        let Some(bitmap) = self.apply_bloom_runtime_filters(&data_block)? else {
            self.output_data = Some(data_block);
            return Ok(());
        };

        let rows_before = data_block.num_rows();
        let output_block = if bitmap.null_count() == 0 {
            data_block
        } else {
            let mut filtered = data_block.filter_with_bitmap(&bitmap)?;
            filtered = Self::maybe_update_internal_column_meta(filtered, &bitmap)?;
            filtered
        };

        let rows_after = output_block.num_rows();
        let bloom_duration = bloom_start.elapsed();
        Profile::record_usize_profile(
            ProfileStatisticsName::RuntimeFilterBloomTime,
            bloom_duration.as_nanos() as usize,
        );
        if rows_before > rows_after {
            Profile::record_usize_profile(
                ProfileStatisticsName::RuntimeFilterBloomRowsFiltered,
                rows_before - rows_after,
            );
        }

        self.output_data = Some(output_block);
        Ok(())
    }
}
