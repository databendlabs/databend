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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::VortexSegmentMeta;
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::file::OpenOptionsSessionExt;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::LayoutChildType;
use vortex::layout::LayoutRef;
use vortex::layout::layouts::buffered::BufferedStrategy;
use vortex::layout::layouts::chunked::writer::ChunkedLayoutStrategy;
use vortex::layout::layouts::collect::CollectStrategy;
use vortex::layout::layouts::compressed::CompressingStrategy;
use vortex::layout::layouts::dict::writer::DictLayoutConstraints;
use vortex::layout::layouts::dict::writer::DictLayoutOptions;
use vortex::layout::layouts::dict::writer::DictStrategy;
use vortex::layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex::layout::layouts::repartition::RepartitionStrategy;
use vortex::layout::layouts::repartition::RepartitionWriterOptions;
use vortex::layout::layouts::struct_::writer::StructStrategy;
use vortex::session::VortexSession;

#[derive(Debug, Clone)]
pub struct VortexWriteMeta {
    pub file_size: u64,
    pub shared_ranges: Vec<VortexSegmentMeta>,
    pub field_ranges: HashMap<String, Vec<VortexSegmentMeta>>,
}

pub fn write_vortex(
    table_schema: &TableSchema,
    block: DataBlock,
    write_buffer: &mut Vec<u8>,
) -> Result<VortexWriteMeta> {
    let arrow_schema = Arc::new(table_schema.into());
    let batch = block.to_record_batch_with_arrow_schema(&arrow_schema)?;
    let array = ArrayRef::from_arrow(batch, false);

    // ---- Constants ----
    const ONE_MEG: u64 = 1 << 20;

    // Row block size: controls repartition row multiples.
    let row_block_size: usize = 8192;

    // ---- Build Layout Strategy Pipeline (bottom-up) ----

    // Step 7: For each chunk, create a flat layout.
    let flat = FlatLayoutStrategy::default();
    let chunked = ChunkedLayoutStrategy::new(flat);

    // Step 6: Buffer chunks so they end up with closer segment IDs physically.
    // buffer_size: bytes to buffer before flushing. Larger = better locality, more memory.
    let buffer_size: u64 = 2 * ONE_MEG; // 2 MB
    let buffered = BufferedStrategy::new(chunked, buffer_size);

    // Step 5: Compress each chunk.
    // Compressor choices:
    //   - BtrBlocksCompressor: adaptive cascading compression, balances size vs decode speed.
    //     Automatically selects from: ALP, BitPacked, Delta, Dict, FSST, FoR, RLE, RunEnd, etc.
    //     `exclude_int_dict_encoding`: when true, skips dictionary encoding for integers.
    //   - CompactCompressor: size-optimized, uses PCO for numerics + Zstd for the rest.
    //     Configure with .with_pco_level() / .with_zstd_level() / .with_values_per_page().
    //     Requires `vortex` crate's "zstd" feature.
    let exclude_int_dict_encoding = true;
    let compressing = CompressingStrategy::new_btrblocks(buffered, exclude_int_dict_encoding);
    // Compression concurrency: number of parallel compression tasks.
    // Default: std::thread::available_parallelism(). Override with:
    //   .with_concurrency(num_threads)

    // Step 4: Prior to compression, coalesce small chunks up to a minimum size.
    let coalescing = RepartitionStrategy::new(compressing, RepartitionWriterOptions {
        // Minimum uncompressed block size in bytes before emitting.
        block_size_minimum: ONE_MEG, // 1 MB
        // Emitted blocks contain a multiple of this many rows.
        block_len_multiple: row_block_size,
        // Whether to canonicalize arrays before emitting (normalize to canonical Arrow form).
        canonicalize: true,
    });

    // Helper: compress-then-flat strategy for stats tables and dict values.
    let compress_then_flat =
        CompressingStrategy::new_btrblocks(FlatLayoutStrategy::default(), false);

    // Step 3: Apply dictionary encoding or fallback.
    let dict = DictStrategy::new(
        coalescing.clone(),
        compress_then_flat.clone(),
        coalescing,
        DictLayoutOptions {
            constraints: DictLayoutConstraints {
                // Maximum dictionary size in bytes. Larger = more values can be dict-encoded.
                max_bytes: 1024 * 1024, // 1 MB
                // Maximum number of distinct values in the dictionary.
                max_len: u16::MAX, // 65535
            },
        },
    );

    // Step 2: Repartition each column to fixed row counts.
    let repartition = RepartitionStrategy::new(dict, RepartitionWriterOptions {
        // No minimum block size in bytes for initial repartition.
        block_size_minimum: 0,
        // Always repartition into row_block_size row blocks.
        block_len_multiple: row_block_size,
        // No canonicalization at this stage.
        canonicalize: false,
    });

    // Step 1: Split struct columns, with a validity collection strategy.
    let validity_strategy = CollectStrategy::new(compress_then_flat);
    let strategy = Arc::new(StructStrategy::new(repartition, validity_strategy));

    // ---- Write Options ----
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let write_options = session
        .write_options()
        // Layout strategy built above.
        .with_strategy(strategy)
        // Disable file-level statistics in the footer.
        .with_file_statistics(vec![]);
    // Other available options on VortexWriteOptions:
    //   .exclude_dtype()  — omit DType from file; caller must supply it at read time.
    // Note: max_variable_length_statistics_size (default 64) is not configurable via public API.

    write_options
        .blocking(&runtime)
        .write(&mut *write_buffer, array.to_array_iterator())
        .map_err(|e| ErrorCode::Internal(format!("Failed to write vortex file: {e}")))?;

    build_vortex_write_meta(write_buffer)
}

fn build_vortex_write_meta(write_buffer: &[u8]) -> Result<VortexWriteMeta> {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = session
        .open_options()
        .open_buffer(write_buffer.to_vec())
        .map_err(|e| ErrorCode::Internal(format!("Failed to open vortex file: {e}")))?;
    let footer = file.footer();

    let file_size = write_buffer.len() as u64;
    let segment_map = footer.segment_map();

    let mut shared_segment_ids = BTreeSet::new();
    let mut field_segment_ids: HashMap<String, BTreeSet<u32>> = HashMap::new();
    collect_segment_ids(
        footer.layout(),
        None,
        &mut shared_segment_ids,
        &mut field_segment_ids,
    )?;

    let shared_ranges = normalize_ranges(segment_ids_to_ranges(
        segment_map.as_ref(),
        &shared_segment_ids,
    ));

    let mut field_ranges = HashMap::with_capacity(field_segment_ids.len());
    for (field, ids) in field_segment_ids {
        field_ranges.insert(
            field,
            normalize_ranges(segment_ids_to_ranges(segment_map.as_ref(), &ids)),
        );
    }

    let mut shared_ranges_with_tail = shared_ranges;
    // Make sure footer-related reads can be served from prefetched data when opening.
    shared_ranges_with_tail.push(vortex_footer_tail_range(file_size));

    Ok(VortexWriteMeta {
        file_size,
        shared_ranges: normalize_ranges(shared_ranges_with_tail),
        field_ranges,
    })
}

fn collect_segment_ids(
    layout: &LayoutRef,
    current_field: Option<String>,
    shared_segment_ids: &mut BTreeSet<u32>,
    field_segment_ids: &mut HashMap<String, BTreeSet<u32>>,
) -> Result<()> {
    for segment_id in layout.segment_ids() {
        if let Some(field) = current_field.as_ref() {
            field_segment_ids
                .entry(field.clone())
                .or_default()
                .insert(*segment_id);
        } else {
            shared_segment_ids.insert(*segment_id);
        }
    }

    for idx in 0..layout.nchildren() {
        let child = layout.child(idx).map_err(|e| {
            ErrorCode::Internal(format!("Failed to access vortex layout child: {e}"))
        })?;
        let child_field = if current_field.is_some() {
            current_field.clone()
        } else {
            match layout.child_type(idx) {
                LayoutChildType::Field(name) => Some(name.to_string()),
                _ => None,
            }
        };

        collect_segment_ids(&child, child_field, shared_segment_ids, field_segment_ids)?;
    }

    Ok(())
}

fn segment_ids_to_ranges(
    segment_map: &[vortex::file::SegmentSpec],
    ids: &BTreeSet<u32>,
) -> Vec<VortexSegmentMeta> {
    ids.iter()
        .filter_map(|id| segment_map.get(*id as usize))
        .map(|seg| VortexSegmentMeta {
            offset: seg.offset,
            len: u64::from(seg.length),
        })
        .collect()
}

fn normalize_ranges(mut ranges: Vec<VortexSegmentMeta>) -> Vec<VortexSegmentMeta> {
    if ranges.is_empty() {
        return ranges;
    }

    ranges.sort_by_key(|r| (r.offset, r.len));
    let mut merged: Vec<VortexSegmentMeta> = Vec::with_capacity(ranges.len());

    for range in ranges {
        if let Some(last) = merged.last_mut() {
            let last_end = last.offset + last.len;
            let curr_end = range.offset + range.len;
            if range.offset <= last_end {
                if curr_end > last_end {
                    last.len = curr_end - last.offset;
                }
                continue;
            }
        }
        merged.push(range);
    }

    merged
}

fn vortex_footer_tail_range(file_size: u64) -> VortexSegmentMeta {
    // 1MB tail read matches vortex-file default initial footer read size.
    const TAIL_SIZE: u64 = 1 << 20;
    let offset = file_size.saturating_sub(TAIL_SIZE);
    VortexSegmentMeta {
        offset,
        len: file_size.saturating_sub(offset),
    }
}
