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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef;
use vortex::array::arrow::FromArrowArray;
use vortex::expr::stats::Stat;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::session::RuntimeSessionExt;
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
use vortex::layout::layouts::zoned::writer::ZonedLayoutOptions;
use vortex::layout::layouts::zoned::writer::ZonedStrategy;
use vortex::session::VortexSession;

pub fn write_vortex(
    table_schema: &TableSchema,
    block: DataBlock,
    write_buffer: &mut Vec<u8>,
) -> Result<()> {
    let arrow_schema = Arc::new(table_schema.into());
    let batch = block.to_record_batch_with_arrow_schema(&arrow_schema)?;
    let array = ArrayRef::from_arrow(batch, false);

    // ---- Constants ----
    const ONE_MEG: u64 = 1 << 20;

    // Row block size: controls zone map granularity and repartition row multiples.
    // Smaller = finer-grained pruning but more metadata overhead.
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

    // Step 2: Calculate statistics (zone maps) for each row group.
    let zoned = ZonedStrategy::new(dict, compress_then_flat.clone(), ZonedLayoutOptions {
        // Zone map block size in rows. Each zone covers this many rows.
        block_size: row_block_size,
        // Which statistics to compute per zone.
        stats: vec![
            Stat::Min,
            Stat::Max,
            Stat::Sum,
            Stat::NullCount,
            Stat::NaNCount,
        ]
        .into(),
        // Max byte length for variable-length stats (e.g. min/max of strings).
        // Longer values are truncated.
        max_variable_length_statistics_size: 64,
        // Concurrency for stats computation.
        concurrency: std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1),
    });

    // Step 1: Repartition each column to fixed row counts.
    let repartition = RepartitionStrategy::new(zoned, RepartitionWriterOptions {
        // No minimum block size in bytes for initial repartition.
        block_size_minimum: 0,
        // Always repartition into row_block_size row blocks.
        block_len_multiple: row_block_size,
        // No canonicalization at this stage.
        canonicalize: false,
    });

    // Step 0: Split struct columns, with a validity collection strategy.
    let validity_strategy = CollectStrategy::new(compress_then_flat);
    let strategy = Arc::new(StructStrategy::new(repartition, validity_strategy));

    // ---- Write Options ----
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let write_options = session
        .write_options()
        // Layout strategy built above.
        .with_strategy(strategy)
        // File-level statistics written into the footer for pruning.
        // These are computed over the entire file (not per-zone).
        .with_file_statistics(vec![
            Stat::Min,
            Stat::Max,
            Stat::Sum,
            Stat::NullCount,
            Stat::NaNCount,
        ]);
    // Other available options on VortexWriteOptions:
    //   .exclude_dtype()  — omit DType from file; caller must supply it at read time.
    // Note: max_variable_length_statistics_size (default 64) is not configurable via public API.

    write_options
        .blocking(&runtime)
        .write(write_buffer, array.to_array_iterator())
        .map_err(|e| ErrorCode::Internal(format!("Failed to write vortex file: {e}")))?;

    Ok(())
}
