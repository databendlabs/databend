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
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_metrics::storage::metrics_inc_remote_io_read_bytes;
use databend_common_metrics::storage::metrics_inc_remote_io_read_parts;
use databend_storages_common_cache::CacheLockStats;
use databend_storages_common_io::MergeIOReadResult;
use databend_storages_common_io::OwnerMemory;
use databend_storages_common_io::RangeReader;
use databend_storages_common_io::ReadSettings;
use opendal::Buffer;
use opendal::Scheme;

use super::BlockReadContext;
use super::BlockReadResult;
use crate::FuseBlockPartInfo;
use crate::io::OffsetsIndex;
use crate::io::create_file_range_reader_with_stats;

const GRANULE_IO_RANGE_SIZE: u64 = 16 * 1024 * 1024;
/// A hole this large always splits: a new request is cheaper than reading through.
const COALESCE_HARD_STOP_GAP: u64 = 2 * 1024 * 1024;
const MAX_COALESCED_RANGE: u64 = 8 * 1024 * 1024;
/// Bound speculative lookahead after the latest acceptable prefix. This keeps
/// pathological tiny-range inputs linear with a small constant while still
/// allowing later dense ranges to redeem a few sparse prefixes.
const MAX_REJECTED_COALESCE_PREFIXES: usize = 64;

/// Cost model for coalescing scattered column ranges into storage requests.
struct RangeCoalescePolicy {
    /// Holes up to this size always merge.
    always_merge_gap: u64,
    /// Target minimum effective bytes per request; 0 disables cost-based
    /// coalescing in favor of gap-only merging.
    equivalent_bytes: u64,
    /// Chunk size of the disk cache layer (1 = no cache). The decision
    /// arithmetic aligns spans to it so hollow/content reflect the bytes a
    /// fetch actually moves.
    chunk_align: u64,
}

impl RangeCoalescePolicy {
    fn align_for_cost(&self, range: &Range<u64>) -> Range<u64> {
        if self.chunk_align <= 1 {
            return range.clone();
        }
        let start = range.start / self.chunk_align * self.chunk_align;
        let end = range.end.div_ceil(self.chunk_align) * self.chunk_align;
        start..end
    }

    fn merge_ranges(&self, ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
        if self.equivalent_bytes == 0 {
            return self.merge_by_gap(ranges);
        }
        self.coalesce_ranges(ranges)
    }

    fn merge_by_gap(&self, ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
        let mut result: Vec<Range<u64>> = Vec::with_capacity(ranges.len());
        for range in ranges {
            if let Some(last) = result.last_mut()
                && last.end - last.start < GRANULE_IO_RANGE_SIZE
                && range.start <= last.end.saturating_add(self.always_merge_gap)
            {
                last.end = last.end.max(range.end);
                continue;
            }
            result.push(range);
        }
        result
    }

    /// Merges file-ordered ranges when one larger request beats many small ones: a
    /// chain prefix is accepted while the average bytes per original request stays
    /// below `equivalent_bytes`, or while the hole bytes stay below 0.8x the
    /// useful bytes. Emitted ranges stay unaligned; only the arithmetic uses the
    /// aligned spans.
    fn coalesce_ranges(&self, ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
        let mut atoms: Vec<(Range<u64>, Range<u64>)> = Vec::with_capacity(ranges.len());
        for range in ranges {
            let aligned = self.align_for_cost(&range);
            if let Some((real, last)) = atoms.last_mut() {
                let gap = aligned.start.saturating_sub(last.end);
                if gap <= self.always_merge_gap
                    && aligned.end.max(last.end) - last.start <= MAX_COALESCED_RANGE
                {
                    real.end = real.end.max(range.end);
                    last.end = last.end.max(aligned.end);
                    continue;
                }
            }
            atoms.push((range, aligned));
        }

        let mut result = Vec::with_capacity(atoms.len());
        let mut index = 0;
        while index < atoms.len() {
            let start = atoms[index].1.start;
            let mut end = atoms[index].1.end;
            let mut content = end - start;
            let mut hollow = 0u64;
            let mut count = 1u64;
            let mut accepted = 1;
            let mut rejected_after_accepted = 0;
            for (_, next) in &atoms[index + 1..] {
                let gap = next.start.saturating_sub(end);
                if gap >= COALESCE_HARD_STOP_GAP || next.end - start > MAX_COALESCED_RANGE {
                    break;
                }
                hollow += gap;
                content += next.end - next.start;
                end = next.end;
                count += 1;
                let per_request = (content + hollow) / count;
                if per_request <= self.equivalent_bytes || hollow * 5 <= content * 4 {
                    accepted = count as usize;
                    rejected_after_accepted = 0;
                } else {
                    rejected_after_accepted += 1;
                    if rejected_after_accepted >= MAX_REJECTED_COALESCE_PREFIXES {
                        break;
                    }
                }
            }
            let real_start = atoms[index].0.start;
            let real_end = atoms[index + accepted - 1].0.end;
            result.push(real_start..real_end);
            index += accepted;
        }
        result
    }
}

enum GranuleColumnOutput {
    Empty,
    Slice {
        range: Range<u64>,
        sub: Range<usize>,
    },
}

fn collect_ranges(groups: &[Vec<Range<usize>>]) -> Vec<Range<usize>> {
    let mut result = Vec::with_capacity(groups.iter().map(Vec::len).sum());
    for group in groups {
        for range in group {
            result.push(range.clone());
        }
    }
    result
}

fn block_file_len(part: &FuseBlockPartInfo) -> u64 {
    let mut result = 0;
    for meta in part.columns_meta.values() {
        let (offset, len) = meta.offset_length();
        result = result.max(offset.saturating_add(len));
    }
    result
}

fn merge_column_ranges(
    column_id: ColumnId,
    input_ranges: &[Range<u64>],
    policy: &RangeCoalescePolicy,
) -> Result<(VecDeque<GranuleColumnOutput>, VecDeque<Range<u64>>)> {
    let mut non_empty = Vec::with_capacity(input_ranges.len());
    let mut previous_start = None;
    for range in input_ranges {
        if range.is_empty() {
            continue;
        }
        if previous_start.is_some_and(|start| range.start < start) {
            return Err(ErrorCode::Internal(format!(
                "granule column {column_id} ranges are not in file order"
            )));
        }
        previous_start = Some(range.start);
        non_empty.push(range.clone());
    }

    let merged_ranges = policy.merge_ranges(non_empty);
    let mut outputs = VecDeque::with_capacity(input_ranges.len());
    let mut merged_index = 0;

    for input in input_ranges {
        if input.is_empty() {
            outputs.push_back(GranuleColumnOutput::Empty);
            continue;
        }

        while merged_ranges
            .get(merged_index)
            .is_some_and(|range| range.end < input.end)
        {
            merged_index += 1;
        }
        let range = merged_ranges.get(merged_index).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "granule range {input:?} not found in merged ranges {merged_ranges:?}"
            ))
        })?;
        if range.start > input.start || input.end > range.end {
            return Err(ErrorCode::Internal(format!(
                "granule range {input:?} not covered by merged range {range:?}"
            )));
        }

        let start = (input.start - range.start) as usize;
        let end = (input.end - range.start) as usize;
        outputs.push_back(GranuleColumnOutput::Slice {
            range: range.clone(),
            sub: start..end,
        });
    }

    Ok((outputs, merged_ranges.into()))
}

fn record_remote_bytes(ranges: &[Range<u64>]) {
    let mut total = 0;
    for range in ranges {
        total += range.end - range.start;
    }
    metrics_inc_remote_io_read_bytes(total);
    Profile::record_usize_profile(ProfileStatisticsName::ScanBytesFromRemote, total as usize);
}

struct GranuleColumnReader {
    column_id: ColumnId,
    has_dictionary: bool,
    dictionary: Option<Buffer>,
    reader: Box<dyn RangeReader>,
    outputs: VecDeque<GranuleColumnOutput>,
    ranges: VecDeque<Range<u64>>,
    selected_bytes: u64,
    coalesced_bytes: u64,
    coalesced_requests: usize,
    fetch_part_num: usize,
    current: Option<(Range<u64>, Buffer)>,
}

impl GranuleColumnReader {
    fn try_create(
        column_id: ColumnId,
        mut reader: Box<dyn RangeReader>,
        input_ranges: &[Range<u64>],
        has_dictionary: bool,
        policy: &RangeCoalescePolicy,
        fetch_part_num: usize,
    ) -> Result<Self> {
        let (outputs, ranges) = merge_column_ranges(column_id, input_ranges, policy)?;
        let selected_bytes = input_ranges
            .iter()
            .map(|range| range.end - range.start)
            .sum();
        let coalesced_bytes = ranges.iter().map(|range| range.end - range.start).sum();
        let coalesced_requests = ranges.len();
        let fetch_part_num = fetch_part_num.max(1);

        // One batched hint for the initial window: the cache layer probes
        // the disk cache once for all of it.
        let mut initial = Vec::with_capacity(fetch_part_num);
        for range in ranges.iter().take(fetch_part_num) {
            initial.push(range.clone());
        }
        if !initial.is_empty() {
            let _ = reader.prefetch(&initial);
        }

        Ok(Self {
            column_id,
            has_dictionary,
            dictionary: None,
            reader,
            outputs,
            ranges,
            selected_bytes,
            coalesced_bytes,
            coalesced_requests,
            fetch_part_num,
            current: None,
        })
    }

    fn read_buffer(&mut self) -> Result<Buffer> {
        let output = match self.outputs.pop_front() {
            Some(output) => output,
            None => {
                return Err(ErrorCode::Internal(format!(
                    "granule column {} has no remaining ranges",
                    self.column_id
                )));
            }
        };

        let (range, sub) = match output {
            GranuleColumnOutput::Empty => return Ok(Buffer::new()),
            GranuleColumnOutput::Slice { range, sub } => (range, sub),
        };

        self.load_range(&range)?;
        let buffer = match &self.current {
            Some((current, buffer)) if current == &range => buffer,
            _ => {
                return Err(ErrorCode::Internal(format!(
                    "granule column {} did not load range {range:?}",
                    self.column_id
                )));
            }
        };

        let result = buffer.slice(sub);
        if !self.next_output_uses(&range) {
            self.current = None;
        }
        Ok(result)
    }

    fn load_range(&mut self, range: &Range<u64>) -> Result<()> {
        if let Some((current, _)) = &self.current {
            if current == range {
                return Ok(());
            }
        }

        let expected = match self.ranges.pop_front() {
            Some(range) => range,
            None => {
                return Err(ErrorCode::Internal(format!(
                    "granule column {} has no merged range for output",
                    self.column_id
                )));
            }
        };
        if &expected != range {
            return Err(ErrorCode::Internal(format!(
                "granule column {} expected range {expected:?}, got {range:?}",
                self.column_id
            )));
        }

        // Hint the window replenishment before blocking on this read: with a
        // lookahead of one, the next range must be announced downstream before
        // this read consumes their shared boundary chunk, and the downstream
        // worker can work ahead while this read blocks.
        self.prefetch_next();
        let buffer = self.reader.read(expected.clone())?;
        self.current = Some((expected, buffer));
        Ok(())
    }

    fn next_output_uses(&self, range: &Range<u64>) -> bool {
        match self.outputs.front() {
            Some(GranuleColumnOutput::Slice { range: next, .. }) => next == range,
            _ => false,
        }
    }

    fn prefetch_next(&mut self) {
        let index = self.fetch_part_num - 1;
        if let Some(range) = self.ranges.get(index) {
            let _ = self.reader.prefetch(std::slice::from_ref(range));
        }
    }

    fn read_next(&mut self) -> Result<Buffer> {
        if self.has_dictionary && self.dictionary.is_none() {
            self.dictionary = Some(self.read_buffer()?);
        }

        let data = self.read_buffer()?;
        let Some(dictionary) = &self.dictionary else {
            return Ok(data);
        };

        let mut parts = Vec::new();
        parts.extend(dictionary.clone());
        parts.extend(data);
        Ok(Buffer::from(parts))
    }
}

pub(crate) struct GranuleRangeRead {
    pub(crate) range: Range<usize>,
    pub(crate) data: BlockReadResult,
}

pub(crate) struct GranuleDataReader {
    location: String,
    ranges: VecDeque<Range<usize>>,
    column_readers: Vec<GranuleColumnReader>,
    granule_rows: usize,
    block_rows: usize,
}

impl GranuleDataReader {
    pub(crate) fn create(
        read_context: &BlockReadContext,
        settings: &ReadSettings,
        part: &FuseBlockPartInfo,
        groups: &[Vec<Range<usize>>],
        offsets: &OffsetsIndex,
        lock_stats: Option<Arc<CacheLockStats>>,
    ) -> Result<Self> {
        let ranges = collect_ranges(groups);
        offsets.validate_ranges(&ranges, part.nums_rows)?;
        let file_len = block_file_len(part);
        let fetch_part_num = read_context.storage_fetch_part_num()?.max(1);
        let range_size = usize::try_from(GRANULE_IO_RANGE_SIZE).unwrap_or(usize::MAX);
        let held_budget = range_size.saturating_mul(fetch_part_num.saturating_add(2));
        let policy = RangeCoalescePolicy {
            always_merge_gap: settings.max_gap_size,
            // Local filesystems seek cheaply; the cost model only pays off on
            // object storage where every request costs a round trip.
            equivalent_bytes: match read_context.operator().info().scheme() {
                Scheme::Fs | Scheme::Memory => 0,
                _ => read_context.storage_io_merge_equivalent_bytes()?,
            },
            chunk_align: crate::io::disk_cache_chunk_size().unwrap_or(1),
        };

        let mut column_readers = Vec::new();
        for (column_id, ..) in read_context.project_indices().values() {
            let meta = part.columns_meta.get(column_id).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "granule data metadata missing projected column {column_id}"
                ))
            })?;
            let (has_dictionary, byte_ranges) =
                offsets.column_byte_ranges(*column_id, meta, &ranges)?;
            record_remote_bytes(&byte_ranges);

            let reader = create_file_range_reader_with_stats(
                read_context.operator().clone(),
                part.location.clone(),
                file_len,
                fetch_part_num,
                GRANULE_IO_RANGE_SIZE,
                held_budget,
                read_context.put_cache(),
                lock_stats.clone(),
            )?;
            let reader = GranuleColumnReader::try_create(
                *column_id,
                reader,
                &byte_ranges,
                has_dictionary,
                &policy,
                fetch_part_num,
            )?;
            column_readers.push(reader);
        }

        Ok(Self {
            location: part.location.clone(),
            ranges: ranges.into(),
            column_readers,
            granule_rows: offsets.granule_rows(),
            block_rows: part.nums_rows,
        })
    }

    pub(crate) fn read_plan_stats(&self) -> (usize, u64, u64, usize) {
        self.column_readers.iter().fold(
            (0, 0, 0, 0),
            |(columns, selected, coalesced, requests), reader| {
                (
                    columns + 1,
                    selected + reader.selected_bytes,
                    coalesced + reader.coalesced_bytes,
                    requests + reader.coalesced_requests,
                )
            },
        )
    }

    pub(crate) fn read_next(&mut self) -> Result<Option<GranuleRangeRead>> {
        let granule_range = match self.ranges.pop_front() {
            Some(range) => range,
            None => return Ok(None),
        };
        let row_range = granule_range.start * self.granule_rows
            ..(granule_range.end * self.granule_rows).min(self.block_rows);

        metrics_inc_remote_io_read_parts(1);
        let mut chunks = Vec::with_capacity(self.column_readers.len());
        let mut column_offsets = HashMap::with_capacity(self.column_readers.len());
        for (index, reader) in self.column_readers.iter_mut().enumerate() {
            let buffer = reader.read_next()?;
            let len = buffer.len();
            chunks.push((index, buffer));
            column_offsets.insert(reader.column_id, (index, 0..len));
        }

        let result = MergeIOReadResult::create(
            OwnerMemory::create(chunks),
            column_offsets,
            self.location.clone(),
        );
        let data = BlockReadResult::create_with_row_range(result, row_range);
        Ok(Some(GranuleRangeRead {
            range: granule_range,
            data,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * 1024;

    fn policy(equivalent_bytes: u64) -> RangeCoalescePolicy {
        RangeCoalescePolicy {
            always_merge_gap: 48,
            equivalent_bytes,
            chunk_align: 1,
        }
    }

    #[test]
    fn test_chunk_align_glues_atoms_sharing_chunks() {
        // Real spans split at the 2MiB hard stop, but under a 1MiB chunk grid
        // both sides fetch whole chunks anyway: the aligned hole shrinks to one
        // chunk and the amplification branch accepts.
        let ranges = vec![0..25 * KIB, (2 * MIB + 15 * KIB)..(2 * MIB + 40 * KIB)];
        let unaligned = policy(MIB).merge_ranges(ranges.clone());
        assert_eq!(unaligned, ranges);

        let aligned_policy = RangeCoalescePolicy {
            chunk_align: MIB,
            ..policy(MIB)
        };
        let merged = aligned_policy.merge_ranges(ranges);
        assert_eq!(merged, vec![0..(2 * MIB + 40 * KIB)]);
    }

    #[test]
    fn test_coalesce_scattered_small_ranges_into_one_request() {
        // 25KiB of content every 725KiB: the per-request average stays under the
        // equivalent size, so the whole chain becomes one request.
        let ranges: Vec<_> = (0..10)
            .map(|i| (i * 725 * KIB)..(i * 725 * KIB + 25 * KIB))
            .collect();
        let merged = policy(MIB).merge_ranges(ranges);
        assert_eq!(merged, vec![0..(9 * 725 * KIB + 25 * KIB)]);
    }

    #[test]
    fn test_coalesce_rejects_sparse_pair() {
        // Per-request 1050KiB > 1MiB and amplification 19x > 0.8: keep separate.
        let ranges = vec![0..100 * KIB, 2_000 * KIB..2_100 * KIB];
        let merged = policy(MIB).merge_ranges(ranges.clone());
        assert_eq!(merged, ranges);
    }

    #[test]
    fn test_coalesce_ratio_accepts_dense_large_ranges() {
        // Per-request 2MiB > 1MiB, but the 1MiB hole is only a third of the
        // 3MiB content: the amplification branch accepts.
        let ranges = vec![0..1_536 * KIB, 2_560 * KIB..4_096 * KIB];
        let merged = policy(MIB).merge_ranges(ranges);
        assert_eq!(merged, vec![0..4_096 * KIB]);
    }

    #[test]
    fn test_coalesce_hard_stop_splits_large_hole() {
        // A 2MiB hole always splits, even with an unlimited equivalent size.
        let ranges = vec![0..25 * KIB, 2_073 * KIB..2_098 * KIB];
        let merged = policy(u64::MAX).merge_ranges(ranges.clone());
        assert_eq!(merged, ranges);
    }

    #[test]
    fn test_coalesce_best_prefix_redeems_early_hole() {
        // The A..B prefix fails both criteria, but appending C dilutes the hole,
        // so the whole chain merges (a greedy scan would split after A).
        let a = 0..100 * KIB;
        let b = 2_000 * KIB..2_100 * KIB;
        let c = 2_110 * KIB..7_000 * KIB;
        let merged = policy(MIB).merge_ranges(vec![a.clone(), b, c.clone()]);
        assert_eq!(merged, vec![a.start..c.end]);
    }

    #[test]
    fn test_coalesce_bounds_rejected_lookahead() {
        let ranges: Vec<_> = (0..256).map(|i| (i * KIB)..(i * KIB + 1)).collect();
        let merged = policy(1).merge_ranges(ranges.clone());
        assert_eq!(merged, ranges);
    }

    #[test]
    fn test_coalesce_respects_max_range() {
        // Dense 2MiB ranges with tiny holes: the chain splits at the 8MiB cap.
        let ranges: Vec<_> = (0..5)
            .map(|i| (i * 2_058 * KIB)..(i * 2_058 * KIB + 2_048 * KIB))
            .collect();
        let merged = policy(MIB).merge_ranges(ranges);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_coalesce_glues_near_adjacent_ranges() {
        let ranges = vec![0..100, 100..200, 240..300];
        let merged = policy(MIB).merge_ranges(ranges);
        assert_eq!(merged, vec![0..300]);
    }

    #[test]
    fn test_merge_column_ranges_rejects_unordered_inputs() {
        let inputs = vec![100..200, 0..50];
        let err = match merge_column_ranges(7, &inputs, &policy(MIB)) {
            Ok(_) => panic!("unordered ranges must be rejected"),
            Err(err) => err,
        };
        assert!(err.message().contains("not in file order"), "{err}");
    }

    #[test]
    fn test_merge_column_ranges_maps_slices_and_empties() {
        let inputs = vec![0..100, 0..0, 100_000..100_200];
        let (outputs, merged) = merge_column_ranges(0, &inputs, &policy(MIB)).unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0], 0..100_200);
        assert_eq!(outputs.len(), 3);
        assert!(matches!(
            &outputs[0],
            GranuleColumnOutput::Slice { range, sub } if *range == (0..100_200) && *sub == (0..100)
        ));
        assert!(matches!(&outputs[1], GranuleColumnOutput::Empty));
        assert!(matches!(
            &outputs[2],
            GranuleColumnOutput::Slice { range, sub }
                if *range == (0..100_200) && *sub == (100_000..100_200)
        ));
    }

    #[test]
    fn test_gap_only_merge_stops_after_range_limit() {
        let ranges = vec![
            0..GRANULE_IO_RANGE_SIZE,
            GRANULE_IO_RANGE_SIZE..GRANULE_IO_RANGE_SIZE + 10,
        ];
        assert_eq!(policy(0).merge_ranges(ranges.clone()), ranges);
    }

    #[test]
    fn test_zero_equivalent_falls_back_to_gap_only() {
        let inputs: Vec<_> = (0..4)
            .map(|i| (i * 725 * KIB)..(i * 725 * KIB + 25 * KIB))
            .collect();
        let (_, merged) = merge_column_ranges(0, &inputs, &policy(0)).unwrap();
        assert_eq!(merged.len(), 4);
        let (_, merged) = merge_column_ranges(0, &inputs, &policy(MIB)).unwrap();
        assert_eq!(merged.len(), 1);
    }
}
