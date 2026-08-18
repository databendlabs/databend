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

use databend_common_base::rangemap::RangeMerger;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_metrics::storage::metrics_inc_remote_io_read_bytes;
use databend_common_metrics::storage::metrics_inc_remote_io_read_parts;
use databend_storages_common_io::MergeIOReadResult;
use databend_storages_common_io::OwnerMemory;
use databend_storages_common_io::RangeReader;
use databend_storages_common_io::ReadSettings;
use opendal::Buffer;

use super::BlockReadContext;
use super::BlockReadResult;
use crate::FuseBlockPartInfo;
use crate::io::GranuleRangeBounds;
use crate::io::OffsetsIndex;
use crate::io::create_file_range_reader;

const GRANULE_IO_RANGE_SIZE: u64 = 16 * 1024 * 1024;

enum GranuleColumnOutput {
    Empty,
    Slice {
        range: Range<u64>,
        sub: Range<usize>,
    },
}

fn collect_ranges(groups: &[Vec<Range<usize>>]) -> Vec<Range<usize>> {
    let mut result = Vec::new();
    for group in groups {
        for range in group {
            result.push(range.clone());
        }
    }
    result
}

fn load_bounds(
    offsets: &OffsetsIndex,
    part: &FuseBlockPartInfo,
    ranges: &[Range<usize>],
) -> Result<Vec<GranuleRangeBounds>> {
    let mut result = Vec::with_capacity(ranges.len());
    for range in ranges {
        result.push(offsets.ranges_for_granules(
            &part.columns_meta,
            range.clone(),
            part.nums_rows,
        )?);
    }
    Ok(result)
}

fn block_file_len(part: &FuseBlockPartInfo) -> u64 {
    let mut result = 0;
    for meta in part.columns_meta.values() {
        let (offset, len) = meta.offset_length();
        result = result.max(offset.saturating_add(len));
    }
    result
}

fn column_byte_ranges(
    column_id: ColumnId,
    bounds: &[GranuleRangeBounds],
) -> Result<(bool, Vec<Range<u64>>)> {
    let mut dictionary = None;
    let mut result = Vec::with_capacity(bounds.len() + 1);

    for (index, bounds) in bounds.iter().enumerate() {
        let column = bounds
            .columns
            .iter()
            .find(|column| column.column_id == column_id);
        let column = match column {
            Some(column) => column,
            None => {
                return Err(ErrorCode::Internal(format!(
                    "granule offset index has no bounds for projected column {column_id}"
                )));
            }
        };

        if index == 0 {
            dictionary = column.dict_range.clone();
            if let Some(range) = &dictionary {
                result.push(range.clone());
            }
        } else if column.dict_range != dictionary {
            return Err(ErrorCode::Internal(format!(
                "granule data bounds disagree on dictionary range for column {column_id}"
            )));
        }

        result.push(column.data_range.clone());
    }

    Ok((dictionary.is_some(), result))
}

fn merge_column_ranges(
    column_id: ColumnId,
    input_ranges: &[Range<u64>],
    max_gap_size: u64,
) -> Result<(VecDeque<GranuleColumnOutput>, VecDeque<Range<u64>>)> {
    let mut non_empty = Vec::with_capacity(input_ranges.len());
    for range in input_ranges {
        if !range.is_empty() {
            non_empty.push(range.clone());
        }
    }

    let merger = RangeMerger::from_iter(non_empty, max_gap_size, GRANULE_IO_RANGE_SIZE);
    let merged_ranges = merger.ranges();
    let mut outputs = VecDeque::with_capacity(input_ranges.len());
    let mut previous_start = None;

    for input in input_ranges {
        if input.is_empty() {
            outputs.push_back(GranuleColumnOutput::Empty);
            continue;
        }

        let merged = merger.get(input.clone());
        let (_, range) = match merged {
            Some(value) => value,
            None => {
                return Err(ErrorCode::Internal(format!(
                    "granule range {input:?} not found in merged ranges {merged_ranges:?}"
                )));
            }
        };

        if let Some(start) = previous_start {
            if range.start < start {
                return Err(ErrorCode::Internal(format!(
                    "granule column {column_id} ranges are not in file order"
                )));
            }
        }
        previous_start = Some(range.start);

        let start = (input.start - range.start) as usize;
        let end = (input.end - range.start) as usize;
        outputs.push_back(GranuleColumnOutput::Slice {
            range,
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
    fetch_part_num: usize,
    current: Option<(Range<u64>, Buffer)>,
}

impl GranuleColumnReader {
    fn try_create(
        column_id: ColumnId,
        mut reader: Box<dyn RangeReader>,
        input_ranges: &[Range<u64>],
        has_dictionary: bool,
        max_gap_size: u64,
        fetch_part_num: usize,
    ) -> Result<Self> {
        let (outputs, ranges) = merge_column_ranges(column_id, input_ranges, max_gap_size)?;
        let fetch_part_num = fetch_part_num.max(1);

        for range in ranges.iter().take(fetch_part_num) {
            let _ = reader.prefetch(std::slice::from_ref(range));
        }

        Ok(Self {
            column_id,
            has_dictionary,
            dictionary: None,
            reader,
            outputs,
            ranges,
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

        let buffer = self.reader.read(expected.clone())?;
        self.current = Some((expected, buffer));
        self.prefetch_next();
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
    ranges: VecDeque<(Range<usize>, GranuleRangeBounds)>,
    column_readers: Vec<GranuleColumnReader>,
}

impl GranuleDataReader {
    pub(crate) fn create(
        read_context: &BlockReadContext,
        settings: &ReadSettings,
        part: &FuseBlockPartInfo,
        groups: &[Vec<Range<usize>>],
        offsets: &OffsetsIndex,
    ) -> Result<Self> {
        let ranges = collect_ranges(groups);
        let bounds = load_bounds(offsets, part, &ranges)?;
        let file_len = block_file_len(part);
        let fetch_part_num = read_context.storage_fetch_part_num()?.max(1);
        let range_size = usize::try_from(GRANULE_IO_RANGE_SIZE).unwrap_or(usize::MAX);
        let held_budget = range_size.saturating_mul(fetch_part_num.saturating_add(2));

        let mut column_readers = Vec::new();
        for (column_id, ..) in read_context.project_indices().values() {
            let (has_dictionary, byte_ranges) = column_byte_ranges(*column_id, &bounds)?;
            record_remote_bytes(&byte_ranges);

            let reader = create_file_range_reader(
                read_context.operator().clone(),
                part.location.clone(),
                file_len,
                fetch_part_num,
                GRANULE_IO_RANGE_SIZE,
                held_budget,
            )?;
            let reader = GranuleColumnReader::try_create(
                *column_id,
                reader,
                &byte_ranges,
                has_dictionary,
                settings.max_gap_size,
                fetch_part_num,
            )?;
            column_readers.push(reader);
        }

        let ranges = ranges.into_iter().zip(bounds).collect();
        Ok(Self {
            location: part.location.clone(),
            ranges,
            column_readers,
        })
    }

    pub(crate) fn read_next(&mut self) -> Result<Option<GranuleRangeRead>> {
        let (granule_range, bounds) = match self.ranges.pop_front() {
            Some(item) => item,
            None => return Ok(None),
        };

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
        let data = BlockReadResult::create_with_row_range(result, bounds.row_range);
        Ok(Some(GranuleRangeRead {
            range: granule_range,
            data,
        }))
    }
}
