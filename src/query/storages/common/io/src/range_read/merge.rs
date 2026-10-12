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
use std::ops::Range;

use databend_common_base::rangemap::RangeMerger;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use opendal::Buffer;

use super::RangeReader;
use crate::ReadSettings;

struct MergedReadSlot {
    data: Option<Buffer>,
    hints: usize,
}

/// Merge known logical ranges without coupling storage request boundaries to
/// consumption windows. This layer owns the logical-to-storage mapping. It
/// retains hinted segments and at most one unhinted, recently consumed segment.
/// Neither cache admission nor remote fetching belongs to this layer.
pub struct MergeRangeReader<R: RangeReader> {
    next: R,
    segments: Vec<Range<u64>>,
    hints: HashMap<Range<u64>, usize>,
    slots: HashMap<Range<u64>, MergedReadSlot>,
    recent: Option<(Range<u64>, Buffer)>,
    max_segments: usize,
}

impl<R: RangeReader> MergeRangeReader<R> {
    pub fn new(
        next: R,
        ranges: &[Range<u64>],
        settings: &ReadSettings,
        max_segments: usize,
    ) -> Result<Self> {
        if max_segments == 0 {
            return Err(ErrorCode::BadArguments(
                "merge range reader requires positive limits",
            ));
        }
        if ranges.iter().any(|range| range.start > range.end) {
            return Err(ErrorCode::BadArguments("inverted merge input range"));
        }
        let merger = RangeMerger::from_iter(
            ranges.iter().filter(|range| !range.is_empty()).cloned(),
            settings.max_gap_size,
            settings.max_range_size,
        );
        Ok(Self {
            next,
            segments: merger.ranges(),
            hints: HashMap::new(),
            slots: HashMap::new(),
            recent: None,
            max_segments,
        })
    }

    fn segment(&self, range: &Range<u64>) -> Result<Range<u64>> {
        let index = self
            .segments
            .partition_point(|segment| segment.end <= range.start);
        match self.segments.get(index) {
            Some(segment) if segment.start <= range.start && range.end <= segment.end => {
                Ok(segment.clone())
            }
            _ => Err(ErrorCode::BadArguments(
                "range is outside the merge reader's input plan",
            )),
        }
    }

    fn retire_hint(&mut self, range: &Range<u64>) -> bool {
        let Some(uses) = self.hints.get_mut(range) else {
            return false;
        };
        *uses -= 1;
        if *uses == 0 {
            self.hints.remove(range);
        }
        true
    }
}

impl<R: RangeReader> RangeReader for MergeRangeReader<R> {
    fn prefetch(&mut self, ranges: &[Range<u64>]) -> bool {
        for range in ranges {
            if range.is_empty() {
                continue;
            }
            let Ok(segment) = self.segment(range) else {
                return false;
            };
            let mut downstream_ready = true;
            if !self.slots.contains_key(&segment) {
                if self.slots.len() >= self.max_segments {
                    return false;
                }
                let data = match self.recent.take() {
                    Some((recent, data)) if recent == segment => Some(data),
                    other => {
                        self.recent = other;
                        None
                    }
                };
                if data.is_none() {
                    // A false return may mean the final hint was accepted and
                    // filled the downstream capacity. Correctness uses read's
                    // on-demand path either way; discard is safe for both cases.
                    downstream_ready = self.next.prefetch(std::slice::from_ref(&segment));
                }
                self.slots
                    .insert(segment.clone(), MergedReadSlot { data, hints: 0 });
            }
            self.slots.get_mut(&segment).unwrap().hints += 1;
            *self.hints.entry(range.clone()).or_default() += 1;
            if !downstream_ready {
                // Preserve downstream saturation even when this layer has spare
                // slots. Later hints must not leap over this segment.
                return false;
            }
        }
        self.slots.len() < self.max_segments
    }

    fn discard(&mut self, range: Range<u64>) {
        if !self.retire_hint(&range) {
            return;
        }
        let segment = self.segment(&range).expect("hinted range has a segment");
        let slot = self.slots.get_mut(&segment).expect("hinted segment");
        slot.hints -= 1;
        if slot.hints != 0 {
            return;
        }
        let slot = self.slots.remove(&segment).unwrap();
        if let Some(data) = slot.data {
            self.recent = Some((segment, data));
        } else {
            self.next.discard(segment);
        }
    }

    fn read(&mut self, range: Range<u64>) -> Result<Buffer> {
        if range.start > range.end {
            return Err(ErrorCode::BadArguments("inverted merge read range"));
        }
        if range.is_empty() {
            return Ok(Buffer::new());
        }
        let segment = self.segment(&range)?;
        let hinted = self.hints.contains_key(&range);
        let data = match self.slots.get_mut(&segment) {
            Some(slot) => {
                if slot.data.is_none() {
                    slot.data = Some(self.next.read(segment.clone())?);
                }
                if hinted {
                    slot.hints -= 1;
                }
                slot.data.as_ref().unwrap().clone()
            }
            None => match &self.recent {
                Some((recent, data)) if recent == &segment => data.clone(),
                _ => self.next.read(segment.clone())?,
            },
        };
        if hinted {
            self.retire_hint(&range);
        }
        if self.slots.get(&segment).is_none_or(|slot| slot.hints == 0) {
            self.slots.remove(&segment);
            self.recent = Some((segment.clone(), data.clone()));
        }
        if data.len() as u64 != segment.end - segment.start {
            return Err(ErrorCode::StorageOther("truncated merged read"));
        }
        let start = (range.start - segment.start) as usize;
        let end = (range.end - segment.start) as usize;
        Ok(data.slice(start..end))
    }
}

impl<R: RangeReader> Drop for MergeRangeReader<R> {
    fn drop(&mut self) {
        for (segment, slot) in &self.slots {
            if slot.data.is_none() {
                self.next.discard(segment.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init_test_runtime;
    use crate::range_read::OperatorRangeReader;
    use crate::range_read::test_util::RecordingReadAccessor;
    use crate::range_read::test_util::recording_operator;
    use crate::range_read::test_util::settings;

    #[test]
    fn test_merged_windows_and_duplicate_hints() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789abcdef", false);
        let tail = OperatorRangeReader::new(recording_operator(accessor.clone()), "data".into(), 2);
        let mut reader =
            MergeRangeReader::new(tail, &[0..4, 4..8, 8..12, 12..16], &settings(0, 8), 2).unwrap();
        reader.prefetch(&[0..4, 4..8, 0..4]);
        reader.discard(0..4);
        assert_eq!(reader.read(0..4).unwrap().to_bytes().as_ref(), b"0123");
        assert_eq!(reader.read(4..8).unwrap().to_bytes().as_ref(), b"4567");
        assert_eq!(reader.read(8..12).unwrap().to_bytes().as_ref(), b"89ab");
        assert_eq!(reader.read(12..16).unwrap().to_bytes().as_ref(), b"cdef");
        assert_eq!(accessor.read_ranges(), vec![0..8, 8..16]);
    }

    #[test]
    fn test_merge_layer_over_stream_tail_keeps_one_backend_request() {
        use std::io::Read;

        use crate::range_read::ChunkedRangeReader;
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789abcdef", false);
        let tail = OperatorRangeReader::new_streaming(
            recording_operator(accessor.clone()),
            "data".into(),
            0..16,
            2,
        )
        .unwrap();
        let merge =
            MergeRangeReader::new(tail, &[0..4, 4..8, 8..12, 12..16], &settings(0, 8), 2).unwrap();
        let mut reader = ChunkedRangeReader::with_range(Box::new(merge), 0..16, 4, 1).unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, b"0123456789abcdef");
        assert_eq!(accessor.read_ranges(), vec![0..16]);
    }

    #[test]
    fn test_boxed_layers_and_zero_merge_limit() {
        use std::io::Read;

        use crate::range_read::ChunkedRangeReader;
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789abcdef", false);
        let tail: Box<dyn RangeReader> = Box::new(
            OperatorRangeReader::new_streaming(
                recording_operator(accessor.clone()),
                "data".into(),
                0..16,
                2,
            )
            .unwrap(),
        );
        // A zero merge limit disables merging, not streaming or correctness.
        let layer: Box<dyn RangeReader> = Box::new(
            MergeRangeReader::new(tail, &[0..4, 4..8, 8..12, 12..16], &settings(0, 0), 2).unwrap(),
        );
        let mut reader = ChunkedRangeReader::with_range(layer, 0..16, 4, 2).unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, b"0123456789abcdef");
        assert_eq!(accessor.read_ranges(), vec![0..16]);
    }

    #[test]
    fn test_stream_order_survives_partial_prefetch_acceptance() {
        use std::io::Read;

        use crate::range_read::ChunkedRangeReader;

        init_test_runtime();
        for lookahead in 1..=4 {
            for capacity in 1..=3 {
                for merge_limit in [0, 4, 8, 16] {
                    let accessor = RecordingReadAccessor::new(b"0123456789abcdef", false);
                    let tail = OperatorRangeReader::new_streaming(
                        recording_operator(accessor.clone()),
                        "data".into(),
                        0..16,
                        capacity,
                    )
                    .unwrap();
                    let merge = MergeRangeReader::new(
                        tail,
                        &[0..4, 4..8, 8..12, 12..16],
                        &settings(0, merge_limit),
                        capacity + 1,
                    )
                    .unwrap();
                    let mut reader =
                        ChunkedRangeReader::with_range(Box::new(merge), 0..16, 4, lookahead)
                            .unwrap();
                    let mut actual = Vec::new();
                    reader.read_to_end(&mut actual).unwrap();
                    assert_eq!(actual, b"0123456789abcdef");
                    assert_eq!(
                        accessor.read_ranges(),
                        vec![0..16],
                        "lookahead={lookahead}, capacity={capacity}, merge_limit={merge_limit}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_failed_hinted_read_can_be_discarded() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"01234567", true);
        let tail = OperatorRangeReader::new(recording_operator(accessor), "data".into(), 1);
        let mut reader = MergeRangeReader::new(tail, &[0..4, 4..8], &settings(0, 8), 1).unwrap();
        reader.prefetch(&[0..4, 4..8]);
        assert!(reader.read(0..4).is_err());
        reader.discard(0..4);
        reader.discard(4..8);
        assert!(reader.hints.is_empty());
        assert!(reader.slots.is_empty());
    }

    #[test]
    fn test_rejected_hints_discard_and_short_response() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789abcdef", false);
        let tail = OperatorRangeReader::new(recording_operator(accessor.clone()), "data".into(), 1);
        let mut reader = MergeRangeReader::new(tail, &[0..4, 8..12], &settings(0, 4), 1).unwrap();
        assert!(!reader.prefetch(&[0..4, 8..12]));
        reader.discard(0..4);
        assert_eq!(reader.read(8..12).unwrap().to_bytes().as_ref(), b"89ab");
        assert!(reader.read(4..6).is_err());
        let inverted = Range { start: 5, end: 4 };
        assert!(reader.read(inverted).is_err());
        assert!(reader.read(4..4).unwrap().is_empty());
        let accessor = RecordingReadAccessor::new(b"01234567", true);
        let tail = OperatorRangeReader::new(recording_operator(accessor), "data".into(), 1);
        let mut reader = MergeRangeReader::new(tail, &[0..4, 4..8], &settings(0, 8), 1).unwrap();
        assert!(reader.read(0..4).is_err());
    }
}
