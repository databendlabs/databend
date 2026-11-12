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

//! Reads a row mask one 64-row word at a time, for kernels that pick rows by
//! a bitmap (`filter`, the row selection of `if`).
//!
//! Consecutive words that are all set or all unset are merged into a run,
//! which a kernel copies (or skips) in bulk; a word with both set and unset
//! bits is passed on as is.

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::utils::BitChunkIterExact;
use databend_common_column::bitmap::utils::BitChunksExact;

/// A part of the mask: a run of whole 64-row words whose bits are all set
/// (`set`) or all unset, or a single word with both set and unset bits.
pub(crate) enum Segment {
    Run { set: bool, start: usize, len: usize },
    Mixed { word: u64, start: usize, len: usize },
}

/// Calls `f` for the segments of `mask` in row order. Bit `i` of a mixed word
/// is the row `start + i`; its bits at and above `len` are unset.
pub(crate) fn for_each_segment(mask: &Bitmap, f: impl FnMut(Segment)) {
    let mut segments = SegmentBuilder {
        f,
        start: 0,
        run: None,
    };
    let (bytes, offset, len) = mask.as_slice();
    if offset == 0 {
        segments.push_chunks(BitChunksExact::<u64>::new(bytes, len));
    } else {
        segments.push_chunks(mask.chunks::<u64>());
    }
    segments.flush_run();
}

struct SegmentBuilder<F: FnMut(Segment)> {
    f: F,
    /// The first row of the next word.
    start: usize,
    /// The pending run: `(set, start, len)`.
    run: Option<(bool, usize, usize)>,
}

impl<F: FnMut(Segment)> SegmentBuilder<F> {
    fn push_chunks(&mut self, mut chunks: impl BitChunkIterExact<u64>) {
        for word in chunks.by_ref() {
            self.push_word(word, 64);
        }
        let remainder_len = chunks.remainder_len();
        if remainder_len > 0 {
            self.push_word(chunks.remainder(), remainder_len);
        }
    }

    #[inline]
    fn push_word(&mut self, word: u64, len: usize) {
        let full = if len == 64 { u64::MAX } else { (1 << len) - 1 };
        let word = word & full;
        let start = self.start;
        self.start += len;

        let set = if word == 0 {
            false
        } else if word == full {
            true
        } else {
            self.flush_run();
            (self.f)(Segment::Mixed { word, start, len });
            return;
        };
        match &mut self.run {
            Some((run_set, _, run_len)) if *run_set == set => *run_len += len,
            _ => {
                self.flush_run();
                self.run = Some((set, start, len));
            }
        }
    }

    #[inline]
    fn flush_run(&mut self) {
        if let Some((set, start, len)) = self.run.take() {
            (self.f)(Segment::Run { set, start, len });
        }
    }
}
