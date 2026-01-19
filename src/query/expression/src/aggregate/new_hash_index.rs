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

/// Single tag in a control group.
#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(transparent)]
struct Tag(u8);
impl Tag {
    /// Control tag value for an empty bucket.
    const EMPTY: Tag = Tag(0b1111_1111);

    /// Creates a control tag representing a full bucket with the given hash.
    #[inline]
    const fn full(hash: u64) -> Tag {
        let top7 = hash >> (8 * 8 - 7);
        Tag((top7 & 0x7f) as u8) // truncation
    }
}

#[derive(Copy, Clone)]
struct BitMask(u64);

impl BitMask {}

/// Helper function to replicate a tag across a `GroupWord`.
#[inline]
fn repeat(tag: Tag) -> u64 {
    u64::from_ne_bytes([tag.0; Group::WIDTH])
}

#[derive(Copy, Clone)]
struct Group(u64);

impl Group {
    /// Number of bytes in the group.
    const WIDTH: usize = 8;

    fn match_tag(self, tag: Tag) -> BitMask {
        // This algorithm is derived from
        // https://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
        let cmp = self.0 ^ repeat(tag);
        BitMask((cmp.wrapping_sub(repeat(Tag(0x01))) & !cmp & repeat(Tag(0x80))).to_le())
    }
}
