// Copyright (c) 2016 Amanieu d'Antras
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

use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(all(
    target_arch = "aarch64",
    target_feature = "neon",
    // NEON intrinsics are currently broken on big-endian targets.
    // See https://github.com/rust-lang/stdarch/issues/1484.
    target_endian = "little",
    not(miri),
))] {
        pub(super) use self::neon::Group;
    } else{
        pub(super) use self::generic::Group;

    }
}

#[cfg(all(
    target_arch = "aarch64",
    target_feature = "neon",
    // NEON intrinsics are currently broken on big-endian targets.
    // See https://github.com/rust-lang/stdarch/issues/1484.
    target_endian = "little",
    not(miri),
))]
mod neon {
    use core::arch::aarch64 as neon;
    use std::mem;

    use crate::aggregate::new_hash_index::bitmask::BitMask;
    use crate::aggregate::new_hash_index::bitmask::Tag;

    #[derive(Copy, Clone)]
    pub struct Group(neon::uint8x8_t);

    impl Group {
        /// Number of bytes in the group.
        pub const WIDTH: usize = mem::size_of::<Self>();

        #[inline]
        pub fn match_tag(self, tag: Tag) -> BitMask {
            unsafe {
                let cmp = neon::vceq_u8(self.0, neon::vdup_n_u8(tag.0));
                BitMask(neon::vget_lane_u64(neon::vreinterpret_u64_u8(cmp), 0))
            }
        }

        #[inline]
        pub fn match_empty(self) -> BitMask {
            unsafe {
                let cmp = neon::vcltz_s8(neon::vreinterpret_s8_u8(self.0));
                BitMask(neon::vget_lane_u64(neon::vreinterpret_u64_u8(cmp), 0))
            }
        }

        #[inline]
        pub unsafe fn load(ctrls: &[Tag], index: usize) -> Self {
            unsafe { Group(neon::vld1_u8(ctrls.as_ptr().add(index) as *const u8)) }
        }
    }
}

mod generic {
    use crate::aggregate::new_hash_index::bitmask::BitMask;
    use crate::aggregate::new_hash_index::bitmask::Tag;
    use crate::aggregate::new_hash_index::bitmask::repeat;

    #[derive(Copy, Clone)]
    pub struct Group(u64);

    impl Group {
        /// Number of bytes in the group.
        pub const WIDTH: usize = 8;

        #[inline]
        pub fn match_tag(self, tag: Tag) -> BitMask {
            // This algorithm is derived from
            // https://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
            let cmp = self.0 ^ repeat(tag);
            BitMask((cmp.wrapping_sub(repeat(Tag(0x01))) & !cmp & repeat(Tag(0x80))).to_le())
        }

        #[inline]
        pub fn match_empty(self) -> BitMask {
            BitMask((self.0 & repeat(Tag(0x80))).to_le())
        }

        #[inline]
        pub unsafe fn load(ctrls: &[Tag], index: usize) -> Self {
            unsafe { Group((ctrls.as_ptr().add(index) as *const u64).read_unaligned()) }
        }
    }
}
