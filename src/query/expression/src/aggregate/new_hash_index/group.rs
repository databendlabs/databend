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
    if #[cfg(not(miri))] {
        pub(super) use self::portable_simd::Group;
    } else{
        pub(super) use self::generic::Group;

    }
}

mod portable_simd {
    use std::simd::cmp::SimdPartialEq;
    use std::simd::num::SimdInt;
    use std::simd::num::SimdUint;
    use std::simd::u16x4;

    use crate::aggregate::new_hash_index::bitmask::BitMask;
    use crate::aggregate::new_hash_index::bitmask::Tag;

    #[derive(Copy, Clone)]
    pub struct Group(u16x4);

    #[inline]
    fn u16x4_to_u64(values: [u16; 4]) -> u64 {
        unsafe { std::mem::transmute(values) }
    }

    impl Group {
        pub const WIDTH: usize = u16x4::LEN;

        #[inline]
        pub fn match_tag(self, tag: Tag) -> BitMask {
            let cmp = self.0.simd_eq(u16x4::splat(tag.0));
            let vec_mask = cmp.to_int().cast::<u16>();
            BitMask(u16x4_to_u64(vec_mask.to_array()))
        }

        #[inline]
        pub fn match_empty(self) -> BitMask {
            let mask = self.0.cast::<i16>().is_negative();
            let vec_mask = mask.to_int().cast::<u16>();
            BitMask(u16x4_to_u64(vec_mask.to_array()))
        }

        #[inline]
        pub unsafe fn load(ctrls: &[Tag], index: usize) -> Self {
            let ptr = unsafe { ctrls.as_ptr().add(index) as *const u16 };
            Group(u16x4::from_array(unsafe {
                ptr.cast::<[u16; 4]>().read_unaligned()
            }))
        }
    }

    #[cfg(all(test, not(miri)))]
    mod tests {
        use rand::RngCore;

        use super::Group;
        use super::u16x4_to_u64;
        use crate::aggregate::new_hash_index::bitmask::Tag;

        #[test]
        fn test_fuzz_portable_simd() {
            let mut rng = rand::thread_rng();

            for _ in 0..4096 {
                let mut bytes = [0u8; 8];
                rng.fill_bytes(&mut bytes);
                let words = [
                    u16::from_ne_bytes([bytes[0], bytes[1]]),
                    u16::from_ne_bytes([bytes[2], bytes[3]]),
                    u16::from_ne_bytes([bytes[4], bytes[5]]),
                    u16::from_ne_bytes([bytes[6], bytes[7]]),
                ];

                let ctrls = words.map(Tag);
                let group = unsafe { Group::load(&ctrls, 0) };

                let tag = Tag((rng.next_u32() & 0xFFFF) as u16);
                let mut expected_tag_words = [0u16; 4];
                let mut expected_empty_words = [0u16; 4];

                for i in 0..4 {
                    if ctrls[i].0 == tag.0 {
                        expected_tag_words[i] = 0xFFFF;
                    }
                    if (ctrls[i].0 as i16) < 0 {
                        expected_empty_words[i] = 0xFFFF;
                    }
                }

                let expected_tag = u16x4_to_u64(expected_tag_words);
                let expected_empty = u16x4_to_u64(expected_empty_words);

                assert_eq!(
                    group.match_tag(tag).0,
                    expected_tag,
                    "ctrls={:?} tag=0x{:04X} expected_tag_words={:?}",
                    words,
                    tag.0,
                    expected_tag_words
                );
                assert_eq!(
                    group.match_empty().0,
                    expected_empty,
                    "ctrls={:?} expected_empty_words={:?}",
                    words,
                    expected_empty_words
                );
            }
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
        /// Number of tags in the group.
        pub const WIDTH: usize = 4;

        #[inline]
        pub fn match_tag(self, tag: Tag) -> BitMask {
            // This algorithm is derived from
            // https://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
            let cmp = self.0 ^ repeat(tag);
            BitMask((cmp.wrapping_sub(repeat(Tag(0x0001))) & !cmp & repeat(Tag(0x8000))).to_le())
        }

        #[inline]
        pub fn match_empty(self) -> BitMask {
            BitMask((self.0 & repeat(Tag(0x8000))).to_le())
        }

        #[inline]
        pub unsafe fn load(ctrls: &[Tag], index: usize) -> Self {
            unsafe { Group((ctrls.as_ptr().add(index) as *const u64).read_unaligned()) }
        }
    }
}
