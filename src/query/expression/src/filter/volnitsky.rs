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

// Rust implementation of Volnitsky's substring search algorithm
// Based from cpp version: https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/Volnitsky.h

use std::borrow::Cow;
use std::mem;

use databend_common_hashtable::fast_memcmp;

const HASH_SIZE: usize = 64 * 1024; // Size of the hash table

type Offset = u8; // Offset in the needle, defaults to 0
type NGram = u16; // N-gram (2 bytes)

#[derive(Debug, Clone)]
pub struct VolnitskyBase<'a> {
    needle: Cow<'a, [u8]>,
    needle_size: usize,
    hash: Option<Box<[Offset; HASH_SIZE]>>,
    step: usize,
    fallback_searcher: memchr::arch::all::rabinkarp::Finder,
}

impl PartialEq for VolnitskyBase<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.needle.as_ref() == other.needle.as_ref()
    }
}

impl<'a> VolnitskyBase<'a> {
    pub fn new(needle: &'a [u8], haystack_size_hint: usize) -> Self {
        Self::new_cow(Cow::Borrowed(needle), haystack_size_hint)
    }

    pub fn new_cow(needle: Cow<'a, [u8]>, haystack_size_hint: usize) -> Self {
        let needle_size = needle.len();

        let fallback = needle_size < 2 * mem::size_of::<NGram>()
            || needle_size > u8::MAX as usize
            || (haystack_size_hint > 0 && haystack_size_hint < 20000);

        let hash = if !fallback {
            let mut hash = Box::new([0; HASH_SIZE]);
            for i in (0..=needle_size - mem::size_of::<NGram>()).rev() {
                let ngram = Self::to_ngram(&needle[i..i + mem::size_of::<NGram>()]);
                Self::put_ngram(hash.as_mut(), ngram, i as u8 + 1);
            }
            Some(hash)
        } else {
            None
        };

        let fallback_searcher = memchr::arch::all::rabinkarp::Finder::new(needle.as_ref());
        Self {
            needle,
            needle_size,
            step: (needle_size + 1).wrapping_sub(mem::size_of::<NGram>()),
            hash,
            fallback_searcher,
        }
    }

    #[inline]
    fn to_ngram(pos: &[u8]) -> NGram {
        unsafe { (pos.as_ptr() as *const u16).read_unaligned() }
    }

    fn put_ngram(hash: &mut [Offset], ngram: NGram, offset: Offset) {
        let mut cell_num = (ngram as usize) % HASH_SIZE;
        while hash[cell_num] != 0 {
            cell_num = (cell_num + 1) % HASH_SIZE;
        }
        hash[cell_num] = offset;
    }

    pub fn needle(&self) -> &[u8] {
        self.needle.as_ref()
    }

    #[inline]
    pub fn search(&self, haystack: &[u8]) -> Option<usize> {
        if self.needle_size == 0 {
            return Some(0);
        }

        let haystack_size = haystack.len();
        if self.hash.is_none() || haystack_size <= self.needle_size {
            return self.fallback_search(haystack);
        }

        let mut pos = self.needle_size - mem::size_of::<NGram>();
        let hash = self.hash.as_ref().unwrap().as_ref();
        let needle = self.needle.as_ref();
        while pos <= haystack_size - self.needle_size {
            let ngram = Self::to_ngram(&haystack[pos..pos + mem::size_of::<NGram>()]);
            let mut cell_num = (ngram as usize) % HASH_SIZE;
            while hash[cell_num] > 0 {
                let res = pos - (hash[cell_num] as usize - 1);
                if fast_memcmp(&haystack[res..res + self.needle_size], needle) {
                    return Some(res);
                }
                cell_num = (cell_num + 1) % HASH_SIZE;
            }
            pos += self.step;
        }

        // Each probe covers `step` consecutive candidate starts. Search only after the last
        // candidate start covered by the final probe.
        let fallback_start = pos - (self.step - 1);
        self.fallback_search(&haystack[fallback_start..])
            .map(|match_pos| fallback_start + match_pos)
    }

    fn fallback_search(&self, haystack: &[u8]) -> Option<usize> {
        if haystack.len() < 64 {
            self.fallback_searcher.find(haystack, self.needle.as_ref())
        } else {
            memchr::memmem::find(haystack, self.needle.as_ref())
        }
    }
}

// Test case for the Volnitsky search algorithm
#[test]
fn test_volnitsky_search() {
    struct TestCase {
        needle: &'static [u8],
        haystack: &'static [u8],
        pos: Option<usize>,
    }

    impl TestCase {
        fn new(needle: &'static str, haystack: &'static str, pos: Option<usize>) -> Self {
            Self {
                needle: needle.as_bytes(),
                haystack: haystack.as_bytes(),
                pos,
            }
        }
    }

    for case in [
        TestCase::new("test", "This is a test string for testing.", Some(10)),
        TestCase::new("google", "687502fdsfsgooglefdafdsf", Some(11)),
        TestCase::new("notfound", "687502fdsfsgooglefdafdsf", None),
    ] {
        let searcher = VolnitskyBase::new(case.needle, 0);
        let result = searcher.search(case.haystack);
        assert_eq!(result, case.pos);
    }

    let mut string = String::new();
    for i in 10..100 {
        string.push_str(&format!("{i}fdsfsgooglefdafdsf"));
    }
    let haystack = string.as_bytes();
    let needle = "google";
    let searcher = VolnitskyBase::new(needle.as_bytes(), 0);
    let step = "fdsfsgooglefdafdsf".len() + 2;

    for i in 10..100 {
        let pos = searcher.search(haystack[(i - 10) * step..].as_ref());
        assert_eq!(pos, Some(7), "Failed at iteration {}", i);
    }
}

#[test]
fn test_volnitsky_fallback_boundaries() {
    let needle = b"needle";
    let searcher = VolnitskyBase::new(needle, 0);

    // The probe loop does not run for a haystack only one byte longer than the needle, so the
    // fallback must still search the complete haystack.
    assert_eq!(searcher.search(b"xneedle"), Some(1));

    // For a 101-byte haystack, the last probe covers candidate starts through 94. The match at
    // the only remaining candidate start must be found by the suffix fallback.
    let mut tail_match = vec![b'x'; 95];
    tail_match.extend_from_slice(needle);
    assert_eq!(searcher.search(&tail_match), Some(95));

    // Candidate 5 partially matches "aabc" before failing the full comparison. The overlapping
    // match at the first fallback candidate, 6, must still be found.
    let searcher = VolnitskyBase::new(b"aabc", 0);
    assert_eq!(searcher.search(b"xxxxxaaabc"), Some(6));

    // When matches overlap on both sides of the boundary, the earlier probe-covered match must
    // win over the fallback match.
    let searcher = VolnitskyBase::new(b"aaaa", 0);
    assert_eq!(searcher.search(b"xxaaaaa"), Some(2));
}

#[test]
fn test_volnitsky_matches_across_probe_boundaries() {
    for needle_size in [4, 5, 6, 7, 16, 31, 64, 127, 255] {
        let distinct: Vec<u8> = (1..=u8::try_from(needle_size).unwrap()).collect();
        let repeated = vec![1; needle_size];

        for (pattern, needle) in [("distinct", &distinct), ("repeated", &repeated)] {
            let searcher = VolnitskyBase::new(needle, 0);
            assert!(searcher.hash.is_some());
            let step = searcher.step;

            // A probe covers `step` candidate starts. Exercise every possible number of fallback
            // candidates after zero through four complete probe blocks.
            for probe_count in 0..=4 {
                for fallback_candidates in 0..step {
                    let covered_candidates = probe_count * step;
                    let candidate_count = covered_candidates + fallback_candidates;
                    let haystack_size = needle_size - 1 + candidate_count;

                    let context = format!(
                        "needle_size={needle_size}, pattern={pattern}, probe_count={probe_count}, \
                         fallback_candidates={fallback_candidates}"
                    );
                    let check_match = |match_start| {
                        let mut haystack = vec![0; haystack_size];
                        haystack[match_start..match_start + needle_size].copy_from_slice(needle);
                        let expected = memchr::memmem::find(&haystack, needle);
                        assert_eq!(expected, Some(match_start), "invalid fixture: {context}");
                        assert_eq!(searcher.search(&haystack), expected, "{context}");
                    };

                    // Negative searches exercise the fallback even when it has no candidates.
                    let no_match = vec![0; haystack_size];
                    assert_eq!(
                        searcher.search(&no_match),
                        memchr::memmem::find(&no_match, needle),
                        "{context}"
                    );

                    // Check the outer bounds and both sides of the probe/fallback boundary.
                    for match_start in [
                        (candidate_count > 0).then_some(0),
                        (covered_candidates > 0).then(|| covered_candidates - 1),
                        (fallback_candidates > 0).then_some(covered_candidates),
                        (candidate_count > 0).then(|| candidate_count - 1),
                    ]
                    .into_iter()
                    .flatten()
                    {
                        check_match(match_start);
                    }
                }
            }

            // Also check every candidate start in one haystack spanning multiple probes and the
            // maximum possible fallback tail.
            let probe_count = 4;
            let fallback_candidates = step - 1;
            let candidate_count = probe_count * step + fallback_candidates;
            let haystack_size = needle_size - 1 + candidate_count;
            for match_start in 0..candidate_count {
                let mut haystack = vec![0; haystack_size];
                haystack[match_start..match_start + needle_size].copy_from_slice(needle);
                assert_eq!(
                    searcher.search(&haystack),
                    memchr::memmem::find(&haystack, needle),
                    "needle_size={needle_size}, pattern={pattern}, match_start={match_start}"
                );
            }
        }
    }
}

#[test]
fn test_volnitsky_userid() {
    let mut string = String::new();
    let start = 38575348285873298;
    for i in start..start + 1000 {
        string.push_str(&format!("{i}xxgooglex"));
    }
    let haystack = string.as_bytes();
    let needle = "google";
    let searcher = VolnitskyBase::new(needle.as_bytes(), 0);
    let step = "xxgooglex".len() + format!("{start}").as_bytes().len();

    for i in start..start + 1000 {
        let pos = searcher.search(haystack[(i - start) * step..].as_ref());
        assert_eq!(
            pos,
            Some(format!("{start}").as_bytes().len() + 2),
            "Failed at iteration {}",
            i
        );
    }
}

#[cfg(test)]
mod property_tests {
    use proptest::prelude::*;

    use super::*;

    // Use `memmem::find` as the correctness oracle. These properties verify that Volnitsky
    // returns the same first-match offset for arbitrary inputs and forced match positions.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn test_volnitsky_matches_memmem(
            needle in prop::collection::vec(any::<u8>(), 0..301),
            haystack in prop::collection::vec(any::<u8>(), 0..513),
        ) {
            let searcher = VolnitskyBase::new(&needle, 0);
            prop_assert_eq!(searcher.search(&haystack), memchr::memmem::find(&haystack, &needle));
        }

        #[test]
        fn test_volnitsky_matches_memmem_with_inserted_match(
            needle in prop::collection::vec(any::<u8>(), 4..65),
            prefix in prop::collection::vec(any::<u8>(), 0..257),
            suffix in prop::collection::vec(any::<u8>(), 0..257),
        ) {
            let searcher = VolnitskyBase::new(&needle, 0);
            let haystack = [&prefix[..], &needle, &suffix[..]].concat();
            prop_assert_eq!(searcher.search(&haystack), memchr::memmem::find(&haystack, &needle));
        }
    }
}
