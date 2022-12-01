//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::cmp::min;

use async_trait::async_trait;
use opendal::layers::CacheFillMethod;
use opendal::layers::CachePolicy;
use opendal::layers::CacheReadEntry;
use opendal::layers::CacheReadEntryIterator;
use opendal::layers::CacheUpdateEntry;
use opendal::layers::CacheUpdateEntryIterator;
use opendal::layers::CacheUpdateMethod;
use opendal::raw::BytesRange;
use opendal::raw::Operation;

/// TODO: implement more complex cache logic.
///
/// For example, we don't need to cache every file.
#[derive(Debug)]
pub struct MemoryCachePolicy {
    step: u64,
}

impl MemoryCachePolicy {
    pub fn new(step: u64) -> Self {
        Self { step }
    }
}

#[async_trait]
impl CachePolicy for MemoryCachePolicy {
    async fn on_read(&self, path: &str, offset: u64, size: u64, _: u64) -> CacheReadEntryIterator {
        let _ = self.step;
        // Box::new(FixedCacheRangeIterator::new(
        //     path,
        //     offset,
        //     size,
        //     self.step,
        //     CacheFillMethod::Skip,
        // ))

        // demo: Don't cache data.
        Box::new(
            vec![CacheReadEntry {
                cache_path: format!("{path}.cache-{offset}-{size}"),
                read_cache: false,
                cache_read_range: (0..size).into(),
                inner_read_range: (offset..offset + size).into(),
                fill_method: CacheFillMethod::Skip,
                cache_fill_range: (offset..offset + size).into(),
            }]
            .into_iter(),
        )
    }

    async fn on_update(&self, path: &str, _: Operation) -> CacheUpdateEntryIterator {
        Box::new(
            vec![CacheUpdateEntry {
                cache_path: path.to_string(),

                // Databend will not update existing file, so we just skip all.
                update_method: CacheUpdateMethod::Skip,
            }]
            .into_iter(),
        )
    }
}

/// TODO: implement more complex cache logic.
///
/// For example:
///
/// - Implement a top n heap, and only cache files exist in heap.
/// - Only cache data file, and ignore snapshot files.
#[derive(Debug)]
pub struct RemoteCachePolicy {
    step: u64,
}

impl RemoteCachePolicy {
    pub fn new(step: u64) -> Self {
        Self { step }
    }
}

#[async_trait]
impl CachePolicy for RemoteCachePolicy {
    async fn on_read(&self, path: &str, offset: u64, size: u64, _: u64) -> CacheReadEntryIterator {
        let _ = self.step;

        // demo: Cache data by fixed range in async way
        // Box::new(FixedCacheRangeIterator::new(
        //     path,
        //     offset,
        //     size,
        //     self.step,
        //     CacheFillMethod::Async,
        // ))

        // demo: Cache data by range in sync way.
        Box::new(
            vec![CacheReadEntry {
                cache_path: format!("{path}.cache-{offset}-{size}"),
                read_cache: true,
                cache_read_range: (0..size).into(),
                inner_read_range: (offset..offset + size).into(),
                fill_method: CacheFillMethod::Sync,
                cache_fill_range: (offset..offset + size).into(),
            }]
            .into_iter(),
        )
    }

    async fn on_update(&self, path: &str, _: Operation) -> CacheUpdateEntryIterator {
        Box::new(
            vec![CacheUpdateEntry {
                cache_path: path.to_string(),

                // Databend will not update existing file, so we just skip all.
                update_method: CacheUpdateMethod::Skip,
            }]
            .into_iter(),
        )
    }
}

#[derive(Clone, Debug)]
pub struct FixedCacheRangeIterator {
    path: String,
    offset: u64,
    size: u64,
    step: u64,

    fill_method: CacheFillMethod,

    cur: u64,
}

impl FixedCacheRangeIterator {
    #[allow(unused)]
    fn new(path: &str, offset: u64, size: u64, step: u64, fill_method: CacheFillMethod) -> Self {
        Self {
            path: path.to_string(),
            offset,
            size,
            step,
            fill_method,

            cur: offset,
        }
    }

    fn cache_path(&self) -> String {
        format!("{}.cache-{}", self.path, self.cache_index())
    }

    /// Cache index is the file index across the whole file.
    fn cache_index(&self) -> u64 {
        self.cur / self.step
    }

    /// Cache range is the range that we need to read from cache file.
    fn cache_read_range(&self) -> BytesRange {
        let skipped_rem = self.cur % self.step;
        let to_read = self.size + self.offset - self.cur;
        if to_read >= (self.step - skipped_rem) {
            (skipped_rem..self.step).into()
        } else {
            (skipped_rem..skipped_rem + to_read).into()
        }
    }

    /// Total range is the range that we need to read from underlying storage.
    ///
    /// # Note
    ///
    /// We will always read `step` bytes from underlying storage.
    fn cache_fill_range(&self) -> BytesRange {
        let idx = self.cur / self.step;
        (self.step * idx..self.step * (idx + 1)).into()
    }

    /// Actual range is the range that read from underlying storage
    /// without padding with step.
    ///
    /// We can read this range if we don't need to fill the cache.
    fn inner_read_range(&self) -> BytesRange {
        let idx = self.cur / self.step;
        (self.cur..min(self.step * (idx + 1), self.size + self.offset)).into()
    }
}

impl Iterator for FixedCacheRangeIterator {
    type Item = CacheReadEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur >= self.offset + self.size {
            None
        } else {
            let cre = CacheReadEntry {
                cache_path: self.cache_path(),
                read_cache: true,
                cache_read_range: self.cache_read_range(),
                inner_read_range: self.inner_read_range(),
                fill_method: self.fill_method,
                cache_fill_range: self.cache_fill_range(),
            };
            self.cur += self
                .cache_read_range()
                .size()
                .expect("cache range size must be valid");
            Some(cre)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_cache_range_iterator() {
        let cases = vec![
            ("first part", 0, 1, 1000, vec![CacheReadEntry {
                cache_path: ".cache-0".to_string(),
                read_cache: true,
                cache_read_range: BytesRange::from(0..1),
                inner_read_range: BytesRange::from(0..1),
                fill_method: CacheFillMethod::Sync,
                cache_fill_range: BytesRange::from(0..1000),
            }]),
            ("first part with offset", 900, 1, 1000, vec![
                CacheReadEntry {
                    cache_path: ".cache-0".to_string(),
                    read_cache: true,
                    cache_read_range: BytesRange::from(900..901),
                    inner_read_range: BytesRange::from(900..901),
                    fill_method: CacheFillMethod::Sync,
                    cache_fill_range: BytesRange::from(0..1000),
                },
            ]),
            ("first part with edge case", 900, 100, 1000, vec![
                CacheReadEntry {
                    cache_path: ".cache-0".to_string(),
                    read_cache: true,
                    cache_read_range: BytesRange::from(900..1000),
                    inner_read_range: BytesRange::from(900..1000),
                    fill_method: CacheFillMethod::Sync,
                    cache_fill_range: BytesRange::from(0..1000),
                },
            ]),
            ("two parts", 900, 101, 1000, vec![
                CacheReadEntry {
                    cache_path: ".cache-0".to_string(),
                    read_cache: true,
                    cache_read_range: BytesRange::from(900..1000),
                    inner_read_range: BytesRange::from(900..1000),
                    fill_method: CacheFillMethod::Sync,
                    cache_fill_range: BytesRange::from(0..1000),
                },
                CacheReadEntry {
                    cache_path: ".cache-1".to_string(),
                    read_cache: true,
                    cache_read_range: BytesRange::from(0..1),
                    inner_read_range: BytesRange::from(1000..1001),
                    fill_method: CacheFillMethod::Sync,
                    cache_fill_range: BytesRange::from(1000..2000),
                },
            ]),
            ("second part", 1001, 1, 1000, vec![CacheReadEntry {
                cache_path: ".cache-1".to_string(),
                read_cache: true,
                cache_read_range: BytesRange::from(1..2),
                inner_read_range: BytesRange::from(1001..1002),
                fill_method: CacheFillMethod::Sync,
                cache_fill_range: BytesRange::from(1000..2000),
            }]),
        ];

        for (name, offset, size, step, expected) in cases {
            let it = FixedCacheRangeIterator::new("", offset, size, step, CacheFillMethod::Sync);
            let actual: Vec<_> = it.collect();

            assert_eq!(expected, actual, "{name}")
        }
    }
}
