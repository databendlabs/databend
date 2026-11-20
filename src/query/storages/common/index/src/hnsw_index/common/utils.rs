// Copyright Qdrant
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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

pub fn rev_range(a: usize, b: usize) -> impl Iterator<Item = usize> {
    (b + 1..=a).rev()
}

pub fn check_process_stopped(stopped: &AtomicBool) -> Result<()> {
    if stopped.load(Ordering::Relaxed) {
        return Err(ErrorCode::Internal("check process stopped error"));
    }
    Ok(())
}

/// Whether the ids of a batch are nearly consecutive, so that the hardware
/// prefetcher already streams their vectors.
pub fn is_dense_batch(ids: &[u32]) -> bool {
    if ids.len() < 2 {
        return false;
    }
    let (min, max) = ids
        .iter()
        .fold((u32::MAX, 0), |(min, max), &id| (min.min(id), max.max(id)));
    ((max - min) as usize) < ids.len() * 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_dense_batch() {
        assert!(!is_dense_batch(&[]));
        assert!(!is_dense_batch(&[7]));
        assert!(is_dense_batch(&[3, 1, 2, 5]));
        assert!(!is_dense_batch(&[1, 900, 37, 4000]));
    }
}
