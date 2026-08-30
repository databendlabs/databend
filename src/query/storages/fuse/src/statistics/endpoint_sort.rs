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

//! Shared endpoint encoding and sorting for clustering diagnostics.
//!
//! Typed endpoint columns are converted to SQL-compatible mem-comparable rows, then their row IDs
//! are sorted with an MSD radix sort in a bounded request-local Rayon pool. Sorting preserves the
//! original row IDs, allowing Linear and Hilbert diagnostics to interpret the caller-supplied
//! alternating min/max layout.

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::types::BinaryColumn;
use databend_common_expression::types::DataType;
use databend_common_pipeline_transforms::sorts::core::RowConverter;
use databend_common_pipeline_transforms::sorts::core::SortKeyDescription;
use databend_common_pipeline_transforms::sorts::core::VariableRowConverter;
use rayon::prelude::*;

// One sentinel bucket for end-of-row plus one bucket for each possible byte value.
const RADIX_BUCKETS: usize = 257;
const RADIX_COMPARISON_SORT_THRESHOLD: usize = 4096;
const MAX_RADIX_RECURSION_LEVELS: usize = 8;
const MAX_DIAGNOSTICS_THREADS: usize = 8;

/// Run one clustering-information calculation in a bounded request-local Rayon pool.
/// The pool and its worker threads are dropped when `operation` returns. Nested Rayon calls inherit
/// this pool, so endpoint sorting, ranking, and partition sweeps share the same thread limit without
/// retaining workers across independent table-function requests.
pub(crate) fn with_request_pool<R: Send>(
    max_threads: usize,
    operation: impl FnOnce(usize) -> Result<R> + Send,
) -> Result<R> {
    let threads = max_threads.clamp(1, MAX_DIAGNOSTICS_THREADS);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(move |index| format!("clustering-info-{threads}-{index}"))
        .build()
        .map_err(|error| {
            ErrorCode::Internal(format!(
                "failed to create clustering information diagnostics pool: {error}"
            ))
        })?;
    pool.install(|| operation(threads))
}

/// Encoded endpoints and their block-independent ascending row order.
/// Sorting is intentionally unstable because consumers group equal keys by value. Under the
/// caller-supplied alternating layout, each original row ID retains its assigned block and min/max
/// role regardless of tie order.
pub(crate) struct SortedEndpoints {
    pub(crate) keys: BinaryColumn,
    pub(crate) order: Vec<u32>,
}

/// Encode typed endpoint columns and sort their row IDs in the caller's Rayon execution context.
/// The returned keys retain input row order; only `order` is sorted. Given the caller-supplied
/// alternating layout, consumers use endpoint-ID parity to interpret each row's block and min/max
/// role. Malformed column/type shapes, odd endpoint counts, and inputs too large for u32 row IDs
/// return an internal error without producing a partial order. Empty typed columns are valid and
/// produce empty encoded keys and an empty order.
pub(crate) fn sort_endpoints(
    builders: Vec<ColumnBuilder>,
    key_types: &[DataType],
) -> Result<SortedEndpoints> {
    if builders.len() != key_types.len() {
        return Err(ErrorCode::Internal(format!(
            "clustering information endpoint columns and types differ: {} columns, {} types",
            builders.len(),
            key_types.len()
        )));
    }
    let endpoint_count = builders.first().map_or(0, ColumnBuilder::len);
    if builders
        .iter()
        .any(|builder| builder.len() != endpoint_count)
    {
        return Err(ErrorCode::Internal(
            "clustering information endpoint columns have different lengths".to_string(),
        ));
    }
    // Sorted orders and block IDs use u32 to halve index memory at multi-million-block scale.
    if endpoint_count > u32::MAX as usize {
        return Err(ErrorCode::Internal(format!(
            "clustering information has too many endpoints for exact diagnostics: {endpoint_count}"
        )));
    }
    // Diagnostics index adjacent rows as endpoint pairs, so their cardinality must be even.
    if !endpoint_count.is_multiple_of(2) {
        return Err(ErrorCode::Internal(format!(
            "clustering information requires an even endpoint count for min/max pairs, got {endpoint_count}"
        )));
    }

    let schema = DataSchemaRefExt::create(
        key_types
            .iter()
            .enumerate()
            .map(|(index, ty)| DataField::new(&format!("cluster_key_{index}"), ty.clone()))
            .collect(),
    );
    // Ascending, NULLS LAST matches clustering-key comparison semantics. The converter produces
    // byte rows whose ordinary lexicographic order is therefore the required endpoint order.
    let sort_desc = (0..key_types.len())
        .map(|offset| SortColumnDescription {
            offset,
            asc: true,
            nulls_first: false,
        })
        .collect::<Vec<_>>();
    let block =
        DataBlock::new_from_columns(builders.into_iter().map(ColumnBuilder::build).collect());
    let converter =
        VariableRowConverter::new(SortKeyDescription::new(sort_desc.into(), schema, true)?)?;
    let keys = converter.convert(&block)?;
    let order = if keys.is_empty() {
        Vec::new()
    } else {
        radix_sort_ids(&keys, (0..keys.len() as u32).collect(), 0, 0)
    };
    Ok(SortedEndpoints { keys, order })
}

/// Map one key position to an MSD bucket.
/// Bucket 0 is the end-of-row sentinel; byte values map to 1..=256. This makes a shorter key sort
/// before any longer key sharing its prefix, matching ordinary byte-slice ordering.
#[inline]
fn radix_digit(key: &[u8], depth: usize) -> usize {
    key.get(depth).map_or(0, |byte| *byte as usize + 1)
}

/// Recursively sort one MSD bucket. Common prefixes are skipped before allocating child buckets;
/// independent child buckets are processed in parallel and concatenated in byte order.
fn radix_sort_ids(
    keys: &BinaryColumn,
    mut ids: Vec<u32>,
    mut depth: usize,
    recursion_level: usize,
) -> Vec<u32> {
    if ids.len() <= RADIX_COMPARISON_SORT_THRESHOLD || recursion_level >= MAX_RADIX_RECURSION_LEVELS
    {
        ids.sort_unstable_by(|left, right| {
            // SAFETY: endpoint ids are constructed from 0..keys.len().
            unsafe { keys.index_unchecked(*left as usize) }
                .cmp(unsafe { keys.index_unchecked(*right as usize) })
        });
        return ids;
    }

    // Skip a common prefix without allocating one bucket per byte. If the shared digit is the
    // sentinel, every remaining row is identical and no further ordering work is required.
    loop {
        let first_digit = radix_digit(
            // SAFETY: this path is only used for a non-empty id vector.
            unsafe { keys.index_unchecked(ids[0] as usize) },
            depth,
        );
        let all_equal = ids.iter().skip(1).all(|id| {
            radix_digit(
                // SAFETY: all ids originate from 0..keys.len().
                unsafe { keys.index_unchecked(*id as usize) },
                depth,
            ) == first_digit
        });
        if !all_equal {
            break;
        }
        if first_digit == 0 {
            return ids;
        }
        depth += 1;
    }

    let mut counts = [0usize; RADIX_BUCKETS];
    for id in &ids {
        counts[radix_digit(
            // SAFETY: all ids originate from 0..keys.len().
            unsafe { keys.index_unchecked(*id as usize) },
            depth,
        )] += 1;
    }

    let mut buckets: [Vec<u32>; RADIX_BUCKETS] =
        std::array::from_fn(|bucket| Vec::with_capacity(counts[bucket]));
    for id in ids {
        let digit = radix_digit(
            // SAFETY: all ids originate from 0..keys.len().
            unsafe { keys.index_unchecked(id as usize) },
            depth,
        );
        buckets[digit].push(id);
    }

    let total = counts.iter().sum();
    // Rayon preserves the indexed bucket order in this collection. Concatenating from sentinel
    // through byte 255 therefore reconstructs the global lexicographic order.
    let sorted_buckets = buckets
        .into_par_iter()
        .enumerate()
        .map(|(digit, bucket)| {
            if digit == 0 || bucket.len() <= 1 {
                bucket
            } else {
                radix_sort_ids(keys, bucket, depth + 1, recursion_level + 1)
            }
        })
        .collect::<Vec<_>>();

    let mut sorted = Vec::with_capacity(total);
    for bucket in sorted_buckets {
        sorted.extend(bucket);
    }
    sorted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_pool_bounds_threads() {
        for (requested, expected) in [(0, 1), (4, 4), (usize::MAX, MAX_DIAGNOSTICS_THREADS)] {
            with_request_pool(requested, |threads| {
                assert_eq!(threads, expected);
                assert_eq!(rayon::current_num_threads(), expected);
                Ok(())
            })
            .unwrap();
        }
    }

    #[test]
    fn test_radix_sort_matches_byte_order() {
        let row_count = RADIX_COMPARISON_SORT_THRESHOLD + MAX_RADIX_RECURSION_LEVELS + 2;
        let row_len = MAX_RADIX_RECURSION_LEVELS + 2;
        let mut rows = (0..row_count)
            .map(|index| {
                (0..row_len)
                    .map(|level| u8::from(index != level))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        rows.extend([
            Vec::new(),
            b"shared-prefix".to_vec(),
            b"shared-prefix-a".to_vec(),
            b"shared-prefix-a".to_vec(),
            b"shared-prefix-b".to_vec(),
        ]);
        let rows = BinaryColumn::from_iter(rows);
        let actual = with_request_pool(4, |_| {
            Ok(radix_sort_ids(
                &rows,
                (0..rows.len() as u32).collect(),
                0,
                0,
            ))
        })
        .unwrap();
        let mut expected = (0..rows.len() as u32).collect::<Vec<_>>();
        expected.sort_unstable_by(|left, right| {
            rows.value(*left as usize).cmp(rows.value(*right as usize))
        });
        assert_eq!(
            actual
                .iter()
                .map(|id| rows.value(*id as usize))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|id| rows.value(*id as usize))
                .collect::<Vec<_>>()
        );
    }
}
