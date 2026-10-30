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

use databend_common_catalog::BasicColumnStatistics;
use databend_common_statistics::Datum;
use databend_common_statistics::NdvEstimate;

fn column(min: Datum, max: Datum, ndv: Option<NdvEstimate>) -> BasicColumnStatistics {
    BasicColumnStatistics {
        min: Some(min),
        max: Some(max),
        ndv,
        null_count: 0,
        in_memory_size: 800,
    }
}

#[test]
fn test_extrapolated_ndv_retains_row_count_upper() {
    let stats = column(
        Datum::Int(0),
        Datum::Int(10_000),
        Some(NdvEstimate::exact(12.0)),
    );
    let estimated = stats.get_useful_stat(3000, 100).unwrap();
    assert_eq!(estimated.ndv, Some(NdvEstimate::new(17.0, 2912.0)));
}

#[test]
fn test_uncovered_rows_extend_the_estimator_upper() {
    let stats = column(
        Datum::Int(0),
        Datum::Int(10_000),
        Some(NdvEstimate::new(12.0, 15.0)),
    );
    assert_eq!(
        stats.get_useful_stat(1000, 999).unwrap().ndv,
        Some(NdvEstimate::new(12.0, 16.0))
    );
}

#[test]
fn test_ndv_domain_cap_applies_after_extrapolation() {
    let stats = column(
        Datum::Int(0),
        Datum::Int(99),
        Some(NdvEstimate::exact(12.0)),
    );
    let estimated = stats.get_useful_stat(3000, 100).unwrap();
    assert_eq!(estimated.ndv, Some(NdvEstimate::new(17.0, 100.0)));
}

#[test]
fn test_domain_bound_after_estimation() {
    // Preserve the upstream legacy-coverage cases without turning their
    // deterministic domain upper bounds into observed NDV points.
    for (ndv, sampled_rows, rows, max, expected) in [
        (
            Some(1_000_000),
            0,
            1_000_000,
            8,
            NdvEstimate::upper_bound(9.0),
        ),
        (Some(3), 100, 1_000_000, 8, NdvEstimate::exact(9.0)),
        (None, 0, 1_000_000, 8, NdvEstimate::upper_bound(9.0)),
        (Some(0), 0, 1_000_000, 8, NdvEstimate::upper_bound(9.0)),
        (Some(3), 0, 1_000, 999, NdvEstimate::upper_bound(1_000.0)),
    ] {
        let stat = column(
            Datum::Int(0),
            Datum::Int(max),
            ndv.map(|value| NdvEstimate::exact(value as f64)),
        );
        assert_eq!(
            stat.get_useful_stat(rows, sampled_rows).unwrap().ndv,
            Some(expected),
            "ndv={ndv:?}, sampled_rows={sampled_rows}, rows={rows}, max={max}"
        );
    }
}

#[test]
fn test_exact_domain_size_at_provider_boundary() {
    for (min, max, expected) in [
        (Datum::Int(-4), Datum::Int(4), Some(9.0)),
        (Datum::Int(i64::MAX - 1), Datum::Int(i64::MAX), Some(2.0)),
        (
            Datum::UInt(1_u64 << 53),
            Datum::UInt((1_u64 << 53) + 1),
            Some(2.0),
        ),
        (Datum::Bool(false), Datum::Bool(true), Some(2.0)),
        (Datum::Bool(true), Datum::Bool(true), Some(1.0)),
        (Datum::Int(i64::MIN), Datum::Int(i64::MAX), Some(100.0)),
        (Datum::UInt(0), Datum::UInt(u64::MAX), Some(100.0)),
        (Datum::Int(1), Datum::Int(0), None),
    ] {
        let stat = column(min, max, None);
        assert_eq!(
            stat.get_useful_stat(100, 100).and_then(|stat| stat.ndv),
            expected.map(NdvEstimate::upper_bound)
        );
    }
}

#[test]
fn test_fractional_and_string_domains_are_not_capped_after_estimation() {
    for (min, max) in [
        (Datum::Float(0.0.into()), Datum::Float(2.5.into())),
        (Datum::Bytes(b"a".to_vec()), Datum::Bytes(b"b".to_vec())),
    ] {
        let stat = column(min, max, Some(NdvEstimate::exact(80.0)));
        for (sampled_rows, expected) in [
            (0, NdvEstimate::upper_bound(1_000.0)),
            (100, NdvEstimate::new(541.0, 980.0)),
            (1_000, NdvEstimate::exact(80.0)),
        ] {
            assert_eq!(
                stat.get_useful_stat(1_000, sampled_rows).unwrap().ndv,
                Some(expected)
            );
        }
    }
}

#[test]
fn test_full_coverage_does_not_make_approximate_ndv_exact() {
    let mut stats = column(
        Datum::Int(0),
        Datum::Int(1000),
        Some(NdvEstimate::new(12.0, 1000.0)),
    );
    stats.null_count = 20;
    let estimated = stats.get_useful_stat(100, 100).unwrap();
    assert_eq!(estimated.ndv, Some(NdvEstimate::new(12.0, 80.0)));
}

#[test]
fn test_missing_ndv_remains_upper_only() {
    for stats_rows in [0, 100] {
        let mut stats = column(Datum::Int(0), Datum::Int(99), None);
        stats.null_count = 20;
        let estimated = stats.get_useful_stat(100, stats_rows).unwrap();
        assert_eq!(estimated.ndv, Some(NdvEstimate::upper_bound(80.0)));
    }
    let stats = column(Datum::Int(0), Datum::Int(9), None);
    assert_eq!(
        stats.get_useful_stat(100, 100).unwrap().ndv,
        Some(NdvEstimate::upper_bound(10.0))
    );
}

#[test]
fn test_no_sample_rows_does_not_invent_expected_ndv() {
    let stats = column(
        Datum::Int(0),
        Datum::Int(1000),
        Some(NdvEstimate::exact(12.0)),
    );
    assert_eq!(
        stats.get_useful_stat(100, 0).unwrap().ndv,
        Some(NdvEstimate::upper_bound(100.0))
    );
}

#[test]
fn test_non_discrete_ranges_do_not_cap_ndv_by_distance() {
    let float = column(
        Datum::Float(0.1.into()),
        Datum::Float(0.9.into()),
        Some(NdvEstimate::new(12.0, 100.0)),
    );
    let bytes = column(
        Datum::Bytes(b"a".to_vec()),
        Datum::Bytes(b"b".to_vec()),
        Some(NdvEstimate::new(12.0, 100.0)),
    );
    for stats in [float, bytes] {
        assert_eq!(
            stats.get_useful_stat(100, 100).unwrap().ndv,
            Some(NdvEstimate::new(12.0, 100.0))
        );
    }
}

#[test]
fn test_zero_ndv_and_empty_columns() {
    let stats = column(Datum::Int(0), Datum::Int(99), Some(NdvEstimate::exact(0.0)));
    assert_eq!(
        stats.get_useful_stat(100, 100).unwrap().ndv,
        Some(NdvEstimate::upper_bound(100.0))
    );
    assert_eq!(
        stats.get_useful_stat(0, 0).unwrap().ndv,
        Some(NdvEstimate::exact(0.0))
    );
    let mut all_null = stats;
    all_null.null_count = 100;
    assert_eq!(
        all_null.get_useful_stat(100, 100).unwrap().ndv,
        Some(NdvEstimate::exact(0.0))
    );
}

#[test]
fn test_discrete_bounds_are_computed_without_float_rounding() {
    let stats = column(Datum::UInt(u64::MAX - 10), Datum::UInt(u64::MAX), None);
    assert_eq!(
        stats.get_useful_stat(100, 100).unwrap().ndv,
        Some(NdvEstimate::upper_bound(11.0))
    );
    let stats = column(Datum::Bool(false), Datum::Bool(true), None);
    assert_eq!(
        stats.get_useful_stat(100, 100).unwrap().ndv,
        Some(NdvEstimate::upper_bound(2.0))
    );
}

fn chunk(min: i64, max: i64, ndv: Option<u64>, nulls: u64, rows: u64) -> BasicColumnStatistics {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::NumberScalar;
    use databend_storages_common_table_meta::meta::ColumnStatistics;

    databend_common_catalog::statistics::basic_statistics::from_chunk_column_statistics(
        ColumnStatistics {
            min: Scalar::Number(NumberScalar::Int64(min)),
            max: Scalar::Number(NumberScalar::Int64(max)),
            distinct_of_values: ndv,
            null_count: nulls,
            in_memory_size: 800,
        },
        rows,
    )
}

fn merge_chunks(chunks: impl IntoIterator<Item = BasicColumnStatistics>) -> BasicColumnStatistics {
    let mut merged = BasicColumnStatistics::new_null();
    for chunk in chunks {
        merged.merge(chunk);
    }
    merged
}

#[test]
fn test_chunk_distinct_count_is_an_upper_bound() {
    assert_eq!(
        chunk(0, 1000, Some(12), 20, 100)
            .get_useful_stat(100, 100)
            .unwrap()
            .ndv,
        Some(NdvEstimate::upper_bound(12.0))
    );
}

#[test]
fn test_chunk_distinct_counts_sum_to_an_upper_bound() {
    // Overlapping and disjoint chunks alike yield only the summed bound.
    for second in [
        chunk(0, 1000, Some(5), 0, 100),
        chunk(2000, 3000, Some(5), 0, 100),
    ] {
        let merged = merge_chunks([chunk(0, 1000, Some(12), 0, 100), second]);
        assert_eq!(
            merged.get_useful_stat(200, 200).unwrap().ndv,
            Some(NdvEstimate::upper_bound(17.0))
        );
    }
}

#[test]
fn test_chunk_without_count_contributes_its_own_bound() {
    // Missing or zero counts over non-NULL rows fall back to that chunk's
    // non-NULL rows and domain; the other chunks keep their counts.
    for ndv in [None, Some(0)] {
        let merged = merge_chunks([
            chunk(0, 1000, Some(12), 0, 100),
            chunk(0, 1000, ndv, 10, 100),
            chunk(0, 4, ndv, 0, 100),
        ]);
        assert_eq!(
            merged.get_useful_stat(300, 300).unwrap().ndv,
            Some(NdvEstimate::upper_bound(12.0 + 90.0 + 5.0))
        );
    }
}

#[test]
fn test_all_null_chunk_adds_no_distinct_values() {
    let mut all_null = chunk(0, 0, Some(0), 100, 100);
    all_null.min = None;
    all_null.max = None;
    let merged = merge_chunks([chunk(0, 1000, Some(12), 0, 100), all_null]);
    assert_eq!(
        merged.get_useful_stat(200, 200).unwrap().ndv,
        Some(NdvEstimate::upper_bound(12.0))
    );
}

#[test]
fn test_disjoint_measured_counts_add() {
    let mut stats = column(Datum::Int(0), Datum::Int(9), Some(NdvEstimate::exact(10.0)));
    stats.merge(column(
        Datum::Int(10),
        Datum::Int(19),
        Some(NdvEstimate::exact(10.0)),
    ));
    assert_eq!(stats.ndv, Some(NdvEstimate::exact(20.0)));
}

#[test]
fn test_merge_preserves_uncertainty_and_missing_ndv() {
    let stats = column(
        Datum::Int(0),
        Datum::Int(1000),
        Some(NdvEstimate::new(12.0, 100.0)),
    );
    let mut merged = BasicColumnStatistics::new_null();
    merged.merge(stats.clone());
    assert_eq!(merged.ndv, stats.ndv);
    merged.merge(stats);
    assert_eq!(merged.ndv, Some(NdvEstimate::upper_bound(200.0)));
    merged.merge(column(Datum::Int(0), Datum::Int(1000), None));
    assert_eq!(merged.ndv, None);
    assert_eq!(
        merged.get_useful_stat(300, 300).unwrap().ndv,
        Some(NdvEstimate::upper_bound(300.0))
    );
}
