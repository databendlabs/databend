// Copyright 2023 Datafuse Labs.
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

use databend_common_expression::BlockThresholds;

fn default_thresholds() -> BlockThresholds {
    BlockThresholds::new(1000, 1_000_000, 100_000, 4)
}

#[test]
fn test_check_perfect_block() {
    let t = default_thresholds();

    // All below threshold
    assert!(!t.check_perfect_block(100, 1000, 1000));

    // Any one above threshold
    assert!(t.check_perfect_block(800, 1000, 1000));
    assert!(t.check_perfect_block(100, 800_000, 1000));
    assert!(t.check_perfect_block(100, 1000, 80_000));
}

#[test]
fn test_check_perfect_segment() {
    let t = default_thresholds();
    let total_blocks = 4;

    // Below threshold
    assert!(!t.check_perfect_segment(total_blocks, 100, 1000, 1000));

    // One condition meets threshold
    assert!(t.check_perfect_segment(total_blocks, 4000, 1000, 1000));
    assert!(t.check_perfect_segment(total_blocks, 100, 4_000_000, 1000));
    assert!(t.check_perfect_segment(total_blocks, 100, 1000, 320_000));
}

#[test]
fn test_check_large_enough() {
    let t = default_thresholds();

    assert!(!t.check_large_enough(100, 1000));
    assert!(t.check_large_enough(800, 1000));
    assert!(t.check_large_enough(100, 800_000));
}

#[test]
fn test_check_for_compact() {
    let t = default_thresholds();

    assert!(t.check_for_compact(1500, 1_500_000)); // just under 2x min
    assert!(!t.check_for_compact(3000, 3_000_000)); // too large
}

#[test]
fn test_check_too_small() {
    let t = default_thresholds();

    assert!(t.check_too_small(50, 10_000, 10_000)); // All very small
    assert!(!t.check_too_small(800, 10_000, 10_000)); // Row count not too small
    assert!(!t.check_too_small(50, 800_000, 10_000)); // Block size not too small
}

#[test]
fn test_calc_rows_for_compact() {
    let t = default_thresholds();

    assert_eq!(t.calc_rows_for_compact(500_000, 1000), 1000);

    // Block number by rows wins → max_rows_per_block
    assert_eq!(
        t.calc_rows_for_compact(2_000_000, 10_000),
        t.max_rows_per_block
    );

    // Size-based block number wins
    assert_eq!(t.calc_rows_for_compact(4_000_000, 2000), 400);
}

#[test]
fn test_calc_rows_for_recluster() {
    let t = default_thresholds();

    // compact enough to skip further calculations
    assert_eq!(t.calc_rows_for_recluster(1000, 500_000, 100_000), 1000);

    // row-based block count exceeds compressed-based block count, use max rows per block.
    assert_eq!(
        t.calc_rows_for_recluster(10_000, 2_000_000, 100_000),
        t.max_rows_per_block
    );

    // Case 1: If the block size is too bigger.
    let result = t.calc_rows_for_recluster(4_000, 30_000_000, 600_000);
    assert_eq!(result, 267);

    // Case 2: If the block size is too smaller.
    let result = t.calc_rows_for_recluster(4_000, 2_000_000, 600_000);
    assert_eq!(result, 800);

    // Case 3: use the compressed-based block count.
    let result = t.calc_rows_for_recluster(4_000, 10_000_000, 600_000);
    assert_eq!(result, 667);
}
