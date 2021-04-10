// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

/// Usage:
/// `assert_blocks_eq!(expected_lines: &[&str], blocks: &[DataBlock])`
///
#[macro_export]
macro_rules! assert_blocks_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        common_datablocks::assert_blocks_eq!($EXPECTED_LINES, $CHUNKS)
    };
}

/// Auto sorted.
/// Usage:
/// `assert_blocks_sorted_eq!(expected_lines: &[&str], blocks: &[DataBlock])`
///
#[macro_export]
macro_rules! assert_blocks_sorted_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        common_datablocks::assert_blocks_sorted_eq!($EXPECTED_LINES, $CHUNKS)
    };
}
