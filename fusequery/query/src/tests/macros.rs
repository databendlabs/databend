// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

///
/// `assert_blocks_eq!(expected_lines: &[&str], blocks: &[DataBlock])`
///
#[macro_export]
macro_rules! assert_blocks_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        use std::convert::TryInto;
        let expected_lines: Vec<String> = $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

        // Convert DataBlock to arrow RecordBatch.
        let batches = $CHUNKS
            .iter()
            .map(|block| block.clone().try_into().unwrap())
            .collect::<Vec<_>>();
        let formatted = common_arrow::arrow::util::pretty::pretty_format_batches(&batches).unwrap();

        let actual_lines: Vec<&str> = formatted.trim().lines().collect();

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}
