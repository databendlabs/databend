// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

#[test]
fn test_progress() -> Result<()> {
    use crate::*;

    let progress = Progress::create();
    let values = ProgressValues {
        read_rows: 2,
        read_bytes: 10,
        total_rows_to_read: 10,
    };

    progress.incr(&values);

    assert_eq!(2, progress.get_values().read_rows);
    assert_eq!(10, progress.get_values().read_bytes);
    progress.reset();

    assert_eq!(0, progress.get_values().read_rows);
    assert_eq!(0, progress.get_values().read_bytes);
    Ok(())
}
