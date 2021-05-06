// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_progress() -> anyhow::Result<()> {
    use crate::*;

    let progress = Progress::create();
    progress.add_rows(2);
    progress.add_bytes(10);

    assert_eq!(2, progress.get_values().read_rows);
    assert_eq!(10, progress.get_values().read_bytes);
    progress.reset();

    assert_eq!(0, progress.get_values().read_rows);
    assert_eq!(0, progress.get_values().read_bytes);

    Ok(())
}
