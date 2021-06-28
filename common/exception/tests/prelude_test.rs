// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::prelude::*;

#[test]
fn test_prelude() -> anyhow::Result<()> {
    let x: std::result::Result<(), std::fmt::Error> = Err(std::fmt::Error {});
    let y: common_exception::Result<()> = x.map_err_to_code(ErrorCode::UnknownException, || 123);

    assert_eq!(
        "Code: 1000, displayText = 123, cause: an error occurred when formatting an argument.",
        format!("{}", y.unwrap_err())
    );
    Ok(())
}
