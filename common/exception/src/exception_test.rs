// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_format_with_error_codes() {
    use crate::exception::*;

    assert_eq!(
        format!("{}", ErrorCodes::Ok("test message 1")),
        "Code: 0, displayText = test message 1."
    );

    assert_eq!(
        format!("{}", ErrorCodes::Ok("test message 2")),
        "Code: 0, displayText = test message 2."
    );
    assert_eq!(
        format!("{}", ErrorCodes::UnknownException("test message 1")),
        "Code: 1000, displayText = test message 1."
    );
    assert_eq!(
        format!("{}", ErrorCodes::UnknownException("test message 2")),
        "Code: 1000, displayText = test message 2."
    );
}

#[test]
fn test_derive_from_std_error() {
    use crate::exception::ErrorCodes;
    use crate::exception::ToErrorCodes;

    let fmt_rst: std::result::Result<(), std::fmt::Error> = Err(std::fmt::Error {});

    let rst1: crate::exception::Result<()> =
        fmt_rst.map_err_to_code(ErrorCodes::UnknownException, || 123);

    assert_eq!(
        "Code: 1000, displayText = 123, cause: an error occurred when formatting an argument.",
        format!("{}", rst1.as_ref().unwrap_err())
    );

    let rst2: crate::exception::Result<()> = rst1.map_err_to_code(ErrorCodes::Ok, || "wrapper");

    assert_eq!(
        "Code: 0, displayText = wrapper, cause: Code: 1000, displayText = 123, cause: an error occurred when formatting an argument..",
        format!("{}", rst2.as_ref().unwrap_err())
    );
}
