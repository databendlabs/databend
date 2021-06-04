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

    let x: std::result::Result<(), std::fmt::Error> = Err(std::fmt::Error {});
    let y: crate::exception::Result<()> =
        x.map_err_to_code(ErrorCodes::UnknownException, || format!("{}", 123));

    assert_eq!(
        "Code: 1000, displayText = Error.",
        format!("{}", y.unwrap_err())
    );
}
