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
