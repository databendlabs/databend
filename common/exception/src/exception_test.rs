// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_format_with_error_codes() {
    use crate::exception::*;

    assert_eq!(format!("{}", ErrorCodes::Ok(String::from("test message 1"))),
               String::from("Code: 0, displayText = test message 1."));

    assert_eq!(
        format!("{}", ErrorCodes::Ok(String::from("test message 2"))),
        String::from("Code: 0, displayText = test message 2.")
    );
    assert_eq!(
        format!(
            "{}",
            ErrorCodes::UnknownException(String::from("test message 1"))
        ),
        String::from("Code: 1000, displayText = test message 1.")
    );
    assert_eq!(
        format!(
            "{}",
            ErrorCodes::UnknownException(String::from("test message 2"))
        ),
        String::from("Code: 1000, displayText = test message 2.")
    );
}
