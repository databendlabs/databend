// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use tonic::Code;
use tonic::Status;

#[test]
fn test_format_with_error_codes() {
    use crate::exception::*;

    assert_eq!(
        format!("{}", ErrorCode::Ok("test message 1")),
        "Code: 0, displayText = test message 1."
    );

    assert_eq!(
        format!("{}", ErrorCode::Ok("test message 2")),
        "Code: 0, displayText = test message 2."
    );
    assert_eq!(
        format!("{}", ErrorCode::UnknownException("test message 1")),
        "Code: 1000, displayText = test message 1."
    );
    assert_eq!(
        format!("{}", ErrorCode::UnknownException("test message 2")),
        "Code: 1000, displayText = test message 2."
    );
}

#[test]
fn test_derive_from_std_error() {
    use crate::exception::ErrorCode;
    use crate::exception::ToErrorCode;

    let fmt_rst: std::result::Result<(), std::fmt::Error> = Err(std::fmt::Error {});

    let rst1: crate::exception::Result<()> =
        fmt_rst.map_err_to_code(ErrorCode::UnknownException, || 123);

    assert_eq!(
        "Code: 1000, displayText = 123, cause: an error occurred when formatting an argument.",
        format!("{}", rst1.as_ref().unwrap_err())
    );

    let rst2: crate::exception::Result<()> = rst1.map_err_to_code(ErrorCode::Ok, || "wrapper");

    assert_eq!(
        "Code: 0, displayText = wrapper, cause: Code: 1000, displayText = 123, cause: an error occurred when formatting an argument..",
        format!("{}", rst2.as_ref().unwrap_err())
    );
}

#[test]
fn test_derive_from_display() {
    use crate::exception::ErrorCode;
    use crate::exception::ToErrorCode;

    let rst: std::result::Result<(), u64> = Err(3);

    let rst1: crate::exception::Result<()> =
        rst.map_err_to_code(ErrorCode::UnknownException, || 123);

    assert_eq!(
        "Code: 1000, displayText = 123, cause: 3.",
        format!("{}", rst1.as_ref().unwrap_err())
    );
}

#[test]
fn test_from_and_to_status() -> anyhow::Result<()> {
    use crate::exception::*;
    let e = ErrorCode::IllegalDataType("foo");
    let status: Status = e.into();
    assert_eq!(Code::Unknown, status.code());

    // Only compare the code and message. Discard backtrace.
    assert_eq!(
        r#"{"code":7,"message":"foo","#.as_bytes(),
        &status.details()[..26]
    );

    {
        // test from &Status

        let e2: ErrorCode = (&status).into();

        assert_eq!(7, e2.code());
        assert_eq!("foo", e2.message());
    }

    {
        // test from Status

        let e2: ErrorCode = status.into();

        assert_eq!(7, e2.code());
        assert_eq!("foo", e2.message());
    }

    Ok(())
}
