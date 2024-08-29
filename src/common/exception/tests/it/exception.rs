// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_exception::ErrorCode;
use databend_common_exception::SerializedError;
use tonic::Code;
use tonic::Status;

#[test]
fn test_format_with_error_codes() {
    use databend_common_exception::exception::*;

    assert_eq!(
        ErrorCode::Ok("test message 1").to_string(),
        "Ok. Code: 0, Text = test message 1."
    );

    assert_eq!(
        ErrorCode::Ok("test message 2").to_string(),
        "Ok. Code: 0, Text = test message 2."
    );
    assert_eq!(
        ErrorCode::UnknownException("test message 1").to_string(),
        "UnknownException. Code: 1067, Text = test message 1."
    );
    assert_eq!(
        ErrorCode::UnknownException("test message 2").to_string(),
        "UnknownException. Code: 1067, Text = test message 2."
    );
}

#[test]
fn test_error_code() {
    use databend_common_exception::exception::*;

    let err = ErrorCode::UnknownException("test message 1");

    assert_eq!(err.code(), ErrorCode::UNKNOWN_EXCEPTION);
}

#[test]
fn test_derive_from_std_error() {
    use databend_common_exception::exception::ErrorCode;
    use databend_common_exception::exception::ToErrorCode;

    let fmt_rst: std::result::Result<(), std::fmt::Error> = Err(std::fmt::Error {});

    let rst1: databend_common_exception::exception::Result<()> =
        fmt_rst.map_err_to_code(ErrorCode::UnknownException, || 123);

    assert_eq!(
        "UnknownException. Code: 1067, Text = 123, cause: an error occurred when formatting an argument.",
        rst1.as_ref().unwrap_err().to_string()
    );

    let rst2: databend_common_exception::exception::Result<()> =
        rst1.map_err_to_code(ErrorCode::Ok, || "wrapper");

    assert_eq!(
        "Ok. Code: 0, Text = wrapper, cause: UnknownException. Code: 1067, Text = 123, cause: an error occurred when formatting an argument..",
        rst2.as_ref().unwrap_err().to_string()
    );
}

#[test]
fn test_derive_from_display() {
    use databend_common_exception::exception::ErrorCode;
    use databend_common_exception::exception::ToErrorCode;

    let rst: std::result::Result<(), u64> = Err(3);

    let rst1: databend_common_exception::exception::Result<()> =
        rst.map_err_to_code(ErrorCode::UnknownException, || 123);

    assert_eq!(
        "UnknownException. Code: 1067, Text = 123, cause: 3.",
        rst1.as_ref().unwrap_err().to_string()
    );
}

#[test]
fn test_from_and_to_serialized_error() {
    let ec = ErrorCode::UnknownDatabase("foo");
    let se = SerializedError::from(&ec);

    let ec2 = ErrorCode::from(&se);
    assert_eq!(ec.code(), ec2.code());
    assert_eq!(ec.message(), ec2.message());
    assert_eq!(ec.to_string(), ec2.to_string());
    assert_eq!(ec.backtrace_str(), ec2.backtrace_str());
}

#[test]
fn test_from_and_to_status() -> anyhow::Result<()> {
    use databend_common_exception::exception::*;
    let e = ErrorCode::IllegalDataType("foo");
    let status: Status = e.into();
    assert_eq!(Code::Unknown, status.code());

    // Only compare the code and message. Discard backtrace.
    assert_eq!(
        r#"{"code":1007,"name":"IllegalDataType","message":"foo""#.as_bytes(),
        &status.details()[..53]
    );

    {
        // test from Status

        let e2: ErrorCode = status.into();

        assert_eq!(1007, e2.code());
        assert_eq!("foo", e2.message());
    }

    // test empty details
    let status1 = Status::unknown("foo");
    assert_eq!(r#""#.as_bytes(), status1.details());

    {
        // test from Status
        let e1: ErrorCode = status1.into();
        assert_eq!(1067, e1.code());
        assert!(e1.message().contains("foo"));
    }

    Ok(())
}
