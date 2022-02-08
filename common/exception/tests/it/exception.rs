// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::SerializedError;
use tonic::Code;
use tonic::Status;

#[test]
fn test_format_with_error_codes() {
    use common_exception::exception::*;

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
        "Code: 1067, displayText = test message 1."
    );
    assert_eq!(
        format!("{}", ErrorCode::UnknownException("test message 2")),
        "Code: 1067, displayText = test message 2."
    );
}

#[test]
fn test_error_code() {
    use common_exception::exception::*;

    let err = ErrorCode::UnknownException("test message 1");

    assert_eq!(err.code(), ErrorCode::unknown_exception_code(),);
    assert_eq!(err.code(), ErrorCode::UnknownExceptionCode(),);
}

#[test]
fn test_derive_from_std_error() {
    use common_exception::exception::ErrorCode;
    use common_exception::exception::ToErrorCode;

    let fmt_rst: std::result::Result<(), std::fmt::Error> = Err(std::fmt::Error {});

    let rst1: common_exception::exception::Result<()> =
        fmt_rst.map_err_to_code(ErrorCode::UnknownException, || 123);

    assert_eq!(
        "Code: 1067, displayText = 123, cause: an error occurred when formatting an argument.",
        format!("{}", rst1.as_ref().unwrap_err())
    );

    let rst2: common_exception::exception::Result<()> =
        rst1.map_err_to_code(ErrorCode::Ok, || "wrapper");

    assert_eq!(
        "Code: 0, displayText = wrapper, cause: Code: 1067, displayText = 123, cause: an error occurred when formatting an argument..",
        format!("{}", rst2.as_ref().unwrap_err())
    );
}

#[test]
fn test_derive_from_display() {
    use common_exception::exception::ErrorCode;
    use common_exception::exception::ToErrorCode;

    let rst: std::result::Result<(), u64> = Err(3);

    let rst1: common_exception::exception::Result<()> =
        rst.map_err_to_code(ErrorCode::UnknownException, || 123);

    assert_eq!(
        "Code: 1067, displayText = 123, cause: 3.",
        format!("{}", rst1.as_ref().unwrap_err())
    );
}

#[test]
fn test_from_and_to_serialized_error() {
    let ec = ErrorCode::UnknownDatabase("foo");
    let se: SerializedError = ec.clone().into();

    let ec2: ErrorCode = se.into();
    assert_eq!(ec.code(), ec2.code());
    assert_eq!(ec.message(), ec2.message());
    assert_eq!(format!("{}", ec), format!("{}", ec2));
    assert_eq!(ec.backtrace_str(), ec2.backtrace_str());
}

#[test]
fn test_from_and_to_status() -> anyhow::Result<()> {
    use common_exception::exception::*;
    let e = ErrorCode::IllegalDataType("foo");
    let status: Status = e.into();
    assert_eq!(Code::Unknown, status.code());

    // Only compare the code and message. Discard backtrace.
    assert_eq!(
        r#"{"code":1007,"message":"foo","#.as_bytes(),
        &status.details()[..29]
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
        assert_eq!("foo", e1.message());
    }

    Ok(())
}
