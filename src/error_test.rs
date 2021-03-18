// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_build_errors() {
    use crate::error::*;

    let e = FuseQueryError::build_internal_error("Tests".into());
    assert_eq!(
        format!("{:?}", e),
        "Internal { message: \"Tests\", backtrace: Backtrace(()) }"
    );

    let e = FuseQueryError::build_plan_error("Tests".into());
    assert_eq!(
        format!("{:?}", e),
        "Plan { message: \"Tests\", backtrace: Backtrace(()) }"
    );
}

#[test]
fn test_from_error() {
    use std::str::FromStr;

    use arrow::error::ArrowError;

    use crate::error::*;

    let e: FuseQueryError = ArrowError::MemoryError("Out Of Memory".into()).into();
    assert_eq!(
        format!("{:?}", e),
        "Arrow { source: MemoryError(\"Out Of Memory\"), backtrace: Backtrace(()) }"
    );

    if let Err(e) = f64::from_str("a.12") {
        let e: FuseQueryError = e.into();
        assert_eq!(
            format!("{:?}", e),
            "Internal { message: \"invalid float literal\", backtrace: Backtrace(()) }"
        );
    }
}
