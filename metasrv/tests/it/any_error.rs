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

use std::error::Error;
use std::fmt;

use anyhow::Context;
use databend_meta::any_error::AnyError;

#[test]
fn test_any_error() -> anyhow::Result<()> {
    // build from std Error

    let fmt_err = fmt::Error {};

    let ae = AnyError::new(&fmt_err);

    let want_str = "core::fmt::Error: an error occurred when formatting an argument";
    assert_eq!(want_str, ae.to_string());
    assert!(ae.source().is_none());
    assert!(!ae.backtrace().unwrap().is_empty());

    // debug output with backtrace

    let debug_str = format!("{:?}", ae);

    assert!(debug_str.starts_with(want_str));
    assert!(debug_str.contains("test_any_error"));

    // chained errors

    let err1 = anyhow::anyhow!("err1");
    let err2 = Err::<(), anyhow::Error>(err1).context("err2");

    let ae = AnyError::from(err2.unwrap_err());

    assert_eq!("err2 source: err1", ae.to_string());
    let src = ae.source().unwrap();
    assert_eq!("err1", src.to_string());
    assert!(!ae.backtrace().unwrap().is_empty());

    // build AnyError from AnyError

    let ae2 = AnyError::new(&ae);
    assert_eq!("err2 source: err1", ae.to_string());

    let ser = serde_json::to_string(&ae2)?;
    let de: AnyError = serde_json::from_str(&ser)?;

    assert_eq!("err2 source: err1", de.to_string());

    Ok(())
}
