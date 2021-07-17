// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::*;

#[test]
fn test_plan_extras() -> Result<()> {
    let extras = Extras::default();
    let expect = "Extras { projection: None, filters: [], limit: None }";
    let actual = format!("{:?}", extras);
    assert_eq!(expect, actual);
    Ok(())
}
