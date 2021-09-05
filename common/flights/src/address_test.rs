// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::address::Address;

#[test]
fn test_serialize_address() -> Result<()> {
    assert_eq!(
        serde_json::to_string(&Address::create(&String::from("localhost:9090"))?)?,
        "\"localhost:9090\""
    );
    assert_eq!(
        serde_json::from_str::<Address>("\"localhost:9090\"")?,
        Address::create(&String::from("localhost:9090"))?
    );

    Ok(())
}
