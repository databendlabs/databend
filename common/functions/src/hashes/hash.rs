// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::hashes::siphash::SipHashFunction;
use crate::FactoryFuncRef;

#[derive(Clone)]
pub struct HashesFunction;

impl HashesFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("siphash", SipHashFunction::try_create);
        Ok(())
    }
}
