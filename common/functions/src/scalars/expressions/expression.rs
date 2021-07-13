// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use common_exception::Result;

use crate::scalars::CastFunctionTemplate;
use crate::scalars::FactoryFuncRef;

#[derive(Clone)]
pub struct ToCastFunction;

impl ToCastFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("toint8", CastFunctionTemplate::try_create_func);
        map.insert("toint16", CastFunctionTemplate::try_create_func);
        map.insert("toint32", CastFunctionTemplate::try_create_func);
        map.insert("toint64", CastFunctionTemplate::try_create_func);
        Ok(())
    }
}
