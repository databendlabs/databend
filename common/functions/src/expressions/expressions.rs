// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::expressions::SubstringFunction;
use crate::FactoryFuncRef;

#[derive(Clone)]
pub struct ExpressionFunction;

impl ExpressionFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("substring", SubstringFunction::try_create);
        Ok(())
    }
}
