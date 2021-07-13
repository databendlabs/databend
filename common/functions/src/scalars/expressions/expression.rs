// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use common_datavalues::DataType;
use common_exception::Result;

use crate::scalars::CastFunction;
use crate::scalars::FactoryFuncRef;

#[derive(Clone)]
pub struct ToCastFunction;

impl ToCastFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("toint8", |display_name| {
            CastFunction::create(display_name.to_string(), DataType::Int8)
        });
        map.insert("toint16", |display_name| {
            CastFunction::create(display_name.to_string(), DataType::Int8)
        });
        map.insert("toint32", |display_name| {
            CastFunction::create(display_name.to_string(), DataType::Int8)
        });
        map.insert("toint64", |display_name| {
            CastFunction::create(display_name.to_string(), DataType::Int8)
        });
        Ok(())
    }
}
