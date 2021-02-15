// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::error::FuseQueryResult;
use crate::functions::udfs::DatabaseFunction;
use crate::functions::udfs::ToTypeNameFunction;
use crate::functions::udfs::UDFExampleFunction;
use crate::functions::FactoryFuncRef;

#[derive(Clone)]
pub struct UDFFunction;

impl UDFFunction {
    pub fn register(map: FactoryFuncRef) -> FuseQueryResult<()> {
        let mut map = map.as_ref().lock()?;
        map.insert("example", UDFExampleFunction::try_create);
        map.insert("totypename", ToTypeNameFunction::try_create);
        map.insert("database", DatabaseFunction::try_create);
        Ok(())
    }
}
