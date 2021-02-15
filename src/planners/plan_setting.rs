// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::sync::Arc;

use crate::datavalues::{DataSchema, DataSchemaRef};

#[derive(Clone)]
pub struct VarValue {
    pub variable: String,
    pub value: String,
}

#[derive(Clone)]
pub struct SettingPlan {
    pub vars: Vec<VarValue>,
}

impl SettingPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
