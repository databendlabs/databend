// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::{DataSchema, DataSchemaRef};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct VarValue {
    pub variable: String,
    pub value: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct SettingPlan {
    pub vars: Vec<VarValue>,
}

impl SettingPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
