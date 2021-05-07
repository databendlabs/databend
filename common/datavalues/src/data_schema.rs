// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow;

use crate::DataField;

pub type DataSchema = arrow::datatypes::Schema;
pub type DataSchemaRef = arrow::datatypes::SchemaRef;

pub struct DataSchemaRefExt;
impl DataSchemaRefExt {
    pub fn create(fields: Vec<DataField>) -> DataSchemaRef {
        Arc::new(DataSchema::new(fields))
    }
}
