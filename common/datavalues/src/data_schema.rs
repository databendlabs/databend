// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow;

use crate::DataField;

pub type DataSchema = arrow::datatypes::Schema;
pub type DataSchemaRef = arrow::datatypes::SchemaRef;

pub struct DataSchemaRefExt;
impl DataSchemaRefExt {
    pub fn create_with_metadata(fields: Vec<DataField>) -> DataSchemaRef {
        // Custom metadata is for deser_json, or it will returns error: value: Error("missing field `metadata`")
        // https://github.com/apache/arrow-rs/issues/241
        let metadata: HashMap<String, String> = [("k".to_string(), "v".to_string())]
            .iter()
            .cloned()
            .collect();
        Arc::new(DataSchema::new_with_metadata(fields, metadata))
    }
}
