// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct EmptyPlan {
    pub schema: DataSchemaRef,
    pub is_cluster: bool,
}

impl EmptyPlan {
    pub fn create() -> Self {
        EmptyPlan {
            schema: DataSchemaRef::new(DataSchema::empty()),
            is_cluster: false,
        }
    }

    pub fn cluster() -> Self {
        EmptyPlan {
            schema: DataSchemaRef::new(DataSchema::empty()),
            is_cluster: true,
        }
    }

    pub fn create_with_schema(schema: DataSchemaRef) -> Self {
        EmptyPlan {
            schema,
            is_cluster: false,
        }
    }
}

impl EmptyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
