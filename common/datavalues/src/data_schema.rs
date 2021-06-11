// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::convert::TryInto;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataField;

/// memory layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSchema {
    pub(crate) fields: Vec<DataField>,
}

impl DataSchema {
    fn new(fields: Vec<DataField>) -> Self {
        Self { fields }
    }
}

pub type DataSchemaRef = Arc<DataSchema>;

pub struct DataSchemaRefExt;
impl DataSchemaRefExt {
    pub fn create(fields: Vec<DataField>) -> DataSchemaRef {
        Arc::new(DataSchema::new(fields))
    }
}

impl TryFrom<&ArrowSchema> for DataSchema {
    type Error = ErrorCode;
    fn try_from(a_schema: &ArrowSchema) -> Result<Self> {
        let fields = a_schema
            .fields()
            .iter()
            .map(|arrow_f| arrow_f.try_into()?)
            .collect::<Result<Vec<_>>>()?;

        Ok(DataSchema::new(fields))
    }
}

impl TryFrom<ArrowSchema> for DataSchema {
    type Error = ErrorCode;
    fn try_from(a_schema: ArrowSchema) -> Result<Self> {
        (&a_schema).try_into()
    }
}
