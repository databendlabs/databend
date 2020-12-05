// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datavalues::DataSchemaRef;

#[derive(Clone)]
pub struct EmptyPlan {
    pub(crate) schema: DataSchemaRef,
}

impl EmptyPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
