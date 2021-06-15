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
    pub fn empty() -> Self {
        Self { fields: vec![] }
    }

    fn new(fields: Vec<DataField>) -> Self {
        Self { fields }
    }

    /// Returns an immutable reference of the vector of `Field` instances.
    #[inline]
    pub const fn fields(&self) -> &Vec<DataField> {
        &self.fields
    }

    /// Returns an immutable reference of a specific `Field` instance selected using an
    /// offset within the internal `fields` vector.
    pub fn field(&self, i: usize) -> &DataField {
        &self.fields[i]
    }

    /// Returns an immutable reference of a specific `Field` instance selected by name.
    pub fn field_with_name(&self, name: &str) -> Result<&DataField> {
        Ok(&self.fields[self.index_of(name)?])
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, name: &str) -> Result<usize> {
        for i in 0..self.fields.len() {
            if self.fields[i].name() == name {
                return Ok(i);
            }
        }
        let valid_fields: Vec<String> = self.fields.iter().map(|f| f.name().clone()).collect();
        Err(ErrorCode::BadArguments(format!(
            "Unable to get field named \"{}\". Valid fields: {:?}",
            name, valid_fields
        )))
    }

    /// Look up a column by name and return a immutable reference to the column along with
    /// its index.
    pub fn column_with_name(&self, name: &str) -> Option<(usize, &DataField)> {
        self.fields
            .iter()
            .enumerate()
            .find(|&(_, c)| c.name() == name)
    }

    /// Check to see if `self` is a superset of `other` schema. Here are the comparision rules:
    pub fn contains(&self, other: &DataSchema) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }

        for (i, field) in other.fields.iter().enumerate() {
            if !self.fields[i].contains(field) {
                return false;
            }
        }
        true
    }

    pub fn to_arrow(&self) -> ArrowSchema {
        let fields = self
            .fields()
            .iter()
            .map(|f| f.to_arrow())
            .collect::<Vec<_>>();

        ArrowSchema::new(fields)
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
            .map(|arrow_f| arrow_f.try_into())
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
