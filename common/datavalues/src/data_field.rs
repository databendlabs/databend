// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::convert::TryInto;

use common_arrow::arrow::datatypes::Field as ArrowField;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataType;
#[derive(
    serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Hash, Eq, PartialOrd, Ord,
)]
pub struct DataField {
    name: String,
    data_type: DataType,
    nullable: bool,
}

impl DataField {
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        DataField {
            name: name.to_string(),
            data_type,
            nullable,
        }
    }
    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    /// Check to see if `self` is a superset of `other` field. Superset is defined as:
    ///
    /// * if nullability doesn't match, self needs to be nullable
    /// * self.metadata is a superset of other.metadata
    /// * all other fields are equal
    pub fn contains(&self, other: &DataField) -> bool {
        if self.name != other.name || self.data_type != other.data_type {
            return false;
        }

        if self.nullable != other.nullable && !self.nullable {
            return false;
        }
        true
    }

    pub fn to_arrow(&self) -> ArrowField {
        ArrowField::new(&self.name, self.data_type.to_arrow(), self.nullable)
    }
}

impl TryFrom<&ArrowField> for DataField {
    type Error = ErrorCode;
    fn try_from(f: &ArrowField) -> Result<Self> {
        Ok(DataField::new(
            f.name(),
            f.data_type().try_into()?,
            f.is_nullable(),
        ))
    }
}
