// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::compute;
use common_exception::Result;

use crate::DFBooleanArray;

impl DFBooleanArray {
    pub fn and(&self, rhs: &DFBooleanArray) -> Result<Self> {
        let result = compute::and(self.downcast_ref(), rhs.downcast_ref())?;
        Ok(DFBooleanArray::from_arrow_array(result))
    }

    pub fn or(&self, rhs: &DFBooleanArray) -> Result<Self> {
        let result = compute::or(self.downcast_ref(), rhs.downcast_ref())?;
        Ok(DFBooleanArray::from_arrow_array(result))
    }

    pub fn not(&self) -> Result<Self> {
        let result = compute::not(self.downcast_ref())?;
        Ok(DFBooleanArray::from_arrow_array(result))
    }

    /// Check if all values are true
    pub fn all_true(&self) -> bool {
        let mut values = self.downcast_iter();
        values.all(|f| f.unwrap_or(false))
    }

    /// Check if all values are false
    pub fn all_false(&self) -> bool {
        let mut values = self.downcast_iter();
        values.all(|f| !f.unwrap_or(true))
    }
}
