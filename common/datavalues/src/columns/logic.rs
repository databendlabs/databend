// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//! Comparison operations on DataColumn.

use common_exception::Result;

use crate::prelude::*;
use crate::DataValueLogicOperator;

macro_rules! apply_logic {
    ($self: ident, $rhs: ident, $op: ident) => {{
        let lhs = $self.to_minimal_array()?;

        let left = lhs.cast_with_type(&DataType::Boolean)?;
        let left = left.bool()?;

        let rhs = $rhs[0].to_minimal_array()?;
        let right = rhs.cast_with_type(&DataType::Boolean)?;
        let right = right.bool()?;

        let result = left.$op(&right)?;
        let result: DataColumn = result.into_series().into();
        Ok(result.resize_constant($self.len()))
    }};

    ($self: ident, $op: ident) => {{
        let lhs = $self.to_minimal_array()?;
        let left = lhs.cast_with_type(&DataType::Boolean)?;
        let left = left.bool()?;

        let result = left.$op()?;
        let result: DataColumn = result.into_series().into();
        Ok(result.resize_constant($self.len()))
    }};
}

impl DataColumn {
    #[allow(unused)]
    pub fn logic(&self, op: DataValueLogicOperator, rhs: &[DataColumn]) -> Result<DataColumn> {
        match op {
            DataValueLogicOperator::And => apply_logic! {self, rhs, and_kleene},
            DataValueLogicOperator::Or => apply_logic! {self, rhs, or_kleene},
            DataValueLogicOperator::Not => apply_logic! {self, not},
        }
    }
}
