// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//! Comparison operations on DataColumn.

use common_exception::Result;

use crate::prelude::*;
use crate::DataValueComparisonOperator;

macro_rules! apply_cmp {
    ($self: ident, $rhs: ident, $op: ident) => {{
        let lhs = $self.to_minimal_array()?;
        let rhs = $rhs.to_minimal_array()?;

        let result = lhs.$op(&rhs)?;
        let result: DataColumn = result.into_series().into();
        Ok(result.resize_constant($self.len()))
    }};
}

impl DataColumn {
    #[allow(unused)]
    pub fn compare(&self, op: DataValueComparisonOperator, rhs: &DataColumn) -> Result<DataColumn> {
        match op {
            DataValueComparisonOperator::Eq => apply_cmp! {self, rhs, eq},
            DataValueComparisonOperator::Lt => apply_cmp! {self, rhs, lt},
            DataValueComparisonOperator::LtEq => apply_cmp! {self, rhs, lt_eq},
            DataValueComparisonOperator::Gt => apply_cmp! {self, rhs, gt},
            DataValueComparisonOperator::GtEq => apply_cmp! {self, rhs, gt_eq},
            DataValueComparisonOperator::NotEq => apply_cmp! {self, rhs, neq},
            DataValueComparisonOperator::Like => apply_cmp! {self, rhs, like},
            DataValueComparisonOperator::NotLike => apply_cmp! {self, rhs, nlike},
        }
    }
}

impl PartialEq for &DataColumn {
    fn eq(&self, other: &Self) -> bool {
        let result = self.compare(DataValueComparisonOperator::Eq, other);
        match result {
            Ok(v) => v.to_array().unwrap().bool().unwrap().all_true(),
            Err(_) => false,
        }
    }
}
