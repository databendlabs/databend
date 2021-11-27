// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::DataColumnWithField;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;
use dyn_clone::DynClone;

use crate::scalars::ComparisonGtEqFunction;
use crate::scalars::ComparisonLtEqFunction;

#[derive(Clone)]
pub struct Monotonicity {
    // Is the function monotonic (non-decreasing or non-increasing).
    // True means the expression/function must be monotonic.
    // False means either unknown or not monotonic.
    pub is_monotonic: bool,

    // Field for indicating monotonic increase or decrease
    //   1. is_positive=true means non-decreasing
    //   2. is_positive=false means non-increasing
    // when is_monotonic is false, just ignore the is_positive information.
    pub is_positive: bool,

    // Is the monotonicity from constant value
    pub is_constant: bool,

    pub left: Option<DataColumnWithField>,

    pub right: Option<DataColumnWithField>,
}

impl Monotonicity {
    /// Create a default monotonicity for non-monotonic and non-constant expression/function.
    /// The fields 'is_monotonic' and 'is_constant' are both false.
    /// The fields 'left' and 'right' boundaries are both None.
    pub fn default() -> Self {
        Monotonicity {
            is_monotonic: false,
            is_positive: true,
            is_constant: false,
            left: None,
            right: None,
        }
    }

    /// Create a monotonicity, with input parameter field. The left and right field are None.
    pub fn create(is_monotonic: bool, is_positive: bool, is_constant: bool) -> Self {
        Monotonicity {
            is_monotonic,
            is_positive,
            is_constant,
            left: None,
            right: None,
        }
    }

    /// Create a monotonicity for constant expression, with 'is_constant' is true.
    pub fn create_constant() -> Self {
        Monotonicity {
            is_monotonic: true,
            is_positive: true,
            is_constant: true, // indicating this is a constant value
            left: None,
            right: None,
        }
    }

    /// Clone from a monotonicity, but with left/right field None.
    pub fn clone_without_range(mono: &Monotonicity) -> Self {
        Monotonicity {
            is_monotonic: mono.is_monotonic,
            is_positive: mono.is_positive,
            is_constant: mono.is_constant,
            left: None,
            right: None,
        }
    }

    /// Compare self.left and self.right, return true when left >= right; false when left < right.
    /// If either left or right is None, return None.
    pub fn compare_left_right(&self) -> Result<Option<bool>> {
        if let (Some(left_val), Some(right_val)) = (&self.left, &self.right) {
            let cmp_func = ComparisonGtEqFunction::try_create_func(">=")?;
            let res_col = cmp_func.eval(&[left_val.clone(), right_val.clone()], 1)?;
            let res = res_col.try_get(0)?.as_bool()?;
            Ok(Some(res))
        } else {
            Ok(None)
        }
    }

    /// Compare self.left, self.right with the target, return true is cmp_func
    pub fn compare_range(
        &self,
        target: DataColumnWithField,
        cmp_func: Box<dyn Function>,
    ) -> Result<bool> {
        if let (Some(left_val), Some(right_val)) = (self.left.clone(), self.right.clone()) {
            let left_res_col = cmp_func.eval(&[left_val, target.clone()], 1)?;
            let right_res_col = cmp_func.eval(&[right_val, target], 1)?;

            let left_res = left_res_col.try_get(0)?.as_bool()?;
            let right_res = right_res_col.try_get(0)?.as_bool()?;

            if left_res && right_res {
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            // No full boundary information found, return false.
            Ok(false)
        }
    }

    /// Check whether the range greater than or equal to the target.
    /// True means the min(left, right) >= the target.
    /// False means the range interval may be unknown, covering the target or both < target.
    pub fn gt_eq(&self, target: DataColumnWithField) -> Result<bool> {
        match self.compare_left_right()? {
            None => Ok(false),
            Some(val) => {
                let min_val = if val {
                    self.right.clone().unwrap()
                } else {
                    self.left.clone().unwrap()
                };
                let cmp_func = ComparisonGtEqFunction::try_create_func(">=")?;
                let res_col = cmp_func.eval(&[min_val, target], 1)?;
                let res = res_col.try_get(0)?.as_bool()?;
                Ok(res)
            }
        }
    }

    /// Check whether the range less than or equal to the target.
    /// True means the max(left, right) <= the target.
    /// False means the range interval may be unknown, covering the target or > target.
    pub fn lt_eq(&self, target: DataColumnWithField) -> Result<bool> {
        match self.compare_left_right()? {
            None => Ok(false),
            Some(val) => {
                let max_val = if val {
                    self.left.clone().unwrap()
                } else {
                    self.right.clone().unwrap()
                };
                let cmp_func = ComparisonLtEqFunction::try_create_func("<=")?;
                let res_col = cmp_func.eval(&[max_val, target], 1)?;
                let res = res_col.try_get(0)?.as_bool()?;
                Ok(res)
            }
        }
    }

    /// Check whether the range greater than or equal to zero.
    /// True means the range interval [left, right] or [right, left] >= 0.
    /// False means the range interval may be unknown, covering 0 or < 0.
    pub fn gt_eq_zero(&self) -> Result<bool> {
        let zero = DataColumn::Constant(DataValue::Int8(Some(0)), 1);
        let zero_column_field =
            DataColumnWithField::new(zero, DataField::new("", DataType::Int8, false));
        self.gt_eq(zero_column_field)
    }

    /// Check whether the range less than or equal to zero.
    /// True means the range interval [left, right] or [right, left] <= 0.
    /// False means the range interval may be unknown, covering 0 or >  0.
    pub fn lt_eq_zero(&self) -> Result<bool> {
        let zero = DataColumn::Constant(DataValue::Int8(Some(0)), 1);
        let zero_column_field =
            DataColumnWithField::new(zero, DataField::new("", DataType::Int8, false));
        self.lt_eq(zero_column_field)
    }
}

pub trait Function: fmt::Display + Sync + Send + DynClone {
    fn name(&self) -> &str;

    fn num_arguments(&self) -> usize {
        0
    }

    // (1, 2) means we only accept [1, 2] arguments
    // None means it's not variadic function
    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        None
    }

    /// Calculate the monotonicity from arguments' monotonicity information.
    /// The input should be argument's monotonicity. For binary function it should be an
    /// array of left expression's monotonicity and right expression's monotonicity.
    /// For unary function, the input should be an array of the only argument's monotonicity.
    /// The returned monotonicity should have 'left' and 'right' fields None -- the boundary
    /// calculation relies on the function.eval method.
    fn get_monotonicity(&self, _args: &[Monotonicity]) -> Result<Monotonicity> {
        Ok(Monotonicity::default())
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType>;
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool>;
    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn>;
}
