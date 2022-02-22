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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use super::cast_column_field;

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

    // Is the Monotonicity from constant value
    pub is_constant: bool,

    pub left: Option<ColumnWithField>,

    pub right: Option<ColumnWithField>,
}

impl Monotonicity {
    /// Create a default Monotonicity for non-monotonic and non-constant expression/function.
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

    /// Create a Monotonicity, with input parameter field. The left and right field are None.
    pub fn create(is_monotonic: bool, is_positive: bool, is_constant: bool) -> Self {
        Monotonicity {
            is_monotonic,
            is_positive,
            is_constant,
            left: None,
            right: None,
        }
    }

    /// Create a Monotonicity for constant expression, with 'is_constant' is true.
    pub fn create_constant() -> Self {
        Monotonicity {
            is_monotonic: true,
            is_positive: true,
            is_constant: true, // indicating this is a constant value
            left: None,
            right: None,
        }
    }

    /// Clone from a Monotonicity, but with left/right field None.
    pub fn clone_without_range(mono: &Monotonicity) -> Self {
        Monotonicity {
            is_monotonic: mono.is_monotonic,
            is_positive: mono.is_positive,
            is_constant: mono.is_constant,
            left: None,
            right: None,
        }
    }

    /// Check whether the range cross zero.
    /// 1 means the range interval [left, right] >= 0.
    /// -1 means the range interval [left, right] <= 0.
    /// 0 means the range interval [left, right] covering 0.
    pub fn compare_with_zero(&self) -> Result<i8> {
        if !self.is_monotonic {
            return Err(ErrorCode::UnknownException("Request Monotonicity function"));
        }

        let (min, max) = if self.is_positive {
            (self.left.clone(), self.right.clone())
        } else {
            (self.right.clone(), self.left.clone())
        };

        if let (Some(max), Some(min)) = (max, min) {
            let col = cast_column_field(&min, &f64::to_data_type())?;
            let min_val = col.get_f64(0)?;

            if min_val >= 0.0 {
                return Ok(1);
            }

            if self.is_constant {
                return Ok(-1);
            }

            let col = cast_column_field(&max, &f64::to_data_type())?;
            let max_val = col.get_f64(0)?;

            if max_val <= 0.0 {
                return Ok(-1);
            }
        }
        Ok(0)
    }
}
