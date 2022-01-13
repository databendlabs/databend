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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::Column;
use crate::ColumnRef;
use crate::ConstColumn;

// Series is a util struct to work with Column
pub struct Series;

impl Series {
    /// Get a pointer to the underlying data of this Series.
    /// Can be useful for fast comparisons.
    pub unsafe fn static_cast<T>(column: &ColumnRef) -> &T {
        let object = column.as_ref();
        &*(object as *const dyn Column as *const T)
    }

    // TODO use typeid
    pub fn check_get<T: 'static + Column>(column: &ColumnRef) -> Result<&T> {
        let arr = column.as_any().downcast_ref::<T>().ok_or_else(|| {
            ErrorCode::UnknownColumn(format!(
                "downcast column error, column type: {:?}",
                column.data_type()
            ))
        });
        arr
    }

    pub fn convert_full_column(column: &ColumnRef) -> ColumnRef {
        if let Ok(c) = Self::check_get::<ConstColumn>(column) {
            c.convert_full_column()
        } else {
            column.clone()
        }
    }
}
