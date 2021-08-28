// Copyright 2020 Datafuse Labs.
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

use std::fmt::Debug;

use common_arrow::arrow::compute::contains::contains;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

pub trait ArrayContain: Debug {
    /// Ex: array = [1,2,3] and list = [[1,3,5], [2,6], [1,2]],
    /// then the result should be [true, true, false].
    fn contain(&self, _list: &DFListArray) -> Result<DFBooleanArray>
    where Self: std::marker::Sized {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported apply contain operation for {:?}",
            self,
        )))
    }
}

macro_rules! contain_internal {
    ($self:expr, $list_arr:expr) => {{
        assert_eq!($self.len(), $list_arr.len());
        let arrow_array = $self.downcast_ref();
        let arrow_list = $list_arr.downcast_ref();
        let arrow_res = contains(arrow_list, arrow_array)?;
        Ok(DFBooleanArray::new(arrow_res))
    }};
}

impl<T> ArrayContain for DFPrimitiveArray<T>
where T: DFPrimitiveType
{
    fn contain(&self, list: &DFListArray) -> Result<DFBooleanArray>
    where Self: std::marker::Sized {
        contain_internal!(self, list)
    }
}

impl ArrayContain for DFUtf8Array {
    fn contain(&self, list: &DFListArray) -> Result<DFBooleanArray>
    where Self: std::marker::Sized {
        contain_internal!(self, list)
    }
}
