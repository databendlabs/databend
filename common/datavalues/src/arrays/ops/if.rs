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

use common_arrow::arrow::compute::if_then_else;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;

macro_rules! impl_if_common {
    ($predicate:ident, $lhs:ident, $rhs:ident) => {{
        match ($predicate.len(), $lhs.len(), $rhs.len()) {
            (1, b, c) if b == c || b == 1 || c == 1 => {
                let pre = $predicate.get(0);
                match pre {
                    Some(true) => Ok($lhs.clone()),
                    None | Some(false) => Ok($rhs.clone()),
                }
            }
            (a, 1, c) if a == c => {
                let left = $lhs.get(0);
                Ok($predicate
                    .into_iter()
                    .zip($rhs.into_iter())
                    .map(|(pre, right)| match pre {
                        Some(true) => left,
                        None | Some(false) => right.copied(),
                    })
                    .collect())
            }
            (_, 1, 1) => {
                let left = $lhs.get(0);
                let right = $rhs.get(0);
                Ok($predicate
                    .into_iter()
                    .map(|pre| match pre {
                        Some(true) => left,
                        None | Some(false) => right,
                    })
                    .collect())
            }
            (a, b, 1) if a == b => {
                let right = $rhs.get(0);
                Ok($predicate
                    .into_iter()
                    .zip($lhs.into_iter())
                    .map(|(pre, left)| match pre {
                        Some(true) => left.copied(),
                        None | Some(false) => right,
                    })
                    .collect())
            }
            (_, _, _) => {
                let result =
                    if_then_else::if_then_else($predicate.inner(), &($lhs.array), &($rhs.array))?;
                Ok(Self::from_arrow_array(result.as_ref()))
            }
        }
    }};
}

macro_rules! impl_if_bool_string {
    ($predicate:ident, $lhs:ident, $rhs:ident) => {{
        match ($predicate.len(), $lhs.len(), $rhs.len()) {
            (1, b, c) if b == c || b == 1 || c == 1 => {
                let pre = $predicate.get(0);
                match pre {
                    Some(true) => Ok($lhs.clone()),
                    None | Some(false) => Ok($rhs.clone()),
                }
            }
            (a, 1, c) if a == c => {
                let left = $lhs.get(0);
                Ok($predicate
                    .into_iter()
                    .zip($rhs.into_iter())
                    .map(|(pre, right)| match pre {
                        Some(true) => left,
                        None | Some(false) => right,
                    })
                    .collect())
            }
            (_, 1, 1) => {
                let left = $lhs.get(0);
                let right = $rhs.get(0);
                Ok($predicate
                    .into_iter()
                    .map(|pre| match pre {
                        Some(true) => left,
                        None | Some(false) => right,
                    })
                    .collect())
            }
            (a, b, 1) if a == b => {
                let right = $rhs.get(0);
                Ok($predicate
                    .into_iter()
                    .zip($lhs.into_iter())
                    .map(|(pre, left)| match pre {
                        Some(true) => left,
                        None | Some(false) => right,
                    })
                    .collect())
            }
            (_, _, _) => {
                let result =
                    if_then_else::if_then_else($predicate.inner(), &($lhs.array), &($rhs.array))?;
                Ok(Self::from_arrow_array(result.as_ref()))
            }
        }
    }};
}

pub trait ArrayIf: Debug {
    fn if_then_else(&self, _rhs: &Self, _predicate: &DFBooleanArray) -> Result<Self>
    where Self: std::marker::Sized {
        Err(ErrorCode::BadDataValueType(format!(
            "Unexpected type:{:?}  of function if",
            self,
        )))
    }
}

impl<T> ArrayIf for DFPrimitiveArray<T>
where T: DFPrimitiveType
{
    fn if_then_else(&self, rhs: &Self, predicate: &DFBooleanArray) -> Result<Self> {
        impl_if_common! {predicate, self, rhs}
    }
}

impl ArrayIf for DFBooleanArray {
    fn if_then_else(&self, rhs: &Self, predicate: &DFBooleanArray) -> Result<Self> {
        impl_if_bool_string! {predicate, self, rhs}
    }
}

impl ArrayIf for DFStringArray {
    fn if_then_else(&self, rhs: &Self, predicate: &DFBooleanArray) -> Result<Self> {
        impl_if_bool_string! {predicate, self, rhs}
    }
}

impl ArrayIf for DFNullArray {
    fn if_then_else(&self, _rhs: &Self, _predicate: &DFBooleanArray) -> Result<Self> {
        Ok(self.clone())
    }
}

impl ArrayIf for DFListArray {}
impl ArrayIf for DFStructArray {}
