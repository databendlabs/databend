// Copyright 2021 Datafuse Labs
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

use std::cmp::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::Number;
use databend_common_expression::Scalar;
use databend_common_sql::plans::WindowFuncFrameBound;

#[derive(Debug, PartialEq)]
pub enum FrameBound<T: Number> {
    CurrentRow,
    Preceding(Option<T>),
    Following(Option<T>),
}

impl<T: Number> FrameBound<T> {
    pub fn get_inner(&self) -> Option<T> {
        match self {
            FrameBound::Preceding(Some(v)) => Some(*v),
            FrameBound::Following(Some(v)) => Some(*v),
            _ => None,
        }
    }
}

impl<T: Number> TryFrom<&WindowFuncFrameBound> for FrameBound<T> {
    type Error = ErrorCode;
    fn try_from(value: &WindowFuncFrameBound) -> Result<Self> {
        match value {
            WindowFuncFrameBound::CurrentRow => Ok(FrameBound::CurrentRow),
            WindowFuncFrameBound::Preceding(v) => Ok(FrameBound::Preceding(
                v.as_ref()
                    .map(|v| {
                        if let Scalar::Number(scalar) = v {
                            T::try_downcast_scalar(scalar).ok_or_else(|| {
                                ErrorCode::Internal(format!("number, but got {:?}", v))
                            })
                        } else {
                            Err(ErrorCode::Internal(format!("number, but got {:?}", v)))
                        }
                    })
                    .transpose()?,
            )),
            WindowFuncFrameBound::Following(v) => Ok(FrameBound::Following(
                v.as_ref()
                    .map(|v| {
                        if let Scalar::Number(scalar) = v {
                            T::try_downcast_scalar(scalar).ok_or_else(|| {
                                ErrorCode::Internal(format!("number, but got {:?}", v))
                            })
                        } else {
                            Err(ErrorCode::Internal(format!("number, but got {:?}", v)))
                        }
                    })
                    .transpose()?,
            )),
        }
    }
}

impl<T: Number> PartialOrd for FrameBound<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (FrameBound::CurrentRow, FrameBound::CurrentRow) => Some(Ordering::Equal),
            (FrameBound::CurrentRow, FrameBound::Preceding(_)) => Some(Ordering::Greater),
            (FrameBound::CurrentRow, FrameBound::Following(_)) => Some(Ordering::Less),
            (FrameBound::Preceding(_), FrameBound::CurrentRow) => Some(Ordering::Less),
            (FrameBound::Preceding(None), FrameBound::Preceding(None)) => Some(Ordering::Equal),
            (FrameBound::Preceding(None), FrameBound::Preceding(_)) => Some(Ordering::Less),
            (FrameBound::Preceding(Some(_)), FrameBound::Preceding(None)) => {
                Some(Ordering::Greater)
            }
            (FrameBound::Preceding(Some(lhs)), FrameBound::Preceding(Some(rhs))) => {
                lhs.partial_cmp(rhs).map(Ordering::reverse)
            }
            (FrameBound::Preceding(_), FrameBound::Following(_)) => Some(Ordering::Less),
            (FrameBound::Following(_), FrameBound::CurrentRow) => Some(Ordering::Greater),
            (FrameBound::Following(_), FrameBound::Preceding(_)) => Some(Ordering::Greater),
            (FrameBound::Following(None), FrameBound::Following(None)) => Some(Ordering::Equal),
            (FrameBound::Following(None), FrameBound::Following(_)) => Some(Ordering::Greater),
            (FrameBound::Following(Some(_)), FrameBound::Following(None)) => Some(Ordering::Less),
            (FrameBound::Following(Some(lhs)), FrameBound::Following(Some(rhs))) => {
                lhs.partial_cmp(rhs)
            }
        }
    }
}
