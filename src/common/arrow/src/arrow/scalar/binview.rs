// Copyright (c) 2020 Ritchie Vink
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

use std::fmt::Debug;
use std::fmt::Formatter;

use crate::arrow::array::ViewType;
use crate::arrow::datatypes::DataType;
use crate::arrow::scalar::Scalar;

#[derive(PartialEq, Eq)]
pub struct BinaryViewScalar<T: ViewType + ?Sized> {
    value: Option<T::Owned>,
    phantom: std::marker::PhantomData<T>,
}

impl<T: ViewType + ?Sized> Debug for BinaryViewScalar<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Scalar({:?})", self.value)
    }
}

impl<T: ViewType + ?Sized> Clone for BinaryViewScalar<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            phantom: Default::default(),
        }
    }
}

impl<T: ViewType + ?Sized> BinaryViewScalar<T> {
    /// Returns a new [`BinaryViewScalar`]
    #[inline]
    pub fn new(value: Option<&T>) -> Self {
        Self {
            value: value.map(|x| x.into_owned()),
            phantom: std::marker::PhantomData,
        }
    }

    /// Returns the value irrespectively of the validity.
    #[allow(unused)]
    #[inline]
    pub fn value(&self) -> Option<&T> {
        self.value.as_ref().map(|x| x.as_ref())
    }
}

impl<T: ViewType + ?Sized> From<Option<&T>> for BinaryViewScalar<T> {
    #[inline]
    fn from(v: Option<&T>) -> Self {
        Self::new(v)
    }
}

impl<T: ViewType + ?Sized> Scalar for BinaryViewScalar<T> {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.value.is_some()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        if T::IS_UTF8 {
            &DataType::Utf8View
        } else {
            &DataType::BinaryView
        }
    }
}
