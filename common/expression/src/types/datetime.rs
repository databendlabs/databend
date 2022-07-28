// Copyright 2022 Datafuse Labs.
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
use std::marker::PhantomData;
use std::ops::Range;

use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::types::NativeType;

use crate::property::Domain;
use crate::property::FloatDomain;
use crate::property::IntDomain;
use crate::property::UIntDomain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::util::buffer_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DateType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntervalType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampType;

impl Number for DateType {
    type Storage = i32;
    type Domain = IntDomain;

    fn data_type() -> DataType {
        DataType::Date
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_int32().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_int32().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_int().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Int32(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Int32(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Int(domain)
    }

    fn full_domain() -> Self::Domain {
        IntDomain {
            min: i32::MIN as i64,
            max: i32::MAX as i64,
        }
    }
}

impl Number for IntervalType {
    type Storage = i64;
    type Domain = IntDomain;

    fn data_type() -> DataType {
        DataType::Interal
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_int64().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_int64().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_int().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Int64(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Int64(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Int(domain)
    }

    fn full_domain() -> Self::Domain {
        IntDomain {
            min: i64::MIN,
            max: i64::MAX,
        }
    }
}

impl Number for TimestampType {
    type Storage = i64;
    type Domain = IntDomain;

    fn data_type() -> DataType {
        DataType::Timestamp
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_int64().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_int64().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_int().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Int64(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Int64(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Int(domain)
    }

    fn full_domain() -> Self::Domain {
        IntDomain {
            min: i64::MIN,
            max: i64::MAX,
        }
    }
}