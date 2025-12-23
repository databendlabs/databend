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

use super::ArgType;
use super::DataType;
use super::ZeroSizeType;
use super::ZeroSizeValueType;
use crate::ColumnBuilder;
use crate::ScalarRef;
use crate::property::Domain;
use crate::values::Column;
use crate::values::Scalar;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreEmptyMap;

pub type EmptyMapType = ZeroSizeValueType<CoreEmptyMap>;

impl ZeroSizeType for CoreEmptyMap {
    fn downcast_scalar(scalar: &ScalarRef) -> Option<()> {
        match scalar {
            ScalarRef::EmptyMap => Some(()),
            _ => None,
        }
    }

    fn downcast_column(col: &Column) -> Option<usize> {
        match col {
            Column::EmptyMap { len } => Some(*len),
            _ => None,
        }
    }

    fn downcast_domain(domain: &Domain) -> Option<()> {
        match domain {
            Domain::Map(None) => Some(()),
            _ => None,
        }
    }

    fn upcast_scalar() -> Scalar {
        Scalar::EmptyMap
    }

    fn upcast_column(len: usize) -> Column {
        Column::EmptyMap { len }
    }

    fn upcast_domain() -> Domain {
        Domain::Map(None)
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut usize> {
        match builder {
            ColumnBuilder::EmptyMap { len } => Some(len),
            _ => None,
        }
    }

    fn downcast_owned_builder(builder: ColumnBuilder) -> Option<usize> {
        match builder {
            ColumnBuilder::EmptyMap { len } => Some(len),
            _ => None,
        }
    }

    fn upcast_column_builder(len: usize) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::EmptyMap { len })
    }
}

impl ArgType for EmptyMapType {
    fn data_type() -> DataType {
        DataType::EmptyMap
    }

    fn full_domain() -> Self::Domain {}
}
