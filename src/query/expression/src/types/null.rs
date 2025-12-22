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
use super::nullable::NullableDomain;
use crate::ColumnBuilder;
use crate::ScalarRef;
use crate::property::Domain;
use crate::values::Column;
use crate::values::Scalar;

#[derive(Debug, PartialEq, Clone)]
pub struct CoreNull;

pub type NullType = ZeroSizeValueType<CoreNull>;

impl ZeroSizeType for CoreNull {
    fn downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Option<()> {
        match scalar {
            ScalarRef::Null => Some(()),
            _ => None,
        }
    }

    fn upcast_scalar() -> Scalar {
        Scalar::Null
    }

    fn downcast_column(col: &Column) -> Option<usize> {
        match col {
            Column::Null { len } => Some(*len),
            _ => None,
        }
    }

    fn upcast_column(len: usize) -> Column {
        Column::Null { len }
    }

    fn downcast_domain(domain: &Domain) -> Option<()> {
        match domain {
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }) => Some(()),
            _ => None,
        }
    }

    fn upcast_domain() -> Domain {
        Domain::Nullable(NullableDomain {
            has_null: true,
            value: None,
        })
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut usize> {
        match builder {
            ColumnBuilder::Null { len } => Some(len),
            _ => None,
        }
    }

    fn downcast_owned_builder(builder: ColumnBuilder) -> Option<usize> {
        match builder {
            ColumnBuilder::Null { len } => Some(len),
            _ => None,
        }
    }

    fn upcast_column_builder(len: usize) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Null { len })
    }
}

impl ArgType for NullType {
    fn data_type() -> DataType {
        DataType::Null
    }

    fn full_domain() -> Self::Domain {}
}
