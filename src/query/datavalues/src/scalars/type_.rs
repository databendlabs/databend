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

use std::any::Any;

use common_exception::Result;
use common_hashtable::KeysRef;
use ordered_float::OrderedFloat;

use super::column::ScalarColumn;
use crate::prelude::*;

/// Owned scalar value
/// primitive types, bool, Vec<u8> ...
pub trait Scalar: 'static + Sized + Default + Any
where for<'a> Self::ColumnType: ScalarColumn<RefItem<'a> = Self::RefType<'a>>
{
    type ColumnType: ScalarColumn<OwnedItem = Self>;
    type RefType<'a>: ScalarRef<'a, ScalarType = Self, ColumnType = Self::ColumnType>
    where Self: 'a;

    /// Viewer is associated with scalar value
    /// the big difference between column is that Viewer may be nullable && constant
    type Viewer<'a>: ScalarViewer<'a, ScalarItem = Self>
    where Self: 'a;

    type KeyType;

    /// Get a reference of the current value.
    fn as_scalar_ref(&self) -> Self::RefType<'_>;

    /// Upcast GAT type's lifetime.
    fn upcast_gat<'short, 'long: 'short>(long: Self::RefType<'long>) -> Self::RefType<'short>;

    fn try_create_viewer(col: &ColumnRef) -> Result<Self::Viewer<'_>> {
        Self::Viewer::try_create(col)
    }
}

pub trait ScalarRef<'a>: std::fmt::Debug + Clone + Copy + Send + 'a {
    type ColumnType: ScalarColumn<RefItem<'a> = Self>;
    /// The corresponding [`Scalar`] type.
    type ScalarType: Scalar<RefType<'a> = Self>;

    /// Convert the reference into an owned value.
    fn to_owned_scalar(&self) -> Self::ScalarType;

    /// Convert the reference into a key type (which is used for hashmap/hashset)
    fn to_key(&self) -> <<Self as ScalarRef<'a>>::ScalarType as Scalar>::KeyType;

    /// Whether to_owned_scalar has heap allocation which is unhandled by Bumplao
    fn has_alloc_beyond_bump() -> bool {
        false
    }
}

macro_rules! impl_primitive_scalar_type {
    ($native:ident, $key_type: ident) => {
        impl Scalar for $native {
            type ColumnType = PrimitiveColumn<$native>;
            type RefType<'a> = $native;
            type Viewer<'a> = PrimitiveViewer<'a, $native>;
            type KeyType = $key_type;

            #[inline]
            fn as_scalar_ref(&self) -> $native {
                *self
            }

            #[allow(clippy::needless_lifetimes)]
            #[inline]
            fn upcast_gat<'short, 'long: 'short>(long: $native) -> $native {
                long
            }
        }

        /// Implement [`ScalarRef`] for primitive types. Note that primitive types are both [`Scalar`] and [`ScalarRef`].
        impl<'a> ScalarRef<'a> for $native {
            type ColumnType = PrimitiveColumn<$native>;
            type ScalarType = $native;

            #[inline]
            fn to_owned_scalar(&self) -> $native {
                *self
            }

            #[inline]
            fn to_key(&self) -> $key_type {
                (*self).into()
            }
        }
    };
}

impl_primitive_scalar_type!(u8, u8);
impl_primitive_scalar_type!(u16, u16);
impl_primitive_scalar_type!(u32, u32);
impl_primitive_scalar_type!(u64, u64);
impl_primitive_scalar_type!(i8, i8);
impl_primitive_scalar_type!(i16, i16);
impl_primitive_scalar_type!(i32, i32);
impl_primitive_scalar_type!(i64, i64);

type F32 = OrderedFloat<f32>;
type F64 = OrderedFloat<f64>;

impl_primitive_scalar_type!(f32, F32);
impl_primitive_scalar_type!(f64, F64);

impl Scalar for bool {
    type ColumnType = BooleanColumn;
    type RefType<'a> = bool;
    type Viewer<'a> = BooleanViewer;
    type KeyType = bool;

    #[inline]
    fn as_scalar_ref(&self) -> bool {
        *self
    }

    #[allow(clippy::needless_lifetimes)]
    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: bool) -> bool {
        long
    }
}

impl<'a> ScalarRef<'a> for bool {
    type ColumnType = BooleanColumn;
    type ScalarType = bool;

    #[inline]
    fn to_owned_scalar(&self) -> bool {
        *self
    }

    #[inline]
    fn to_key(&self) -> bool {
        *self
    }
}

impl Scalar for Vec<u8> {
    type ColumnType = StringColumn;
    type RefType<'a> = &'a [u8];
    type Viewer<'a> = StringViewer<'a>;
    type KeyType = KeysRef;

    #[inline]
    fn as_scalar_ref(&self) -> &[u8] {
        self
    }

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: &'long [u8]) -> &'short [u8] {
        long
    }
}

impl<'a> ScalarRef<'a> for &'a [u8] {
    type ColumnType = StringColumn;
    type ScalarType = Vec<u8>;

    #[inline]
    fn to_owned_scalar(&self) -> Vec<u8> {
        self.to_vec()
    }

    #[inline]
    fn to_key(&self) -> KeysRef {
        KeysRef::create(self.as_ptr() as usize, self.len())
    }

    fn has_alloc_beyond_bump() -> bool {
        true
    }
}

impl Scalar for VariantValue {
    type ColumnType = ObjectColumn<VariantValue>;
    type RefType<'a> = &'a VariantValue;
    type Viewer<'a> = ObjectViewer<'a, VariantValue>;
    type KeyType = VariantValue;

    #[inline]
    fn as_scalar_ref(&self) -> &VariantValue {
        self
    }

    #[allow(clippy::needless_lifetimes)]
    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: &'long VariantValue) -> &'short VariantValue {
        long
    }
}

impl<'a> ScalarRef<'a> for &'a VariantValue {
    type ColumnType = ObjectColumn<VariantValue>;
    type ScalarType = VariantValue;

    #[inline]
    fn to_owned_scalar(&self) -> VariantValue {
        (*self).clone()
    }

    #[inline]
    fn to_key(&self) -> VariantValue {
        (*self).clone()
    }

    fn has_alloc_beyond_bump() -> bool {
        true
    }
}

impl Scalar for ArrayValue {
    type ColumnType = ArrayColumn;
    type RefType<'a> = ArrayValueRef<'a>;
    type Viewer<'a> = ArrayViewer<'a>;
    type KeyType = ArrayValue;

    #[inline]
    fn as_scalar_ref(&self) -> ArrayValueRef<'_> {
        ArrayValueRef::ValueRef { val: self }
    }

    #[allow(clippy::needless_lifetimes)]
    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: ArrayValueRef<'long>) -> ArrayValueRef<'short> {
        long
    }
}

impl<'a> ScalarRef<'a> for ArrayValueRef<'a> {
    type ColumnType = ArrayColumn;
    type ScalarType = ArrayValue;

    #[inline]
    fn to_owned_scalar(&self) -> ArrayValue {
        match self {
            ArrayValueRef::Indexed { column, idx } => column.get(*idx).into(),
            ArrayValueRef::ValueRef { val } => (*val).clone(),
        }
    }

    #[inline]
    fn to_key(&self) -> ArrayValue {
        self.to_owned_scalar()
    }
}

impl Scalar for StructValue {
    type ColumnType = StructColumn;
    type RefType<'a> = StructValueRef<'a>;
    type Viewer<'a> = StructViewer<'a>;
    type KeyType = StructValue;

    #[inline]
    fn as_scalar_ref(&self) -> StructValueRef<'_> {
        StructValueRef::ValueRef { val: self }
    }

    #[allow(clippy::needless_lifetimes)]
    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: StructValueRef<'long>) -> StructValueRef<'short> {
        long
    }
}

impl<'a> ScalarRef<'a> for StructValueRef<'a> {
    type ColumnType = StructColumn;
    type ScalarType = StructValue;

    #[inline]
    fn to_owned_scalar(&self) -> StructValue {
        match self {
            StructValueRef::Indexed { column, idx } => column.get(*idx).into(),
            StructValueRef::ValueRef { val } => (*val).clone(),
        }
    }

    #[inline]
    fn to_key(&self) -> StructValue {
        self.to_owned_scalar()
    }
}
