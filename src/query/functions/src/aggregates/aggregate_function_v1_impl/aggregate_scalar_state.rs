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

use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::ValueType;

use super::StateSerde;

// These types can downcast their builders successfully.
// TODO(@b41sh):  Variant => VariantType can't be used because it will use Scalar::String to compare
// Maybe we could use ValueType::compare() to compare them.
#[macro_export]
macro_rules! with_simple_no_number_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                String => StringType,
                Boolean => BooleanType,
                Timestamp => TimestampType,
                Null => NullType,
                EmptyArray => EmptyArrayType,
                EmptyMap => EmptyMapType,
                Date => DateType,
            ],
            $($tail)*
        }
    }
}

#[macro_export]
macro_rules! with_simple_no_number_no_string_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                Boolean => BooleanType,
                Timestamp => TimestampType,
                Null => NullType,
                EmptyArray => EmptyArrayType,
                EmptyMap => EmptyMapType,
                Date => DateType,
            ],
            $($tail)*
        }
    }
}

pub const TYPE_ANY: u8 = 0;
pub const TYPE_MIN: u8 = 1;
pub const TYPE_MAX: u8 = 2;

#[macro_export]
macro_rules! with_compare_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                TYPE_ANY => CmpAny,
                TYPE_MIN => CmpMin,
                TYPE_MAX => CmpMax,
            ],
            $($tail)*
        }
    }
}

pub trait ChangeIf<T: AccessType>: Default + Send + Sync + 'static {
    fn change_if(l: &T::ScalarRef<'_>, r: &T::ScalarRef<'_>) -> bool;
    fn change_if_ordering(ordering: Ordering) -> bool;
}

#[derive(Default)]
pub struct CmpMin;

impl<T> ChangeIf<T> for CmpMin
where
    T: ValueType,
    for<'a, 'b> T::ScalarRef<'a>: PartialOrd<T::ScalarRef<'b>>,
{
    #[inline]
    fn change_if<'a>(l: &T::ScalarRef<'_>, r: &T::ScalarRef<'_>) -> bool {
        matches!(l.partial_cmp(r), Some(Ordering::Greater))
    }

    #[inline]
    fn change_if_ordering(ordering: Ordering) -> bool {
        ordering == Ordering::Greater
    }
}

#[derive(Default)]
pub struct CmpMax;

impl<T> ChangeIf<T> for CmpMax
where
    T: ValueType,
    for<'a, 'b> T::ScalarRef<'a>: PartialOrd<T::ScalarRef<'b>>,
{
    #[inline]
    fn change_if<'a>(l: &T::ScalarRef<'_>, r: &T::ScalarRef<'_>) -> bool {
        matches!(l.partial_cmp(r), Some(Ordering::Less))
    }

    #[inline]
    fn change_if_ordering(ordering: Ordering) -> bool {
        ordering == Ordering::Less
    }
}

#[derive(Default)]
pub struct CmpAny;

impl<T: ValueType> ChangeIf<T> for CmpAny {
    #[inline]
    fn change_if(_: &T::ScalarRef<'_>, _: &T::ScalarRef<'_>) -> bool {
        false
    }

    #[inline]
    fn change_if_ordering(_: Ordering) -> bool {
        false
    }
}

pub(super) trait ScalarStateFunc<T: AccessType>: StateSerde + Send + 'static {
    fn new() -> Self;
    fn add(&mut self, other: Option<T::ScalarRef<'_>>);
    fn add_batch(&mut self, column: ColumnView<T>, validity: Option<&Bitmap>) -> Result<()>;
    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()>;
}
