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
use std::marker::PhantomData;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::ValueType;
use common_expression::ColumnBuilder;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

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

pub trait ChangeIf<T: ValueType>: Send + Sync + 'static {
    fn change_if(l: T::ScalarRef<'_>, r: T::ScalarRef<'_>) -> bool;
}

#[derive(Default)]
pub struct CmpMin;

impl<T> ChangeIf<T> for CmpMin
where
    T: ValueType,
    for<'a> T::ScalarRef<'a>: PartialOrd,
{
    #[inline]
    fn change_if<'a>(l: T::ScalarRef<'_>, r: T::ScalarRef<'_>) -> bool {
        let l = T::upcast_gat(l);
        let r = T::upcast_gat(r);
        l.partial_cmp(&r).unwrap_or(Ordering::Equal) == Ordering::Greater
    }
}

#[derive(Default)]
pub struct CmpMax;

impl<T> ChangeIf<T> for CmpMax
where
    T: ValueType,
    for<'a> T::ScalarRef<'a>: PartialOrd,
{
    #[inline]
    fn change_if<'a>(l: T::ScalarRef<'_>, r: T::ScalarRef<'_>) -> bool {
        let l = T::upcast_gat(l);
        let r = T::upcast_gat(r);
        l.partial_cmp(&r).unwrap_or(Ordering::Equal) == std::cmp::Ordering::Less
    }
}

#[derive(Default)]
pub struct CmpAny;

impl<T: ValueType> ChangeIf<T> for CmpAny {
    #[inline]
    fn change_if(_: T::ScalarRef<'_>, _: T::ScalarRef<'_>) -> bool {
        false
    }
}

pub trait ScalarStateFunc<T: ValueType>:
    Serialize + DeserializeOwned + Send + Sync + 'static
{
    fn new() -> Self;
    fn mem_size() -> Option<usize> {
        None
    }
    fn add(&mut self, other: Option<T::ScalarRef<'_>>);
    fn add_batch(&mut self, column: &T::Column, validity: Option<&Bitmap>) -> Result<()>;
    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()>;
}

#[derive(Serialize, Deserialize)]
pub struct ScalarState<T, C>
where
    T: ValueType,
    T::Scalar: Serialize + DeserializeOwned,
{
    #[serde(bound(deserialize = "T::Scalar: DeserializeOwned"))]
    pub value: Option<T::Scalar>,
    #[serde(skip)]
    _c: PhantomData<C>,
}

impl<T, C> Default for ScalarState<T, C>
where
    T: ValueType,
    T::Scalar: Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self {
            value: None,
            _c: PhantomData,
        }
    }
}

impl<T, C> ScalarStateFunc<T> for ScalarState<T, C>
where
    T: ValueType,
    T::Scalar: Serialize + DeserializeOwned + Send + Sync,
    C: ChangeIf<T> + Default,
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<T::ScalarRef<'_>>) {
        if let Some(other) = other {
            match &self.value {
                Some(v) => {
                    if C::change_if(T::to_scalar_ref(v), other.clone()) {
                        self.value = Some(T::to_owned_scalar(other));
                    }
                }
                None => {
                    self.value = Some(T::to_owned_scalar(other));
                }
            }
        }
    }

    fn add_batch(&mut self, column: &T::Column, validity: Option<&Bitmap>) -> Result<()> {
        let column_len = T::column_len(column);
        if column_len == 0 {
            return Ok(());
        }

        let column_iter = T::iter_column(column);

        if let Some(validity) = validity {
            if validity.unset_bits() == column_len {
                return Ok(());
            }

            // V::ScalarRef doesn't derive Default, so take the first value as default.
            let mut v = unsafe { T::index_column_unchecked(column, 0) };
            let mut has_v = validity.get_bit(0);

            for (data, valid) in column_iter.skip(1).zip(validity.iter().skip(1)) {
                if !valid {
                    continue;
                }
                if !has_v {
                    has_v = true;
                    v = data.clone();
                } else if C::change_if(v.clone(), data.clone()) {
                    v = data.clone();
                }
            }

            if has_v {
                self.add(Some(v));
            }
        } else {
            let v = column_iter.reduce(|l, r| {
                if !C::change_if(l.clone(), r.clone()) {
                    l
                } else {
                    r
                }
            });
            self.add(v);
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some(v) = &rhs.value {
            self.add(Some(T::to_scalar_ref(v)));
        }
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        if let Some(v) = &self.value {
            if let Some(inner) = T::try_downcast_builder(builder) {
                T::push_item(inner, T::to_scalar_ref(v));
            } else {
                builder.push(T::upcast_scalar(v.clone()).as_ref());
            }
        } else if let Some(inner) = T::try_downcast_builder(builder) {
            T::push_default(inner);
        } else {
            builder.push_default();
        }
        Ok(())
    }
}

pub fn need_manual_drop_state(data_type: &DataType) -> bool {
    match data_type {
        DataType::String | DataType::Variant => true,
        DataType::Nullable(t) | DataType::Array(t) | DataType::Map(t) => need_manual_drop_state(t),
        DataType::Tuple(ts) => ts.iter().any(need_manual_drop_state),
        _ => false,
    }
}
