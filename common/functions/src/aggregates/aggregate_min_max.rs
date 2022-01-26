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

use std::alloc::Layout;
use std::cmp::Ordering;
use std::fmt;
use std::marker::PhantomData;

use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues2::prelude::*;
use common_datavalues2::MutableColumn;
use common_datavalues2::Scalar;
use common_exception::Result;
use common_io::prelude::*;
use serde::Deserialize;
use serde::Serialize;

use super::StateAddr;
use crate::aggregates::AggregateFunction;

pub trait AggregateMinMaxState<S: Scalar> {
    fn default() -> Self;
    fn add(&mut self, value: S) -> Result<()>;
    fn add_batch(&mut self, column: &ColumnRef, validity: Option<&Bitmap>) -> Result<()>;

    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn serialize(&self, writer: &mut BytesMut) -> Result<()>;
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()>;
    fn merge_result(&mut self, array: &mut dyn MutableColumn) -> Result<()>;
}

pub trait ChangeIf<S: Scalar> {
    fn change_if(&self, l: S::RefType<'_>, r: S::RefType<'_>) -> bool;
}

struct Min {}

impl<S> ChangeIf<S> for Min
where
    S: Scalar,
    for<'a> S::RefType<'a>: PartialOrd,
{
    fn change_if<'a>(&self, l: S::RefType<'_>, r: S::RefType<'_>) -> bool {
        let l = S::upcast_gat(l);
        let r = S::upcast_gat(r);
        l.partial_cmp(&r).unwrap_or(Ordering::Equal) == Ordering::Greater
    }
}

struct Max {}

impl<S> ChangeIf<S> for Max
where
    S: Scalar,
    for<'a> S::RefType<'a>: PartialOrd,
{
    fn change_if<'a>(&self, l: S::RefType<'_>, r: S::RefType<'_>) -> bool {
        let l = S::upcast_gat(l);
        let r = S::upcast_gat(r);

        l.partial_cmp(&r).unwrap_or(Ordering::Equal) == Ordering::Less
    }
}

#[derive(Serialize, Deserialize)]
struct ScalarState<S: Scalar, C> {
    pub value: Option<S>,
    #[serde(skip)]
    c: C,
}

impl<S, C> AggregateMinMaxState<S> for ScalarState<S, C>
where
    S: Scalar + Ord,
    C: ChangeIf<S> + Default,
    for<'a> <S as Scalar>::RefType<'a>: Ord,
{
    fn default() -> Self {
        Self {
            value: None,
            c: C::default(),
        }
    }

    fn add(&mut self, other: S) -> Result<()> {
        match &self.value {
            Some(a) => {
                if self.c.change_if(a, &other) {
                    self.value = Some(other);
                }
            }
            _ => self.value = Some(other),
        }
        Ok(())
    }

    fn add_batch(&mut self, column: &ColumnRef, validity: Option<&Bitmap>) -> Result<()> {
        let col: &<S as Scalar>::ColumnType = unsafe { Series::static_cast(column) };
        if let Some(bit) = validity {
            if bit.null_count() == column.len() {
                return Ok(());
            }
            let mut v = S::default();
            let mut has_v = false;
            let view = ColumnViewer::<S>::create(column)?;

            for row in 0..view.len() {
                if view.null_at(row) {
                    continue;
                }
                let data = view.value(row).to_owned_scalar();
                if !has_v {
                    has_v = true;
                    v = data;
                } else {
                    if self.c.change_if(&v, &data) {
                        v = data;
                    }
                }
            }

            if has_v {
                self.add(v)?;
            }
        } else {
            let v = col.iter().reduce(|a, b| {
                let aa = a.to_owned_scalar();
                let bb = b.to_owned_scalar();
                if !self.c.change_if(&aa, &bb) {
                    a
                } else {
                    b
                }
            });

            if let Some(v) = v {
                self.add(v.to_owned_scalar())?;
            }
        };
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some(value) = &rhs.value {
            self.add(value)?;
        }
        Ok(())
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        todo!()
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        todo!()
    }

    fn merge_result(&mut self, array: &mut dyn MutableColumn) -> Result<()> {
        todo!()
    }
}

/// S: ScalarType
/// A: Aggregate State
#[derive(Clone)]
pub struct AggregateMinMaxFunction<S, C> {
    display_name: String,
    arguments: Vec<DataField>,
    _s: PhantomData<S>,
    _c: PhantomData<C>,
}

impl<S, C> AggregateFunction for AggregateMinMaxFunction<S, C>
where
    S: Scalar + PartialEq + Ord + Send + Sync,
    C: ChangeIf<S> + Default + Send + Sync,
    for<'a> <S as Scalar>::RefType<'a>: Ord,
{
    fn name(&self) -> &str {
        "AggregateMinMaxFunction"
    }

    fn return_type(&self) -> Result<DataTypePtr> {
        Ok(self.arguments[0].data_type().clone())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(ScalarState::<S, C>::default);
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<ScalarState<S, C>>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[common_datavalues2::ColumnRef],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<ScalarState<S, C>>();
        state.add_batch(&columns[0], validity)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[common_datavalues2::ColumnRef],
        _input_rows: usize,
    ) -> Result<()> {
        let col: &<S as Scalar>::ColumnType = unsafe { Series::static_cast(&columns[0]) };

        col.iter().zip(places.iter()).try_for_each(|(item, place)| {
            let addr = place.next(offset);
            let state = addr.get::<ScalarState<S, C>>();
            state.add(item.to_owned_scalar())
        })
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<ScalarState<S, C>>();
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<ScalarState<S, C>>();
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let rhs = rhs.get::<ScalarState<S, C>>();
        let state = place.get::<ScalarState<S, C>>();
        state.merge(rhs)
    }

    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableColumn) -> Result<()> {
        let state = place.get::<ScalarState<S, C>>();
        state.merge_result(array)?;
        Ok(())
    }
}

impl<S, C> fmt::Display for AggregateMinMaxFunction<S, C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

// pub fn try_create_aggregate_minmax_function<const IS_MIN: bool>(
//     display_name: &str,
//     _params: Vec<DataValue>,
//     arguments: Vec<DataField>,
// ) -> Result<Arc<dyn AggregateFunction>> {
//     assert_unary_arguments(display_name, arguments.len())?;
//     let data_type = arguments[0].data_type();

//     with_match_primitive_type!(data_type, |$T| {
//         type AggState = NumericState<$T>;
//         if IS_MIN {
//             AggregateMinMaxFunction::<AggState>::try_create_min(
//                 display_name,
//                 arguments,
//             )
//         } else {
//             AggregateMinMaxFunction::<AggState>::try_create_max(
//                 display_name,
//                 arguments,
//             )
//         }
//     },

//     {
//         if data_type == &DataType::String {
//             if IS_MIN {
//                 AggregateMinMaxFunction::<StringState>::try_create_min(display_name, arguments)
//             } else {
//                 AggregateMinMaxFunction::<StringState>::try_create_max(display_name, arguments)
//             }
//         } else {
//             Err(ErrorCode::BadDataValueType(format!(
//                 "AggregateMinMaxFunction does not support type '{:?}'",
//                 data_type
//             )))
//         }
//     })
// }

// pub fn aggregate_min_function_desc() -> AggregateFunctionDescription {
//     AggregateFunctionDescription::creator(Box::new(try_create_aggregate_minmax_function::<true>))
// }

// pub fn aggregate_max_function_desc() -> AggregateFunctionDescription {
//     AggregateFunctionDescription::creator(Box::new(try_create_aggregate_minmax_function::<false>))
// }
