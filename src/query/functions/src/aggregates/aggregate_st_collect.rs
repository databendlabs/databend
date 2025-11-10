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

use std::alloc::Layout;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::GeometryType;
use databend_common_expression::types::ValueType;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_io::ewkb_to_geo;
use databend_common_io::geo_to_ewkb;
use geo::Geometry;
use geo::GeometryCollection;
use geo::LineString;
use geo::MultiLineString;
use geo::MultiPoint;
use geo::MultiPolygon;
use geo::Point;
use geo::Polygon;
use geozero::wkb::Ewkb;

use super::aggregate_scalar_state::ScalarStateFunc;
use super::assert_params;
use super::assert_unary_arguments;
use super::batch_merge1;
use super::batch_serialize1;
use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionSortDesc;
use super::StateAddr;
use super::StateSerde;

#[derive(Debug)]
pub struct StCollectState<T: ValueType> {
    values: Vec<T::Scalar>,
}

impl<T: ValueType> Default for StCollectState<T> {
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}

impl<T> ScalarStateFunc<T> for StCollectState<T>
where T: ArgType
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<T::ScalarRef<'_>>) {
        if let Some(other) = other {
            self.values.push(T::to_owned_scalar(other));
        }
    }

    fn add_batch(&mut self, column: &T::Column, validity: Option<&Bitmap>) -> Result<()> {
        let column_len = T::column_len(column);
        if column_len == 0 {
            return Ok(());
        }
        let column_iter = T::iter_column(column);
        if let Some(validity) = validity {
            for (val, valid) in column_iter.zip(validity.iter()) {
                if valid {
                    self.values.push(T::to_owned_scalar(val));
                }
            }
        } else {
            for val in column_iter {
                self.values.push(T::to_owned_scalar(val));
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.values.extend_from_slice(&rhs.values);
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        if self.values.is_empty() {
            builder.push(ScalarRef::Null);
            return Ok(());
        }

        let mut has_point = false;
        let mut has_line_string = false;
        let mut has_polygon = false;
        let mut has_other = false;

        let mut srid = None;
        let mut geos = Vec::with_capacity(self.values.len());
        let values = mem::take(&mut self.values);
        for (i, value) in values.into_iter().enumerate() {
            let val = T::upcast_scalar(value);
            let v = val.as_geometry().unwrap();
            let (geo, geo_srid) = ewkb_to_geo(&mut Ewkb(v))?;
            if i == 0 {
                srid = geo_srid;
            } else if !srid.eq(&geo_srid) {
                return Err(ErrorCode::GeometryError(format!(
                    "Incompatible SRID: {} and {}",
                    srid.unwrap_or_default(),
                    geo_srid.unwrap_or_default()
                )));
            }
            match geo {
                Geometry::Point(_) => {
                    has_point = true;
                }
                Geometry::LineString(_) => {
                    has_line_string = true;
                }
                Geometry::Polygon(_) => {
                    has_polygon = true;
                }
                _ => {
                    has_other = true;
                }
            }
            geos.push(geo);
        }
        let geo = if has_point && !has_line_string && !has_polygon && !has_other {
            let points: Vec<Point> = geos
                .into_iter()
                .map(|geo| geo.try_into().unwrap())
                .collect();
            let multi_point = MultiPoint::from_iter(points);
            Geometry::MultiPoint(multi_point)
        } else if !has_point && has_line_string && !has_polygon && !has_other {
            let line_strings: Vec<LineString> = geos
                .into_iter()
                .map(|geo| geo.try_into().unwrap())
                .collect();
            let multi_line_string = MultiLineString::from_iter(line_strings);
            Geometry::MultiLineString(multi_line_string)
        } else if !has_point && !has_line_string && has_polygon && !has_other {
            let polygons: Vec<Polygon> = geos
                .into_iter()
                .map(|geo| geo.try_into().unwrap())
                .collect();
            let multi_polygon = MultiPolygon::from_iter(polygons);
            Geometry::MultiPolygon(multi_polygon)
        } else {
            let geo_collect = GeometryCollection::from_iter(geos);
            Geometry::GeometryCollection(geo_collect)
        };

        let data = geo_to_ewkb(geo, srid)?;
        let geometry_value = Scalar::Geometry(data);
        builder.push(geometry_value.as_ref());
        Ok(())
    }
}

impl<T> StateSerde for StCollectState<T>
where T: ArgType
{
    fn serialize_type(_: Option<&dyn super::SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::Array(Box::new(T::data_type())).into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<ArrayType<T>, Self, _>(places, loc, builders, |state, builder| {
            for v in &state.values {
                builder.put_item(T::to_scalar_ref(v));
            }
            builder.commit_row();
            Ok(())
        })
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<ArrayType<T>, Self, _>(places, loc, state, filter, |state, values| {
            state
                .values
                .extend(T::iter_column(&values).map(T::to_owned_scalar));
            Ok(())
        })
    }
}

#[derive(Clone)]
struct AggregateStCollectFunction<T, State> {
    display_name: String,
    return_type: DataType,
    _t: PhantomData<fn(T, State)>,
}

impl<T, State> AggregateFunction for AggregateStCollectFunction<T, State>
where
    T: ValueType,
    State: ScalarStateFunc<T>,
{
    fn name(&self) -> &str {
        "AggregateStCollectFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: AggrState) {
        place.write(State::new);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<State>()));
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        _validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<State>();
        match &columns[0].to_column() {
            Column::Null { .. } => Ok(()),
            Column::Nullable(box nullable_column) => {
                let column = T::try_downcast_column(&nullable_column.column).unwrap();
                state.add_batch(&column, Some(&nullable_column.validity))
            }
            _ => {
                let column = T::try_downcast_column(&columns[0].to_column()).unwrap();
                state.add_batch(&column, None)
            }
        }
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        match &columns[0].to_column() {
            Column::Null { .. } => (),
            Column::Nullable(box nullable_column) => {
                let column = T::try_downcast_column(&nullable_column.column).unwrap();
                let column_iter = T::iter_column(&column);
                column_iter
                    .zip(nullable_column.validity.iter().zip(places.iter()))
                    .for_each(|(v, (valid, place))| {
                        let state = AggrState::new(*place, loc).get::<State>();
                        if valid {
                            state.add(Some(v.clone()))
                        } else {
                            state.add(None)
                        }
                    });
            }
            _ => {
                let column = T::try_downcast_column(&columns[0].to_column()).unwrap();
                let column_iter = T::iter_column(&column);
                column_iter.zip(places.iter()).for_each(|(v, place)| {
                    let state = AggrState::new(*place, loc).get::<State>();
                    state.add(Some(v.clone()))
                });
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        let state = place.get::<State>();
        match &columns[0].to_column() {
            Column::Null { .. } => (),
            Column::Nullable(box nullable_column) => {
                let valid = nullable_column.validity.get_bit(row);
                if valid {
                    let column = T::try_downcast_column(&nullable_column.column).unwrap();
                    let v = T::index_column(&column, row);
                    state.add(v);
                } else {
                    state.add(None);
                }
            }
            _ => {
                let column = T::try_downcast_column(&columns[0].to_column()).unwrap();
                let v = T::index_column(&column, row);
                state.add(v);
            }
        }

        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        State::serialize_type(None)
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        State::batch_serialize(places, loc, builders)
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        State::batch_merge(places, loc, state, filter)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<State>();
        let other = rhs.get::<State>();
        state.merge(other)
    }

    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let state = place.get::<State>();
        state.merge_result(builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<State>();
        std::ptr::drop_in_place(state);
    }
}

impl<T, State> fmt::Display for AggregateStCollectFunction<T, State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, State> AggregateStCollectFunction<T, State>
where
    T: ValueType,
    State: ScalarStateFunc<T>,
{
    fn try_create(display_name: &str, return_type: DataType) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateStCollectFunction::<T, State> {
            display_name: display_name.to_string(),
            return_type,
            _t: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

pub fn try_create_aggregate_st_collect_function(
    display_name: &str,
    params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_params(display_name, params.len(), 0)?;
    assert_unary_arguments(display_name, argument_types.len())?;
    if argument_types[0].remove_nullable() != DataType::Geometry
        && argument_types[0] != DataType::Null
    {
        return Err(ErrorCode::BadDataValueType(format!(
            "The argument of aggregate function {display_name} must be Geometry",
        )));
    }
    let return_type = DataType::Nullable(Box::new(DataType::Geometry));

    type State = StCollectState<GeometryType>;
    AggregateStCollectFunction::<GeometryType, State>::try_create(display_name, return_type)
}

pub fn aggregate_st_collect_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_st_collect_function))
}
