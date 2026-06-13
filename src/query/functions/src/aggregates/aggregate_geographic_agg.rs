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
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::geographic::CollectAggOp;
use databend_common_expression::geographic::EnvelopeAggOp;
use databend_common_expression::geographic::GeoAggOp;
use databend_common_expression::geographic::GeometryIntersectionAggOp;
use databend_common_expression::geographic::GeometryUnionAggOp;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::GeometryType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::array::ArrayColumn;
use databend_common_expression::types::binary::BinaryColumn;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::binary::BinaryType;
use databend_common_expression::types::geography::Geography;
use databend_common_expression::types::geography::GeographyColumn;
use databend_common_io::ewkb_to_geo;
use databend_common_io::geo_to_ewkb;
use geo::Geometry;
use geozero::ToGeo;
use geozero::wkb::Ewkb;

use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionFeatures;
use super::AggregateFunctionSortDesc;
use super::StateAddr;
use super::StateSerde;
use super::aggregate_scalar_state::ScalarStateFunc;
use super::assert_params;
use super::assert_unary_arguments;
use super::batch_merge1;
use super::batch_serialize1;

fn geometry_column_to_geos_and_srid(
    column: &BinaryColumn,
) -> Result<(Vec<Geometry<f64>>, Option<i32>)> {
    let mut srid = None;
    let mut geos = Vec::with_capacity(column.len());
    for value in column.iter() {
        let (geo, geo_srid) = ewkb_to_geo(&mut Ewkb(value))?;
        let geo_srid = geo_srid.unwrap_or_default();
        if let Some(srid) = srid {
            if srid != geo_srid {
                return Err(ErrorCode::GeometryError(format!(
                    "Incompatible SRID: {} and {}",
                    srid, geo_srid,
                )));
            }
        } else {
            srid = Some(geo_srid);
        }
        geos.push(geo);
    }

    Ok((geos, srid))
}

fn geography_column_to_geos(column: &BinaryColumn) -> Result<Vec<Geometry<f64>>> {
    let mut geos = Vec::with_capacity(column.len());
    for value in column.iter() {
        let geo = Ewkb(value).to_geo()?;
        geos.push(geo);
    }

    Ok(geos)
}

fn normalize_output_srid(srid: Option<i32>) -> Option<i32> {
    match srid {
        Some(0) => None,
        other => other,
    }
}

#[derive(Debug)]
pub struct GeometryAggState<O>
where O: GeoAggOp
{
    value: Option<Geometry<f64>>,
    srid: Option<i32>,
    error: Option<String>,
    _phantom: PhantomData<O>,
}

impl<O> Default for GeometryAggState<O>
where O: GeoAggOp
{
    fn default() -> Self {
        Self {
            value: None,
            srid: None,
            error: None,
            _phantom: PhantomData,
        }
    }
}

impl<O> GeometryAggState<O>
where O: GeoAggOp
{
    fn set_error(&mut self, err: ErrorCode) {
        if self.error.is_none() {
            self.error = Some(err.message());
        }
    }

    fn add_geo(&mut self, geo: Geometry<f64>, geo_srid: Option<i32>) -> Result<()> {
        let geo_srid = geo_srid.unwrap_or_default();
        if let Some(srid) = self.srid {
            if srid != geo_srid {
                return Err(ErrorCode::GeometryError(format!(
                    "Incompatible SRID: {} and {}",
                    srid, geo_srid,
                )));
            }
        } else {
            self.srid = Some(geo_srid);
        }

        match &self.value {
            None => {
                self.value = Some(geo);
            }
            Some(acc) => {
                let merged = O::compute(vec![acc.clone(), geo])?;
                self.value = merged;
            }
        }

        Ok(())
    }
}

impl<O> ScalarStateFunc<GeometryType> for GeometryAggState<O>
where O: GeoAggOp
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<&[u8]>) {
        if self.error.is_some() {
            return;
        }
        let Some(other) = other else {
            return;
        };
        match ewkb_to_geo(&mut Ewkb(other)) {
            Ok((geo, srid)) => {
                if let Err(err) = self.add_geo(geo, srid) {
                    self.set_error(err);
                }
            }
            Err(err) => {
                self.set_error(err);
            }
        }
    }

    fn add_batch(
        &mut self,
        column: ColumnView<GeometryType>,
        validity: Option<&Bitmap>,
    ) -> Result<()> {
        if self.error.is_some() {
            return Ok(());
        }
        if let Some(validity) = validity {
            for (value, valid) in column.iter().zip(validity.iter()) {
                if !valid {
                    continue;
                }
                match ewkb_to_geo(&mut Ewkb(value)) {
                    Ok((geo, srid)) => {
                        if let Err(err) = self.add_geo(geo, srid) {
                            self.set_error(err);
                            break;
                        }
                    }
                    Err(err) => {
                        self.set_error(err);
                        break;
                    }
                }
            }
        } else {
            for value in column.iter() {
                match ewkb_to_geo(&mut Ewkb(value)) {
                    Ok((geo, srid)) => {
                        if let Err(err) = self.add_geo(geo, srid) {
                            self.set_error(err);
                            break;
                        }
                    }
                    Err(err) => {
                        self.set_error(err);
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some(err) = &self.error {
            return Err(ErrorCode::GeometryError(err.clone()));
        }
        if let Some(err) = &rhs.error {
            return Err(ErrorCode::GeometryError(err.clone()));
        }
        let Some(rhs_value) = &rhs.value else {
            return Ok(());
        };
        match (self.srid, rhs.srid) {
            (Some(lhs_srid), Some(rhs_srid)) => {
                if lhs_srid != rhs_srid {
                    return Err(ErrorCode::GeometryError(format!(
                        "Incompatible SRID: {} and {}",
                        lhs_srid, rhs_srid
                    )));
                }
            }
            (None, Some(rhs_srid)) => {
                self.srid = Some(rhs_srid);
            }
            (_, _) => {}
        }

        match &self.value {
            None => {
                self.value = Some(rhs_value.clone());
            }
            Some(acc) => {
                let merged = O::compute(vec![acc.clone(), rhs_value.clone()])?;
                self.value = merged;
            }
        }

        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        if let Some(err) = self.error.take() {
            return Err(ErrorCode::GeometryError(err));
        }
        let Some(geo) = self.value.take() else {
            builder.push(ScalarRef::Null);
            return Ok(());
        };

        let data = geo_to_ewkb(geo, normalize_output_srid(self.srid))?;
        let geometry_value = Scalar::Geometry(data);
        builder.push(geometry_value.as_ref());
        Ok(())
    }
}

impl<O> StateSerde for GeometryAggState<O>
where O: GeoAggOp
{
    fn serialize_type(_: Option<&dyn super::SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let binary_builder = builders[0].as_binary_mut().unwrap();
        for place in places {
            let state = AggrState::new(*place, loc).get::<Self>();
            if let Some(err) = &state.error {
                return Err(ErrorCode::GeometryError(err.clone()));
            }
            if let Some(geo) = &state.value {
                let data = geo_to_ewkb(geo.clone(), state.srid)?;
                binary_builder.data.extend_from_slice(&data);
            }
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<BinaryType, Self, _>(places, loc, state, filter, |state, value| {
            if value.is_empty() {
                return Ok(());
            }
            let (geo, geo_srid) = ewkb_to_geo(&mut Ewkb(value))?;
            state.add_geo(geo, geo_srid)?;
            Ok(())
        })
    }
}

#[derive(Debug)]
pub struct GeographicAggState<T, O>
where T: ArgType
{
    builder: BinaryColumnBuilder,
    _phantom: PhantomData<(T, O)>,
}

impl<T, O> Default for GeographicAggState<T, O>
where T: ArgType
{
    fn default() -> Self {
        Self {
            builder: BinaryColumnBuilder::with_capacity(0, 0),
            _phantom: PhantomData,
        }
    }
}

impl<T, O> ScalarStateFunc<T> for GeographicAggState<T, O>
where
    T: ArgType + std::marker::Send,
    O: GeoAggOp,
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<T::ScalarRef<'_>>) {
        if let Some(other) = other {
            let scalar = T::upcast_scalar(T::to_owned_scalar(other));
            let bytes = scalar
                .as_bytes()
                .expect("geometry/geography values must be bytes");
            self.builder.put_slice(bytes);
            self.builder.commit_row();
        }
    }

    fn add_batch(&mut self, column: ColumnView<T>, validity: Option<&Bitmap>) -> Result<()> {
        let column_len = column.len();
        if column_len == 0 {
            return Ok(());
        }
        if let Some(validity) = validity {
            for (val, valid) in column.iter().zip(validity.iter()) {
                if valid {
                    let scalar = T::upcast_scalar(T::to_owned_scalar(val));
                    let bytes = scalar
                        .as_bytes()
                        .expect("geometry/geography values must be bytes");
                    self.builder.put_slice(bytes);
                    self.builder.commit_row();
                }
            }
        } else {
            for val in column.iter() {
                let scalar = T::upcast_scalar(T::to_owned_scalar(val));
                let bytes = scalar
                    .as_bytes()
                    .expect("geometry/geography values must be bytes");
                self.builder.put_slice(bytes);
                self.builder.commit_row();
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.builder.append_column(&rhs.builder.clone().build());
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let mut drained = BinaryColumnBuilder::with_capacity(0, 0);
        std::mem::swap(&mut self.builder, &mut drained);
        let binary_column = drained.build();
        if binary_column.is_empty() {
            builder.push(ScalarRef::Null);
            return Ok(());
        }

        match T::data_type() {
            DataType::Geometry => {
                let (geos, srid) = geometry_column_to_geos_and_srid(&binary_column)?;
                let geo = O::compute(geos)?;
                match geo {
                    Some(geo) => {
                        let data = geo_to_ewkb(geo, normalize_output_srid(srid))?;
                        let geometry_value = Scalar::Geometry(data);
                        builder.push(geometry_value.as_ref());
                    }
                    None => builder.push(ScalarRef::Null),
                }
            }
            DataType::Geography => {
                let geos = geography_column_to_geos(&binary_column)?;
                let geo = O::compute(geos)?;
                match geo {
                    Some(geo) => {
                        let data = geo_to_ewkb(geo, None)?;
                        let geography_value = Scalar::Geography(Geography(data));
                        builder.push(geography_value.as_ref());
                    }
                    None => builder.push(ScalarRef::Null),
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}

impl<T, O> StateSerde for GeographicAggState<T, O>
where
    T: ArgType + std::marker::Send,
    O: GeoAggOp,
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
            let binary_column = state.builder.clone().build();
            let offsets = vec![0, binary_column.len() as u64];

            let column = match T::data_type() {
                DataType::Geometry => Column::Geometry(binary_column),
                DataType::Geography => Column::Geography(GeographyColumn(binary_column)),
                _ => unreachable!(),
            };
            let column = T::try_downcast_column(&column).unwrap();
            let array_column = ArrayColumn::new(column, offsets.into());
            builder.append_column(&array_column);

            Ok(())
        })
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<ArrayType<T>, Self, _>(places, loc, state, filter, |state, data| {
            let builder = T::column_to_builder(data);
            let column_builder = T::try_upcast_column_builder(builder, &T::data_type()).unwrap();
            let binary_builder = match column_builder {
                ColumnBuilder::Geometry(builder) | ColumnBuilder::Geography(builder) => builder,
                _ => unreachable!(),
            };

            let rhs = Self {
                builder: binary_builder,
                _phantom: PhantomData,
            };

            state.merge(&rhs)
        })
    }
}

#[derive(Clone)]
struct AggregateGeographicAggFunction<T, State> {
    display_name: String,
    return_type: DataType,
    _t: PhantomData<fn(T, State)>,
}

impl<T, State> AggregateFunction for AggregateGeographicAggFunction<T, State>
where
    T: ValueType,
    State: ScalarStateFunc<T>,
{
    fn name(&self) -> &str {
        "AggregateGeographicAggFunction"
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
        block: ProjectedBlock,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<State>();
        match &block[0] {
            BlockEntry::Const(_, DataType::Null, _) | BlockEntry::Column(Column::Null { .. }) => {
                Ok(())
            }
            entry => {
                let column = entry.downcast::<T>().unwrap();
                state.add_batch(column, validity)
            }
        }
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        block: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        let entry = &block[0];
        match entry.data_type() {
            DataType::Null => {}
            _ => entry
                .downcast::<T>()
                .unwrap()
                .iter()
                .zip(places.iter())
                .for_each(|(v, place)| {
                    let state = AggrState::new(*place, loc).get::<State>();
                    state.add(Some(v))
                }),
        }

        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, block: ProjectedBlock, row: usize) -> Result<()> {
        let state = place.get::<State>();
        let entry = &block[0];
        match entry.data_type() {
            DataType::Null => {}
            _ => {
                let view = entry.downcast::<T>().unwrap();
                let v = view.index(row).unwrap();
                state.add(Some(v));
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
        unsafe { std::ptr::drop_in_place(state) };
    }
}

impl<T, State> fmt::Display for AggregateGeographicAggFunction<T, State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, State> AggregateGeographicAggFunction<T, State>
where
    T: ValueType,
    State: ScalarStateFunc<T>,
{
    fn try_create(display_name: &str, return_type: DataType) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateGeographicAggFunction::<T, State> {
            display_name: display_name.to_string(),
            return_type,
            _t: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

fn try_create_aggregate_geographic_agg_function<GeomState>(
    display_name: &str,
    params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>>
where
    GeomState: ScalarStateFunc<GeometryType>,
{
    assert_params(display_name, params.len(), 0)?;
    assert_unary_arguments(display_name, argument_types.len())?;
    let arg_type = argument_types[0].remove_nullable();
    match arg_type {
        DataType::Geometry | DataType::Null => {
            let return_type = DataType::Nullable(Box::new(DataType::Geometry));
            AggregateGeographicAggFunction::<GeometryType, GeomState>::try_create(
                display_name,
                return_type,
            )
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "The argument of aggregate function {display_name} must be Geometry",
        ))),
    }
}

pub fn aggregate_st_union_agg_function_desc() -> AggregateFunctionDescription {
    type GeomState = GeometryAggState<GeometryUnionAggOp>;
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_geographic_agg_function::<GeomState>),
        AggregateFunctionFeatures {
            ..Default::default()
        },
    )
}

pub fn aggregate_st_intersection_agg_function_desc() -> AggregateFunctionDescription {
    type GeomState = GeometryAggState<GeometryIntersectionAggOp>;
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_geographic_agg_function::<GeomState>),
        AggregateFunctionFeatures {
            ..Default::default()
        },
    )
}

pub fn aggregate_st_envelope_agg_function_desc() -> AggregateFunctionDescription {
    type GeomState = GeometryAggState<EnvelopeAggOp>;
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_geographic_agg_function::<GeomState>),
        AggregateFunctionFeatures {
            ..Default::default()
        },
    )
}

pub fn aggregate_st_collect_function_desc() -> AggregateFunctionDescription {
    type GeomState = GeographicAggState<GeometryType, CollectAggOp>;
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_geographic_agg_function::<GeomState>),
        AggregateFunctionFeatures {
            ..Default::default()
        },
    )
}
