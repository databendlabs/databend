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
use std::marker::PhantomData;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::geographic::CollectAggOp;
use databend_common_expression::geographic::EnvelopeAggOp;
use databend_common_expression::geographic::GeoAggOp;
use databend_common_expression::geographic::GeometryIntersectionAggOp;
use databend_common_expression::geographic::GeometryUnionAggOp;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::GeometryType;
use databend_common_io::ewkb_to_geo;
use databend_common_io::geo_to_ewkb;
use geo::Geometry;
use geozero::wkb::Ewkb;

use super::AggregateFunctionV2Factory;
use super::adaptors_v2 as v2;

struct GeographicBuilder;

trait GeographicAggregateMetadata {
    const NAMES: &'static [&'static str];
    const FEATURES: v2::FunctionFeatures;

    fn route() -> v2::DirectNameRoute;
}

impl GeographicBuilder {
    fn register(registry: &mut v2::AggregateFunctionRegistry) {
        CollectAggOp::route().register(registry);
        GeometryUnionAggOp::route().register(registry);
        GeometryIntersectionAggOp::route().register(registry);
        EnvelopeAggOp::route().register(registry);
    }
}

inventory::submit! {
    AggregateFunctionV2Factory {
        register: GeographicBuilder::register,
    }
}

impl GeographicAggregateMetadata for CollectAggOp {
    const NAMES: &'static [&'static str] = &["st_collect"];
    const FEATURES: v2::FunctionFeatures = GeographicBuilder::ST_COLLECT_FEATURES;

    fn route() -> v2::DirectNameRoute {
        let arguments = GeographicBuilder::geometry_arguments();
        let features = CollectAggOp::FEATURES;
        v2::DirectNameRoute::new(
            CollectAggOp::NAMES,
            arguments.clone(),
            features.clone(),
            v2::NullPolicy::Skip,
        )
        .then(v2::MergeRoute::new(
            false,
            GeographicBuilder::create_collect::<CollectAggOp>,
        ))
        .then(v2::MergeRoute::new(
            true,
            GeographicBuilder::create_collect::<CollectAggOp>,
        ))
        .then(v2::PlainRoute::new(
            GeographicBuilder::create_collect::<CollectAggOp>,
        ))
        .then(v2::IfRoute::new(
            GeographicBuilder::create_collect::<CollectAggOp>,
        ))
        .then(v2::StateRoute::new(
            GeographicBuilder::create_collect::<CollectAggOp>,
        ))
    }
}

impl GeographicAggregateMetadata for GeometryUnionAggOp {
    const NAMES: &'static [&'static str] = &["st_union_agg"];
    const FEATURES: v2::FunctionFeatures = GeographicBuilder::ST_UNION_AGG_FEATURES;

    fn route() -> v2::DirectNameRoute {
        let arguments = GeographicBuilder::geometry_arguments();
        let features = GeometryUnionAggOp::FEATURES;
        v2::DirectNameRoute::new(
            GeometryUnionAggOp::NAMES,
            arguments.clone(),
            features.clone(),
            v2::NullPolicy::Skip,
        )
        .then(v2::MergeRoute::new(
            false,
            GeographicBuilder::create_agg::<GeometryUnionAggOp>,
        ))
        .then(v2::MergeRoute::new(
            true,
            GeographicBuilder::create_agg::<GeometryUnionAggOp>,
        ))
        .then(v2::PlainRoute::new(
            GeographicBuilder::create_agg::<GeometryUnionAggOp>,
        ))
        .then(v2::IfRoute::new(
            GeographicBuilder::create_agg::<GeometryUnionAggOp>,
        ))
        .then(v2::StateRoute::new(
            GeographicBuilder::create_agg::<GeometryUnionAggOp>,
        ))
        .then(v2::DistinctAliasRoute::new(
            GeographicBuilder::create_agg::<GeometryUnionAggOp>,
        ))
    }
}

impl GeographicAggregateMetadata for GeometryIntersectionAggOp {
    const NAMES: &'static [&'static str] = &["st_intersection_agg"];
    const FEATURES: v2::FunctionFeatures = GeographicBuilder::ST_INTERSECTION_AGG_FEATURES;

    fn route() -> v2::DirectNameRoute {
        let arguments = GeographicBuilder::geometry_arguments();
        let features = GeometryIntersectionAggOp::FEATURES;
        v2::DirectNameRoute::new(
            GeometryIntersectionAggOp::NAMES,
            arguments.clone(),
            features.clone(),
            v2::NullPolicy::Skip,
        )
        .then(v2::MergeRoute::new(
            false,
            GeographicBuilder::create_agg::<GeometryIntersectionAggOp>,
        ))
        .then(v2::MergeRoute::new(
            true,
            GeographicBuilder::create_agg::<GeometryIntersectionAggOp>,
        ))
        .then(v2::PlainRoute::new(
            GeographicBuilder::create_agg::<GeometryIntersectionAggOp>,
        ))
        .then(v2::IfRoute::new(
            GeographicBuilder::create_agg::<GeometryIntersectionAggOp>,
        ))
        .then(v2::StateRoute::new(
            GeographicBuilder::create_agg::<GeometryIntersectionAggOp>,
        ))
        .then(v2::DistinctAliasRoute::new(
            GeographicBuilder::create_agg::<GeometryIntersectionAggOp>,
        ))
    }
}

impl GeographicAggregateMetadata for EnvelopeAggOp {
    const NAMES: &'static [&'static str] = &["st_envelope_agg"];
    const FEATURES: v2::FunctionFeatures = GeographicBuilder::ST_ENVELOPE_AGG_FEATURES;

    fn route() -> v2::DirectNameRoute {
        let arguments = GeographicBuilder::geometry_arguments();
        let features = EnvelopeAggOp::FEATURES;
        v2::DirectNameRoute::new(
            EnvelopeAggOp::NAMES,
            arguments.clone(),
            features.clone(),
            v2::NullPolicy::Skip,
        )
        .then(v2::MergeRoute::new(
            false,
            GeographicBuilder::create_agg::<EnvelopeAggOp>,
        ))
        .then(v2::MergeRoute::new(
            true,
            GeographicBuilder::create_agg::<EnvelopeAggOp>,
        ))
        .then(v2::PlainRoute::new(
            GeographicBuilder::create_agg::<EnvelopeAggOp>,
        ))
        .then(v2::IfRoute::new(
            GeographicBuilder::create_agg::<EnvelopeAggOp>,
        ))
        .then(v2::StateRoute::new(
            GeographicBuilder::create_agg::<EnvelopeAggOp>,
        ))
        .then(v2::DistinctAliasRoute::new(
            GeographicBuilder::create_agg::<EnvelopeAggOp>,
        ))
    }
}

impl GeographicBuilder {
    fn geometry_arguments() -> v2::AggregateArgumentsPattern {
        v2::AggregateArgumentsPattern::one_of(vec![
            v2::AggregateArgumentsPattern::fixed(vec![v2::AggregateArgumentPattern::exact(
                DataType::Geometry,
            )]),
            v2::AggregateArgumentsPattern::fixed(vec![v2::AggregateArgumentPattern::exact(
                DataType::Null,
            )]),
        ])
    }

    const ST_COLLECT_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: false,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "collects geometry values",
        definition: "st_collect(geometry)",
        example: "select st_collect(geom) from t",
    };

    const ST_UNION_AGG_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: false,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the union of geometry values",
        definition: "st_union_agg(geometry)",
        example: "select st_union_agg(geom) from t",
    };

    const ST_INTERSECTION_AGG_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: false,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the intersection of geometry values",
        definition: "st_intersection_agg(geometry)",
        example: "select st_intersection_agg(geom) from t",
    };

    const ST_ENVELOPE_AGG_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: false,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the envelope of geometry values",
        definition: "st_envelope_agg(geometry)",
        example: "select st_envelope_agg(geom) from t",
    };
}

pub struct AggregateGeometryAggState<O> {
    value: Option<Geometry<f64>>,
    srid: Option<i32>,
    _p: PhantomData<fn(O)>,
}

impl<O> Default for AggregateGeometryAggState<O> {
    fn default() -> Self {
        Self {
            value: None,
            srid: None,
            _p: PhantomData,
        }
    }
}

impl<O> AggregateGeometryAggState<O> {
    pub fn state_description() -> v2::AggregateStateDescription {
        v2::AggregateStateDescription::new(
            vec![AggrStateType::Custom(Layout::new::<Self>())],
            vec![StateSerdeItem::Binary(None)],
        )
        .with_manual_drop(true)
    }
}

impl<O> AggregateGeometryAggState<O>
where O: GeoAggOp
{
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
                self.value = O::compute(vec![acc.clone(), geo])?;
            }
        }
        Ok(())
    }

    fn add(&mut self, value: &[u8]) -> Result<()> {
        let (geo, srid) = ewkb_to_geo(&mut Ewkb(value))?;
        self.add_geo(geo, srid)
    }

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let binary_builder = builder.as_binary_mut().unwrap();
        if let Some(geo) = &self.value {
            let data = geo_to_ewkb(geo.clone(), self.srid)?;
            binary_builder.data.extend_from_slice(&data);
        }
        binary_builder.commit_row();
        Ok(())
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        let ScalarRef::Binary(data) = value else {
            unreachable!()
        };
        if data.is_empty() {
            return Ok(());
        }
        let (geo, srid) = ewkb_to_geo(&mut Ewkb(data))?;
        self.add_geo(geo, srid)
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        let Some(rhs_value) = rhs.value.take() else {
            return Ok(());
        };
        self.add_geo(rhs_value, rhs.srid)
    }

    fn push_result(
        builder: &mut ColumnBuilder,
        geo: Option<Geometry<f64>>,
        srid: Option<i32>,
    ) -> Result<()> {
        let Some(geo) = geo else {
            builder.push(ScalarRef::Null);
            return Ok(());
        };

        let data = geo_to_ewkb(geo, normalize_output_srid(srid))?;
        let geometry_value = Scalar::Geometry(data);
        builder.push(geometry_value.as_ref());
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        Self::push_result(builder, self.value.take(), self.srid)
    }

    fn merge_result_read_only(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        Self::push_result(builder, self.value.clone(), self.srid)
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
struct AggregateGeometryCollectState<O> {
    values: Vec<Vec<u8>>,
    _p: PhantomData<fn(O)>,
}

impl<O> Default for AggregateGeometryCollectState<O> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            _p: PhantomData,
        }
    }
}

impl<O> AggregateGeometryCollectState<O> {
    fn state_description() -> v2::AggregateStateDescription {
        v2::AggregateStateDescription::new(
            vec![AggrStateType::Custom(Layout::new::<Self>())],
            vec![StateSerdeItem::Binary(None)],
        )
        .with_manual_drop(true)
    }
}

impl<O> AggregateGeometryCollectState<O>
where O: GeoAggOp
{
    fn add(&mut self, value: &[u8]) {
        self.values.push(value.to_vec());
    }

    fn append(&mut self, rhs: &mut Self) {
        self.values.append(&mut rhs.values);
    }

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let binary_builder = builder.as_binary_mut().unwrap();
        BorshSerialize::serialize(self, &mut binary_builder.data)?;
        binary_builder.commit_row();
        Ok(())
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        let ScalarRef::Binary(mut data) = value else {
            unreachable!()
        };
        let mut rhs = Self::deserialize_reader(&mut data)?;
        self.append(&mut rhs);
        Ok(())
    }

    fn compute_result(&self) -> Result<Option<(Geometry<f64>, Option<i32>)>> {
        let mut srid = None;
        let mut geos = Vec::with_capacity(self.values.len());
        for value in &self.values {
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

        let geo = O::compute(geos)?;
        Ok(geo.map(|geo| (geo, normalize_output_srid(srid))))
    }

    fn push_result(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let result = self.compute_result()?;
        match result {
            Some((geo, srid)) => {
                let data = geo_to_ewkb(geo, srid)?;
                let geometry_value = Scalar::Geometry(data);
                builder.push(geometry_value.as_ref());
            }
            None => builder.push(ScalarRef::Null),
        }
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        self.push_result(builder)?;
        self.values.clear();
        Ok(())
    }

    fn merge_result_read_only(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        self.push_result(builder)
    }
}

struct AggregateGeometryAggImplementation<O> {
    _p: PhantomData<fn(O)>,
}

struct AggregateGeometryCollectImplementation<O> {
    _p: PhantomData<fn(O)>,
}

impl<O> Default for AggregateGeometryAggImplementation<O> {
    fn default() -> Self {
        Self { _p: PhantomData }
    }
}

impl<O> Default for AggregateGeometryCollectImplementation<O> {
    fn default() -> Self {
        Self { _p: PhantomData }
    }
}

fn strip_nullable_geometry_input(
    columns: ProjectedBlock<'_>,
    validity: Option<&Bitmap>,
) -> (BlockEntry, Option<Bitmap>) {
    let entry = &columns[0];
    let validity = merge_geometry_validity(entry, validity.cloned());
    (
        entry.clone().remove_nullable(),
        Bitmap::map_all_sets_to_none(validity),
    )
}

fn merge_geometry_validity(entry: &BlockEntry, validity: Option<Bitmap>) -> Option<Bitmap> {
    let entry_validity = match entry {
        BlockEntry::Const(scalar, _, rows) if scalar.is_null() => Some(Bitmap::new_zeroed(*rows)),
        BlockEntry::Column(Column::Null { len }) => Some(Bitmap::new_zeroed(*len)),
        BlockEntry::Column(Column::Nullable(column)) => {
            let validity = column.validity();
            (validity.null_count() != 0).then(|| validity.clone())
        }
        _ => None,
    };

    match (validity, entry_validity) {
        (Some(left), Some(right)) => Some(&left & &right),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

impl<O> v2::AggrImpl for AggregateGeometryAggImplementation<O>
where O: GeoAggOp
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateGeometryAggState::<O>::default);
    }

    fn accumulate(&self, input: v2::AccumulateInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateGeometryAggState<O>>();
        let (entry, validity) = strip_nullable_geometry_input(input.columns, input.validity);
        if entry.data_type().is_null() {
            return Ok(());
        }
        let values = entry.downcast::<GeometryType>()?;
        match validity.as_ref() {
            Some(validity) => {
                for (value, valid) in values.iter().zip(validity.iter()) {
                    if valid {
                        state.add(value)?;
                    }
                }
            }
            None => {
                for value in values.iter() {
                    state.add(value)?;
                }
            }
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: v2::AccumulateKeysInput<'_>) -> Result<()> {
        let (entry, validity) = strip_nullable_geometry_input(input.columns, None);
        if entry.data_type().is_null() {
            return Ok(());
        }
        let values = entry.downcast::<GeometryType>()?;
        for (row, state) in input.states.iter().enumerate() {
            if validity
                .as_ref()
                .is_some_and(|validity| !validity.get(row).unwrap())
            {
                continue;
            }
            state
                .get::<AggregateGeometryAggState<O>>()
                .add(values.index(row).unwrap())?;
        }
        Ok(())
    }

    fn accumulate_row(&self, input: v2::AccumulateRowInput<'_>) -> Result<()> {
        let (entry, validity) = strip_nullable_geometry_input(input.columns, None);
        if entry.data_type().is_null()
            || validity
                .as_ref()
                .is_some_and(|validity| !validity.get(input.row).unwrap())
        {
            return Ok(());
        }
        let values = entry.downcast::<GeometryType>()?;
        input
            .state
            .get::<AggregateGeometryAggState<O>>()
            .add(values.index(input.row).unwrap())?;
        Ok(())
    }

    fn serialize(&self, input: v2::SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            state
                .get::<AggregateGeometryAggState<O>>()
                .serialize(&mut input.builders[0])?;
        }
        Ok(())
    }

    fn merge_serialized(&self, input: v2::MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            state
                .get::<AggregateGeometryAggState<O>>()
                .merge_serialized(super::serialized_scalar_at(input.state, row, 0))?;
        }
        Ok(())
    }

    fn merge_states(&self, input: v2::MergeStatesInput<'_>) -> Result<()> {
        input
            .state
            .get::<AggregateGeometryAggState<O>>()
            .merge_owned(input.rhs.get::<AggregateGeometryAggState<O>>())?;
        Ok(())
    }

    fn merge_result(&self, input: v2::MergeResultInput<'_>) -> Result<()> {
        input
            .state
            .get::<AggregateGeometryAggState<O>>()
            .merge_result(input.builder)
    }

    fn merge_result_read_only(&self, input: v2::MergeResultInput<'_>) -> Result<()> {
        input
            .state
            .get::<AggregateGeometryAggState<O>>()
            .merge_result_read_only(input.builder)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(state.get::<AggregateGeometryAggState<O>>()) };
    }
}

impl<O> v2::AggrImpl for AggregateGeometryCollectImplementation<O>
where O: GeoAggOp
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateGeometryCollectState::<O>::default);
    }

    fn accumulate(&self, input: v2::AccumulateInput<'_>) -> Result<()> {
        let state = input.state.get::<AggregateGeometryCollectState<O>>();
        let (entry, validity) = strip_nullable_geometry_input(input.columns, input.validity);
        if entry.data_type().is_null() {
            return Ok(());
        }
        let values = entry.downcast::<GeometryType>()?;
        match validity.as_ref() {
            Some(validity) => {
                for (value, valid) in values.iter().zip(validity.iter()) {
                    if valid {
                        state.add(value);
                    }
                }
            }
            None => {
                for value in values.iter() {
                    state.add(value);
                }
            }
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: v2::AccumulateKeysInput<'_>) -> Result<()> {
        let (entry, validity) = strip_nullable_geometry_input(input.columns, None);
        if entry.data_type().is_null() {
            return Ok(());
        }
        let values = entry.downcast::<GeometryType>()?;
        for (row, state) in input.states.iter().enumerate() {
            if validity
                .as_ref()
                .is_some_and(|validity| !validity.get(row).unwrap())
            {
                continue;
            }
            state
                .get::<AggregateGeometryCollectState<O>>()
                .add(values.index(row).unwrap());
        }
        Ok(())
    }

    fn accumulate_row(&self, input: v2::AccumulateRowInput<'_>) -> Result<()> {
        let (entry, validity) = strip_nullable_geometry_input(input.columns, None);
        if entry.data_type().is_null()
            || validity
                .as_ref()
                .is_some_and(|validity| !validity.get(input.row).unwrap())
        {
            return Ok(());
        }
        let values = entry.downcast::<GeometryType>()?;
        input
            .state
            .get::<AggregateGeometryCollectState<O>>()
            .add(values.index(input.row).unwrap());
        Ok(())
    }

    fn serialize(&self, input: v2::SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            state
                .get::<AggregateGeometryCollectState<O>>()
                .serialize(&mut input.builders[0])?;
        }
        Ok(())
    }

    fn merge_serialized(&self, input: v2::MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            state
                .get::<AggregateGeometryCollectState<O>>()
                .merge_serialized(super::serialized_scalar_at(input.state, row, 0))?;
        }
        Ok(())
    }

    fn merge_states(&self, input: v2::MergeStatesInput<'_>) -> Result<()> {
        input
            .state
            .get::<AggregateGeometryCollectState<O>>()
            .append(input.rhs.get::<AggregateGeometryCollectState<O>>());
        Ok(())
    }

    fn merge_result(&self, input: v2::MergeResultInput<'_>) -> Result<()> {
        input
            .state
            .get::<AggregateGeometryCollectState<O>>()
            .merge_result(input.builder)
    }

    fn merge_result_read_only(&self, input: v2::MergeResultInput<'_>) -> Result<()> {
        input
            .state
            .get::<AggregateGeometryCollectState<O>>()
            .merge_result_read_only(input.builder)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(state.get::<AggregateGeometryCollectState<O>>()) };
    }
}

impl GeographicBuilder {
    fn create_agg<O>(
        build: v2::DirectBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<v2::AggregateFunctionRef>
    where O: GeoAggOp {
        Self::validate_request(&build)?;

        build.create(
            DataType::Geometry.wrap_nullable(),
            AggregateGeometryAggState::<O>::state_description(),
            AggregateGeometryAggImplementation::<O>::default(),
        )
    }

    fn create_collect<O>(
        build: v2::DirectBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<v2::AggregateFunctionRef>
    where O: GeoAggOp {
        Self::validate_request(&build)?;

        build.create(
            DataType::Geometry.wrap_nullable(),
            AggregateGeometryCollectState::<O>::state_description(),
            AggregateGeometryCollectImplementation::<O>::default(),
        )
    }

    fn validate_request(build: &v2::DirectBuildContext<'_, impl v2::CombinatorImpl>) -> Result<()> {
        if !build.params().is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects no parameters",
                build.name()
            )));
        }
        Ok(())
    }
}

fn normalize_output_srid(srid: Option<i32>) -> Option<i32> {
    match srid {
        Some(0) => None,
        other => other,
    }
}
