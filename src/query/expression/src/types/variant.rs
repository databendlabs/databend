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

use core::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Range;

use databend_common_io::deserialize_bitmap;
use geozero::wkb::Ewkb;
use geozero::ToJson;
use jiff::tz::TimeZone;
use jsonb::OwnedJsonb;
use jsonb::RawJsonb;
use jsonb::Value;

use super::binary::BinaryColumn;
use super::binary::BinaryColumnBuilder;
use super::binary::BinaryColumnIter;
use super::date::date_to_string;
use super::number::NumberScalar;
use super::timestamp::timestamp_to_string;
use super::AccessType;
use crate::property::Domain;
use crate::types::map::KvPair;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::BuilderMut;
use crate::types::DataType;
use crate::types::DecimalScalar;
use crate::types::GenericMap;
use crate::types::ReturnType;
use crate::types::ValueType;
use crate::types::VectorScalarRef;
use crate::values::Column;
use crate::values::Scalar;
use crate::values::ScalarRef;
use crate::with_vector_number_type;
use crate::ColumnBuilder;
use crate::TableDataType;

/// JSONB bytes representation of `null`.
pub const JSONB_NULL: &[u8] = &[0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariantType;

impl AccessType for VariantType {
    type Scalar = Vec<u8>;
    type ScalarRef<'a> = &'a [u8];
    type Column = BinaryColumn;
    type Domain = ();
    type ColumnIterator<'a> = BinaryColumnIter<'a>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.to_vec()
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Option<Self::ScalarRef<'a>> {
        scalar.as_variant().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        col.as_variant().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        if domain.is_undefined() {
            Some(())
        } else {
            None
        }
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.index(index)
    }

    #[inline(always)]
    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        col.index_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter()
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        scalar.len()
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }

    #[inline(always)]
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        let left_jsonb = RawJsonb::new(lhs);
        let right_jsonb = RawJsonb::new(rhs);
        left_jsonb.cmp(&right_jsonb)
    }
}

impl ValueType for VariantType {
    type ColumnBuilder = BinaryColumnBuilder;
    type ColumnBuilderMut<'a> = BuilderMut<'a, Self>;

    fn upcast_scalar_with_type(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        debug_assert!(data_type.is_variant());
        Scalar::Variant(scalar)
    }

    fn upcast_domain_with_type(_domain: Self::Domain, data_type: &DataType) -> Domain {
        debug_assert!(data_type.is_variant());
        Domain::Undefined
    }

    fn upcast_column_with_type(col: Self::Column, data_type: &DataType) -> Column {
        debug_assert!(data_type.is_variant());
        Column::Variant(col)
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_> {
        builder.as_variant_mut().unwrap().into()
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        debug_assert!(data_type.is_variant());
        Some(ColumnBuilder::Variant(builder))
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        BinaryColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn builder_len_mut(builder: &Self::ColumnBuilderMut<'_>) -> usize {
        builder.len()
    }

    fn push_item_mut(builder: &mut Self::ColumnBuilderMut<'_>, item: Self::ScalarRef<'_>) {
        builder.put_slice(item);
        builder.commit_row();
    }

    fn push_item_repeat_mut(
        builder: &mut Self::ColumnBuilderMut<'_>,
        item: Self::ScalarRef<'_>,
        n: usize,
    ) {
        builder.push_repeat(item, n);
    }

    fn push_default_mut(builder: &mut Self::ColumnBuilderMut<'_>) {
        builder.commit_row();
    }

    fn append_column_mut(builder: &mut Self::ColumnBuilderMut<'_>, other: &Self::Column) {
        builder.append_column(other);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }
}

impl ArgType for VariantType {
    fn data_type() -> DataType {
        DataType::Variant
    }

    fn full_domain() -> Self::Domain {}
}

impl ReturnType for VariantType {
    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        BinaryColumnBuilder::with_capacity(capacity, 0)
    }
}

impl VariantType {
    pub fn create_column_from_variants(variants: &[Value]) -> BinaryColumn {
        let mut builder = BinaryColumnBuilder::with_capacity(variants.len(), 0);
        for v in variants {
            v.write_to_vec(&mut builder.data);
            builder.commit_row();
        }
        builder.build()
    }
}

pub fn cast_scalar_to_variant(
    scalar: ScalarRef,
    tz: &TimeZone,
    buf: &mut Vec<u8>,
    table_data_type: Option<&TableDataType>,
) {
    let opaque_buf;
    let value = match scalar {
        ScalarRef::Null => jsonb::Value::Null,
        ScalarRef::EmptyArray => jsonb::Value::Array(vec![]),
        ScalarRef::EmptyMap => jsonb::Value::Object(jsonb::Object::new()),
        ScalarRef::Number(n) => match n {
            NumberScalar::UInt8(n) => n.into(),
            NumberScalar::UInt16(n) => n.into(),
            NumberScalar::UInt32(n) => n.into(),
            NumberScalar::UInt64(n) => n.into(),
            NumberScalar::Int8(n) => n.into(),
            NumberScalar::Int16(n) => n.into(),
            NumberScalar::Int32(n) => n.into(),
            NumberScalar::Int64(n) => n.into(),
            NumberScalar::Float32(n) => n.0.into(),
            NumberScalar::Float64(n) => n.0.into(),
        },
        ScalarRef::Decimal(x) => match x {
            DecimalScalar::Decimal64(value, size) => {
                let dec = jsonb::Decimal64 {
                    scale: size.scale(),
                    value,
                };
                jsonb::Value::Number(jsonb::Number::Decimal64(dec))
            }
            DecimalScalar::Decimal128(value, size) => {
                let dec = jsonb::Decimal128 {
                    scale: size.scale(),
                    value,
                };
                jsonb::Value::Number(jsonb::Number::Decimal128(dec))
            }
            DecimalScalar::Decimal256(value, size) => {
                let dec = jsonb::Decimal256 {
                    scale: size.scale(),
                    value: value.0,
                };
                jsonb::Value::Number(jsonb::Number::Decimal256(dec))
            }
        },
        ScalarRef::Boolean(b) => jsonb::Value::Bool(b),
        ScalarRef::Binary(s) => jsonb::Value::Binary(s),
        ScalarRef::Opaque(o) => {
            opaque_buf = o.to_le_bytes();
            jsonb::Value::Binary(&opaque_buf)
        }
        ScalarRef::String(s) => jsonb::Value::String(s.into()),
        ScalarRef::Timestamp(ts) => jsonb::Value::Timestamp(jsonb::Timestamp { value: ts }),
        ScalarRef::Date(d) => jsonb::Value::Date(jsonb::Date { value: d }),
        ScalarRef::Interval(i) => {
            let interval = jsonb::Interval {
                months: i.months(),
                days: i.days(),
                micros: i.microseconds(),
            };
            jsonb::Value::Interval(interval)
        }
        ScalarRef::Array(col) => {
            let typ = if let Some(TableDataType::Array(typ)) = table_data_type {
                Some(typ.remove_nullable())
            } else {
                None
            };
            let items = cast_scalars_to_variants(col.iter(), tz, typ.as_ref());
            let owned_jsonb = OwnedJsonb::build_array(items.iter().map(RawJsonb::new))
                .expect("failed to build jsonb array");
            buf.extend_from_slice(owned_jsonb.as_ref());
            return;
        }
        ScalarRef::Map(col) => {
            let typ = if let Some(TableDataType::Map(typ)) = table_data_type {
                Some(typ.remove_nullable())
            } else {
                None
            };

            let kv_col = KvPair::<AnyType, AnyType>::try_downcast_column(&col).unwrap();
            let mut kvs = BTreeMap::new();
            for (k, v) in kv_col.iter() {
                let key = match k {
                    ScalarRef::String(v) => v.to_string(),
                    ScalarRef::Number(v) => v.to_string(),
                    ScalarRef::Decimal(v) => v.to_string(),
                    ScalarRef::Boolean(v) => v.to_string(),
                    ScalarRef::Timestamp(v) => timestamp_to_string(v, tz).to_string(),
                    ScalarRef::Date(v) => date_to_string(v, tz).to_string(),
                    _ => unreachable!(),
                };
                let mut val = vec![];
                cast_scalar_to_variant(v, tz, &mut val, typ.as_ref());
                kvs.insert(key, val);
            }
            let owned_jsonb =
                OwnedJsonb::build_object(kvs.iter().map(|(k, v)| (k, RawJsonb::new(&v[..]))))
                    .expect("failed to build jsonb object from map");
            buf.extend_from_slice(owned_jsonb.as_ref());
            return;
        }
        ScalarRef::Bitmap(b) => {
            jsonb::Value::Array(
                deserialize_bitmap(b)
                    .unwrap()
                    .iter()
                    .map(|x| x.into())
                    .collect(),
            )
            .write_to_vec(buf);
            return;
        }
        ScalarRef::Tuple(fields) => {
            let owned_jsonb = match table_data_type {
                Some(TableDataType::Tuple {
                    fields_name,
                    fields_type,
                }) => {
                    assert_eq!(fields.len(), fields_type.len());
                    let iter = fields.into_iter();
                    let mut builder = BinaryColumnBuilder::with_capacity(iter.size_hint().0, 0);
                    for (scalar, typ) in iter.zip(fields_type) {
                        cast_scalar_to_variant(
                            scalar,
                            tz,
                            &mut builder.data,
                            Some(&typ.remove_nullable()),
                        );
                        builder.commit_row();
                    }
                    let values = builder.build();
                    OwnedJsonb::build_object(
                        values
                            .iter()
                            .enumerate()
                            .map(|(i, bytes)| (fields_name[i].clone(), RawJsonb::new(bytes))),
                    )
                    .expect("failed to build jsonb object from tuple")
                }
                _ => {
                    let values = cast_scalars_to_variants(fields, tz, None);
                    OwnedJsonb::build_object(
                        values
                            .iter()
                            .enumerate()
                            .map(|(i, bytes)| (format!("{}", i + 1), RawJsonb::new(bytes))),
                    )
                    .expect("failed to build jsonb object from tuple")
                }
            };
            buf.extend_from_slice(owned_jsonb.as_ref());
            return;
        }
        ScalarRef::Variant(bytes) => {
            buf.extend_from_slice(bytes);
            return;
        }
        ScalarRef::Geometry(bytes) => {
            let geom = Ewkb(bytes).to_json().expect("failed to decode wkb data");
            jsonb::parse_owned_jsonb_with_buf(geom.as_bytes(), buf)
                .expect("failed to parse geojson to json value");
            return;
        }
        ScalarRef::Geography(bytes) => {
            // todo: Implement direct conversion, omitting intermediate processes
            let geom = Ewkb(bytes.0).to_json().expect("failed to decode wkb data");
            jsonb::parse_owned_jsonb_with_buf(geom.as_bytes(), buf)
                .expect("failed to parse geojson to json value");
            return;
        }
        ScalarRef::Vector(scalar) => with_vector_number_type!(|NUM_TYPE| match scalar {
            VectorScalarRef::NUM_TYPE(vals) => {
                let items = cast_scalars_to_variants(
                    vals.iter()
                        .map(|n| ScalarRef::Number(NumberScalar::NUM_TYPE(*n))),
                    tz,
                    None,
                );
                let owned_jsonb = OwnedJsonb::build_array(items.iter().map(RawJsonb::new))
                    .expect("failed to build jsonb array");
                buf.extend_from_slice(owned_jsonb.as_ref());
                return;
            }
        }),
    };
    value.write_to_vec(buf);
}

pub fn cast_scalars_to_variants(
    scalars: impl IntoIterator<Item = ScalarRef>,
    tz: &TimeZone,
    table_data_type: Option<&TableDataType>,
) -> BinaryColumn {
    let iter = scalars.into_iter();
    let mut builder = BinaryColumnBuilder::with_capacity(iter.size_hint().0, 0);
    for scalar in iter {
        cast_scalar_to_variant(scalar, tz, &mut builder.data, table_data_type);
        builder.commit_row();
    }
    builder.build()
}
