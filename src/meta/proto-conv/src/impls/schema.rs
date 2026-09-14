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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use databend_common_expression as ex;
use databend_common_expression::TableDataType;
use databend_common_expression::VariantDataType;
use databend_common_expression::types::AggregateFunctionParam;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::OrderedFloat;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::decimal::i256;
use databend_common_protos::pb;
use databend_common_protos::pb::aggregate_decimal_param;
use databend_common_protos::pb::aggregate_function_param;
use databend_common_protos::pb::aggregate_number_param;
use databend_common_protos::pb::data_type::Dt;
use databend_common_protos::pb::data_type::Dt24;
use databend_common_protos::pb::number::Num;
use databend_common_protos::pb::variant_data_type;

use crate::FromProtoOptionExt;
use crate::FromToProto;
use crate::FromToProtoEnum;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;
use crate::reader_check_msg;

fn aggregate_param_from_pb(
    param: pb::AggregateFunctionParam,
) -> Result<AggregateFunctionParam, Incompatible> {
    reader_check_msg(param.ver, param.min_reader_ver)?;
    let value = param.value.ok_or_else(|| {
        Incompatible::new("AggregateFunctionParam.value can not be None".to_string())
    })?;

    match value {
        aggregate_function_param::Value::NullValue(_) => Ok(AggregateFunctionParam::Null),
        aggregate_function_param::Value::Number(number) => {
            reader_check_msg(number.ver, number.min_reader_ver)?;
            let value = number.value.ok_or_else(|| {
                Incompatible::new("AggregateNumberParam.value can not be None".to_string())
            })?;
            let number = match value {
                aggregate_number_param::Value::Uint8(value) => {
                    NumberScalar::UInt8(value.try_into().map_err(|_| {
                        Incompatible::new(format!(
                            "Aggregate uint8 parameter is out of range: {value}"
                        ))
                    })?)
                }
                aggregate_number_param::Value::Uint16(value) => {
                    NumberScalar::UInt16(value.try_into().map_err(|_| {
                        Incompatible::new(format!(
                            "Aggregate uint16 parameter is out of range: {value}"
                        ))
                    })?)
                }
                aggregate_number_param::Value::Uint32(value) => NumberScalar::UInt32(value),
                aggregate_number_param::Value::Uint64(value) => NumberScalar::UInt64(value),
                aggregate_number_param::Value::Int8(value) => {
                    NumberScalar::Int8(value.try_into().map_err(|_| {
                        Incompatible::new(format!(
                            "Aggregate int8 parameter is out of range: {value}"
                        ))
                    })?)
                }
                aggregate_number_param::Value::Int16(value) => {
                    NumberScalar::Int16(value.try_into().map_err(|_| {
                        Incompatible::new(format!(
                            "Aggregate int16 parameter is out of range: {value}"
                        ))
                    })?)
                }
                aggregate_number_param::Value::Int32(value) => NumberScalar::Int32(value),
                aggregate_number_param::Value::Int64(value) => NumberScalar::Int64(value),
                aggregate_number_param::Value::Float32Bits(value) => {
                    NumberScalar::Float32(OrderedFloat(f32::from_bits(value)))
                }
                aggregate_number_param::Value::Float64Bits(value) => {
                    NumberScalar::Float64(OrderedFloat(f64::from_bits(value)))
                }
            };
            Ok(AggregateFunctionParam::Number(number))
        }
        aggregate_function_param::Value::Decimal(decimal) => {
            reader_check_msg(decimal.ver, decimal.min_reader_ver)?;
            let value = decimal.value.ok_or_else(|| {
                Incompatible::new("AggregateDecimalParam.value can not be None".to_string())
            })?;
            let decimal = match value {
                aggregate_decimal_param::Value::Decimal64(value) => {
                    let (bytes, size) = aggregate_decimal_value_from_pb(value, 8, 18)?;
                    DecimalScalar::Decimal64(i64::from_be_bytes(bytes.try_into().unwrap()), size)
                }
                aggregate_decimal_param::Value::Decimal128(value) => {
                    let (bytes, size) = aggregate_decimal_value_from_pb(value, 16, 38)?;
                    DecimalScalar::Decimal128(i128::from_be_bytes(bytes.try_into().unwrap()), size)
                }
                aggregate_decimal_param::Value::Decimal256(value) => {
                    let (bytes, size) = aggregate_decimal_value_from_pb(value, 32, 76)?;
                    DecimalScalar::Decimal256(i256::from_be_bytes(bytes.try_into().unwrap()), size)
                }
            };
            Ok(AggregateFunctionParam::Decimal(decimal))
        }
        aggregate_function_param::Value::Timestamp(value) => {
            Ok(AggregateFunctionParam::Timestamp(value))
        }
        aggregate_function_param::Value::TimestampTz(value) => {
            reader_check_msg(value.ver, value.min_reader_ver)?;
            Ok(AggregateFunctionParam::timestamp_tz_from_parts(
                value.timestamp,
                value.offset,
            ))
        }
        aggregate_function_param::Value::Date(value) => Ok(AggregateFunctionParam::Date(value)),
        aggregate_function_param::Value::Interval(value) => {
            reader_check_msg(value.ver, value.min_reader_ver)?;
            Ok(AggregateFunctionParam::interval_from_parts(
                value.months,
                value.days,
                value.microseconds,
            ))
        }
        aggregate_function_param::Value::Boolean(value) => {
            Ok(AggregateFunctionParam::Boolean(value))
        }
        aggregate_function_param::Value::Binary(value) => Ok(AggregateFunctionParam::Binary(value)),
        aggregate_function_param::Value::String(value) => Ok(AggregateFunctionParam::String(value)),
        aggregate_function_param::Value::Bitmap(value) => Ok(AggregateFunctionParam::Bitmap(value)),
        aggregate_function_param::Value::Tuple(tuple) => {
            reader_check_msg(tuple.ver, tuple.min_reader_ver)?;
            let fields = tuple
                .fields
                .into_iter()
                .map(aggregate_param_from_pb)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AggregateFunctionParam::Tuple(fields))
        }
        aggregate_function_param::Value::Variant(value) => {
            Ok(AggregateFunctionParam::Variant(value))
        }
        aggregate_function_param::Value::Geometry(value) => {
            Ok(AggregateFunctionParam::Geometry(value))
        }
    }
}

fn aggregate_decimal_value_from_pb(
    value: pb::AggregateDecimalValue,
    expected_bytes: usize,
    max_precision: u8,
) -> Result<(Vec<u8>, DecimalSize), Incompatible> {
    reader_check_msg(value.ver, value.min_reader_ver)?;
    if value.value.len() != expected_bytes {
        return Err(Incompatible::new(format!(
            "Aggregate decimal parameter expects {expected_bytes} bytes, but got {}",
            value.value.len()
        )));
    }
    let precision = value.precision.try_into().map_err(|_| {
        Incompatible::new(format!(
            "Aggregate decimal precision is out of range: {}",
            value.precision
        ))
    })?;
    let scale = value.scale.try_into().map_err(|_| {
        Incompatible::new(format!(
            "Aggregate decimal scale is out of range: {}",
            value.scale
        ))
    })?;
    let size = DecimalSize::new(precision, scale)
        .map_err(|error| Incompatible::new(format!("Invalid aggregate decimal size: {error}")))?;
    if size.precision() > max_precision {
        return Err(Incompatible::new(format!(
            "Aggregate decimal parameter precision {} exceeds the maximum {max_precision}",
            size.precision()
        )));
    }
    Ok((value.value, size))
}

fn aggregate_param_to_pb(param: &AggregateFunctionParam) -> pb::AggregateFunctionParam {
    let value = match param {
        AggregateFunctionParam::Null => aggregate_function_param::Value::NullValue(pb::Empty {}),
        AggregateFunctionParam::Number(number) => {
            let value = match number {
                NumberScalar::UInt8(value) => aggregate_number_param::Value::Uint8(*value as u32),
                NumberScalar::UInt16(value) => aggregate_number_param::Value::Uint16(*value as u32),
                NumberScalar::UInt32(value) => aggregate_number_param::Value::Uint32(*value),
                NumberScalar::UInt64(value) => aggregate_number_param::Value::Uint64(*value),
                NumberScalar::Int8(value) => aggregate_number_param::Value::Int8(*value as i32),
                NumberScalar::Int16(value) => aggregate_number_param::Value::Int16(*value as i32),
                NumberScalar::Int32(value) => aggregate_number_param::Value::Int32(*value),
                NumberScalar::Int64(value) => aggregate_number_param::Value::Int64(*value),
                NumberScalar::Float32(value) => {
                    aggregate_number_param::Value::Float32Bits(value.0.to_bits())
                }
                NumberScalar::Float64(value) => {
                    aggregate_number_param::Value::Float64Bits(value.0.to_bits())
                }
            };
            aggregate_function_param::Value::Number(pb::AggregateNumberParam {
                ver: VER,
                min_reader_ver: MIN_READER_VER,
                value: Some(value),
            })
        }
        AggregateFunctionParam::Decimal(decimal) => {
            let value = match decimal {
                DecimalScalar::Decimal64(value, size) => aggregate_decimal_param::Value::Decimal64(
                    aggregate_decimal_value_to_pb(value.to_be_bytes().to_vec(), *size),
                ),
                DecimalScalar::Decimal128(value, size) => {
                    aggregate_decimal_param::Value::Decimal128(aggregate_decimal_value_to_pb(
                        value.to_be_bytes().to_vec(),
                        *size,
                    ))
                }
                DecimalScalar::Decimal256(value, size) => {
                    aggregate_decimal_param::Value::Decimal256(aggregate_decimal_value_to_pb(
                        value.to_be_bytes().to_vec(),
                        *size,
                    ))
                }
            };
            aggregate_function_param::Value::Decimal(pb::AggregateDecimalParam {
                ver: VER,
                min_reader_ver: MIN_READER_VER,
                value: Some(value),
            })
        }
        AggregateFunctionParam::Timestamp(value) => {
            aggregate_function_param::Value::Timestamp(*value)
        }
        AggregateFunctionParam::TimestampTz(value) => {
            aggregate_function_param::Value::TimestampTz(pb::AggregateTimestampTzParam {
                ver: VER,
                min_reader_ver: MIN_READER_VER,
                timestamp: value.timestamp(),
                offset: value.seconds_offset(),
            })
        }
        AggregateFunctionParam::Date(value) => aggregate_function_param::Value::Date(*value),
        AggregateFunctionParam::Interval(value) => {
            aggregate_function_param::Value::Interval(pb::AggregateIntervalParam {
                ver: VER,
                min_reader_ver: MIN_READER_VER,
                months: value.months(),
                days: value.days(),
                microseconds: value.microseconds(),
            })
        }
        AggregateFunctionParam::Boolean(value) => aggregate_function_param::Value::Boolean(*value),
        AggregateFunctionParam::Binary(value) => {
            aggregate_function_param::Value::Binary(value.clone())
        }
        AggregateFunctionParam::String(value) => {
            aggregate_function_param::Value::String(value.clone())
        }
        AggregateFunctionParam::Bitmap(value) => {
            aggregate_function_param::Value::Bitmap(value.clone())
        }
        AggregateFunctionParam::Tuple(fields) => {
            aggregate_function_param::Value::Tuple(pb::AggregateTupleParam {
                ver: VER,
                min_reader_ver: MIN_READER_VER,
                fields: fields.iter().map(aggregate_param_to_pb).collect(),
            })
        }
        AggregateFunctionParam::Variant(value) => {
            aggregate_function_param::Value::Variant(value.clone())
        }
        AggregateFunctionParam::Geometry(value) => {
            aggregate_function_param::Value::Geometry(value.clone())
        }
    };

    pb::AggregateFunctionParam {
        ver: VER,
        min_reader_ver: MIN_READER_VER,
        value: Some(value),
    }
}

fn aggregate_decimal_value_to_pb(value: Vec<u8>, size: DecimalSize) -> pb::AggregateDecimalValue {
    pb::AggregateDecimalValue {
        ver: VER,
        min_reader_ver: MIN_READER_VER,
        value,
        precision: size.precision() as u32,
        scale: size.scale() as u32,
    }
}

impl FromToProto for AggregateFunctionParam {
    type PB = pb::AggregateFunctionParam;

    fn get_pb_ver(param: &Self::PB) -> u64 {
        param.ver
    }

    fn from_pb(param: Self::PB) -> Result<Self, Incompatible> {
        aggregate_param_from_pb(param)
    }

    fn to_pb(&self) -> Self::PB {
        aggregate_param_to_pb(self)
    }
}

impl FromToProto for ex::TableSchema {
    type PB = pb::DataSchema;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DataSchema) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let mut fs = Vec::with_capacity(p.fields.len());
        for f in p.fields {
            fs.push(ex::TableField::from_pb(f)?);
        }

        let v = Self::new_from_column_ids(fs, p.metadata, p.next_column_id);
        Ok(v)
    }

    fn to_pb(&self) -> pb::DataSchema {
        let mut fs = Vec::with_capacity(self.fields().len());
        for f in self.fields() {
            fs.push(f.to_pb());
        }

        pb::DataSchema {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            fields: fs,
            metadata: self.meta().clone(),
            next_column_id: self.next_column_id(),
        }
    }
}

impl FromToProto for ex::TableField {
    type PB = pb::DataField;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DataField) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let computed_expr = p.computed_expr.from_pb_opt()?;
        let auto_increment_expr = p
            .auto_increment_expr
            .map(FromToProto::from_pb)
            .transpose()?;

        let v = ex::TableField::new_from_column_id(
            &p.name,
            ex::TableDataType::from_pb(p.data_type.ok_or_else(|| {
                Incompatible::new("DataField.data_type can not be None".to_string())
            })?)?,
            p.column_id,
        )
        .with_default_expr(p.default_expr)
        .with_computed_expr(computed_expr)
        .with_auto_increment_expr(auto_increment_expr);
        Ok(v)
    }

    fn to_pb(&self) -> pb::DataField {
        let computed_expr = self.computed_expr().map(FromToProto::to_pb);
        let auto_increment_expr = self.auto_increment_expr().map(FromToProto::to_pb);

        pb::DataField {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name().clone(),
            default_expr: self.default_expr().cloned(),
            data_type: Some(self.data_type().to_pb()),
            column_id: self.column_id(),
            computed_expr,
            auto_increment_expr,
        }
    }
}

impl FromToProto for ex::ComputedExpr {
    type PB = pb::ComputedExpr;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::ComputedExpr) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let computed_expr = p.computed_expr.ok_or_else(|| {
            Incompatible::new("Invalid ComputedExpr: .computed_expr can not be None".to_string())
        })?;

        let x = match computed_expr {
            pb::computed_expr::ComputedExpr::Virtual(expr) => Self::Virtual(expr),
            pb::computed_expr::ComputedExpr::Stored(expr) => Self::Stored(expr),
        };
        Ok(x)
    }

    fn to_pb(&self) -> pb::ComputedExpr {
        let x = match self {
            ex::ComputedExpr::Virtual(expr) => {
                pb::computed_expr::ComputedExpr::Virtual(expr.clone())
            }
            ex::ComputedExpr::Stored(expr) => pb::computed_expr::ComputedExpr::Stored(expr.clone()),
        };

        pb::ComputedExpr {
            ver: VER,
            min_reader_ver: MIN_READER_VER,

            computed_expr: Some(x),
        }
    }
}

impl FromToProto for ex::AutoIncrementExpr {
    type PB = pb::AutoIncrementExpr;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(ex::AutoIncrementExpr {
            column_id: p.column_id,
            start: p.start,
            step: p.step,
            is_ordered: p.is_ordered,
        })
    }

    fn to_pb(&self) -> Self::PB {
        pb::AutoIncrementExpr {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            start: self.start,
            step: self.step,
            column_id: self.column_id,
            is_ordered: self.is_ordered,
        }
    }
}

impl FromToProto for ex::TableDataType {
    type PB = pb::DataType;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DataType) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        match (&p.dt, &p.dt24) {
            (None, None) => Err(Incompatible::new(
                "DataType .dt and .dt24 are both None".to_string(),
            )),
            (Some(_), None) => {
                // Convert from version 23 or lower:
                let x = match p.dt.unwrap() {
                    Dt::NullType(_) => ex::TableDataType::Null,
                    Dt::NullableType(nullable_type) => {
                        //
                        reader_check_msg(nullable_type.ver, nullable_type.min_reader_ver)?;

                        let inner = Box::into_inner(nullable_type).inner;
                        let inner = inner.ok_or_else(|| {
                            Incompatible::new("NullableType.inner can not be None".to_string())
                        })?;
                        let inner = Box::into_inner(inner);
                        ex::TableDataType::Nullable(Box::new(ex::TableDataType::from_pb(inner)?))
                    }
                    Dt::BoolType(_) => ex::TableDataType::Boolean,
                    Dt::Int8Type(_) => ex::TableDataType::Number(NumberDataType::Int8),
                    Dt::Int16Type(_) => ex::TableDataType::Number(NumberDataType::Int16),
                    Dt::Int32Type(_) => ex::TableDataType::Number(NumberDataType::Int32),
                    Dt::Int64Type(_) => ex::TableDataType::Number(NumberDataType::Int64),
                    Dt::Uint8Type(_) => ex::TableDataType::Number(NumberDataType::UInt8),
                    Dt::Uint16Type(_) => ex::TableDataType::Number(NumberDataType::UInt16),
                    Dt::Uint32Type(_) => ex::TableDataType::Number(NumberDataType::UInt32),
                    Dt::Uint64Type(_) => ex::TableDataType::Number(NumberDataType::UInt64),
                    Dt::Float32Type(_) => ex::TableDataType::Number(NumberDataType::Float32),
                    Dt::Float64Type(_) => ex::TableDataType::Number(NumberDataType::Float64),
                    Dt::DateType(_) => ex::TableDataType::Date,
                    Dt::TimestampType(_) => ex::TableDataType::Timestamp,
                    Dt::StringType(_) => ex::TableDataType::String,
                    Dt::StructType(stt) => {
                        reader_check_msg(stt.ver, stt.min_reader_ver)?;

                        let mut types = vec![];
                        for x in stt.types {
                            let vv = ex::TableDataType::from_pb(x)?;
                            types.push(vv);
                        }

                        ex::TableDataType::Tuple {
                            fields_name: stt.names,
                            fields_type: types,
                        }
                    }
                    Dt::ArrayType(a) => {
                        reader_check_msg(a.ver, a.min_reader_ver)?;

                        let inner = Box::into_inner(a).inner;
                        let inner = inner.ok_or_else(|| {
                            Incompatible::new("Array.inner can not be None".to_string())
                        })?;
                        let inner = Box::into_inner(inner);
                        ex::TableDataType::Array(Box::new(ex::TableDataType::from_pb(inner)?))
                    }
                    Dt::VariantType(_) => ex::TableDataType::Variant,
                    Dt::VariantArrayType(_) => ex::TableDataType::Variant,
                    Dt::VariantObjectType(_) => ex::TableDataType::Variant,
                    // NOTE: No Interval type is ever stored in meta-service.
                    //       This variant should never be matched.
                    //       Thus it is safe for this conversion to map it to any type.
                    Dt::IntervalType(_) => ex::TableDataType::Null,
                };
                Ok(x)
            }
            (None, Some(_)) => {
                // Convert from version 24 or higher:
                let x = match p.dt24.unwrap() {
                    Dt24::NullT(_) => ex::TableDataType::Null,
                    Dt24::EmptyArrayT(_) => ex::TableDataType::EmptyArray,
                    Dt24::BoolT(_) => ex::TableDataType::Boolean,
                    Dt24::BinaryT(_) => ex::TableDataType::Binary,
                    Dt24::StringT(_) => ex::TableDataType::String,
                    Dt24::OpaqueT(size) => ex::TableDataType::Opaque(size as _),
                    Dt24::NumberT(n) => {
                        ex::TableDataType::Number(ex::types::NumberDataType::from_pb(n)?)
                    }
                    Dt24::TimestampT(_) => ex::TableDataType::Timestamp,
                    Dt24::DateT(_) => ex::TableDataType::Date,
                    Dt24::IntervalT(_) => ex::TableDataType::Interval,
                    Dt24::NullableT(x) => ex::TableDataType::Nullable(Box::new(
                        ex::TableDataType::from_pb(Box::into_inner(x))?,
                    )),
                    Dt24::ArrayT(x) => ex::TableDataType::Array(Box::new(
                        ex::TableDataType::from_pb(Box::into_inner(x))?,
                    )),
                    Dt24::MapT(x) => ex::TableDataType::Map(Box::new(ex::TableDataType::from_pb(
                        Box::into_inner(x),
                    )?)),
                    Dt24::BitmapT(_) => ex::TableDataType::Bitmap,
                    Dt24::TupleT(t) => {
                        reader_check_msg(t.ver, t.min_reader_ver)?;

                        let mut types = vec![];
                        for x in t.field_types {
                            let vv = ex::TableDataType::from_pb(x)?;
                            types.push(vv);
                        }

                        ex::TableDataType::Tuple {
                            fields_name: t.field_names,
                            fields_type: types,
                        }
                    }
                    Dt24::VariantT(_) => ex::TableDataType::Variant,
                    Dt24::GeometryT(_) => ex::TableDataType::Geometry,
                    Dt24::GeographyT(_) => ex::TableDataType::Geography,
                    Dt24::DecimalT(x) => {
                        ex::TableDataType::Decimal(ex::types::decimal::DecimalDataType::from_pb(x)?)
                    }
                    Dt24::EmptyMapT(_) => ex::TableDataType::EmptyMap,
                    Dt24::VectorT(v) => {
                        ex::TableDataType::Vector(ex::types::VectorDataType::from_pb(v)?)
                    }
                    Dt24::StageLocationT(_) => ex::TableDataType::StageLocation,
                    Dt24::TimestampTzT(_) => ex::TableDataType::TimestampTz,
                    Dt24::AggregateStateT(state) => {
                        reader_check_msg(state.ver, state.min_reader_ver)?;
                        let argument_types = state
                            .argument_types
                            .into_iter()
                            .map(ex::TableDataType::from_pb)
                            .collect::<Result<Vec<_>, _>>()?;
                        let params = state
                            .params
                            .into_iter()
                            .map(AggregateFunctionParam::from_pb)
                            .collect::<Result<Vec<_>, _>>()?;
                        let state_type = state.state_type.ok_or_else(|| {
                            Incompatible::new(
                                "AggregateState.state_type can not be None".to_string(),
                            )
                        })?;
                        ex::TableDataType::AggregateState {
                            function_name: state.function_name,
                            params,
                            argument_types,
                            state_type: Box::new(ex::TableDataType::from_pb(*state_type)?),
                        }
                    }
                };
                Ok(x)
            }
            (Some(_), Some(_)) => Err(Incompatible::new(
                "Invalid DataType: at most only one of .dt and .dt23 can be Some".to_string(),
            )),
        }
    }

    fn to_pb(&self) -> pb::DataType {
        match self {
            TableDataType::Null => new_pb_dt24(Dt24::NullT(pb::Empty {})),
            TableDataType::EmptyArray => new_pb_dt24(Dt24::EmptyArrayT(pb::Empty {})),
            TableDataType::EmptyMap => new_pb_dt24(Dt24::EmptyMapT(pb::Empty {})),
            TableDataType::Boolean => new_pb_dt24(Dt24::BoolT(pb::Empty {})),
            TableDataType::Binary => new_pb_dt24(Dt24::BinaryT(pb::Empty {})),
            TableDataType::String => new_pb_dt24(Dt24::StringT(pb::Empty {})),
            TableDataType::Opaque(size) => new_pb_dt24(Dt24::OpaqueT(*size as _)),
            TableDataType::Number(n) => {
                let x = n.to_pb();
                new_pb_dt24(Dt24::NumberT(x))
            }
            TableDataType::Decimal(n) => {
                let x = n.to_pb();
                new_pb_dt24(Dt24::DecimalT(x))
            }
            TableDataType::Timestamp => new_pb_dt24(Dt24::TimestampT(pb::Empty {})),
            TableDataType::Date => new_pb_dt24(Dt24::DateT(pb::Empty {})),
            TableDataType::Interval => new_pb_dt24(Dt24::IntervalT(pb::Empty {})),
            TableDataType::Nullable(v) => {
                let x = v.to_pb();
                new_pb_dt24(Dt24::NullableT(Box::new(x)))
            }
            TableDataType::Array(v) => {
                let x = v.to_pb();
                new_pb_dt24(Dt24::ArrayT(Box::new(x)))
            }
            TableDataType::Map(v) => {
                let x = v.to_pb();
                new_pb_dt24(Dt24::MapT(Box::new(x)))
            }
            TableDataType::Bitmap => new_pb_dt24(Dt24::BitmapT(pb::Empty {})),
            TableDataType::Tuple {
                fields_name,
                fields_type,
            } => {
                //
                let mut types = vec![];
                for t in fields_type {
                    let p = t.to_pb();
                    types.push(p);
                }

                let x = pb::Tuple {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    field_names: fields_name.clone(),
                    field_types: types,
                };
                new_pb_dt24(Dt24::TupleT(x))
            }
            TableDataType::Variant => new_pb_dt24(Dt24::VariantT(pb::Empty {})),
            TableDataType::Geometry => new_pb_dt24(Dt24::GeometryT(pb::Empty {})),
            TableDataType::Geography => new_pb_dt24(Dt24::GeographyT(pb::Empty {})),
            TableDataType::Vector(v) => {
                let x = v.to_pb();
                new_pb_dt24(Dt24::VectorT(x))
            }
            TableDataType::StageLocation => new_pb_dt24(Dt24::StageLocationT(pb::Empty {})),
            TableDataType::TimestampTz => new_pb_dt24(Dt24::TimestampTzT(pb::Empty {})),
            TableDataType::AggregateState {
                function_name,
                params,
                argument_types,
                state_type,
            } => new_pb_dt24(Dt24::AggregateStateT(Box::new(pb::AggregateState {
                ver: VER,
                min_reader_ver: MIN_READER_VER,
                function_name: function_name.clone(),
                params: params.iter().map(FromToProto::to_pb).collect(),
                argument_types: argument_types.iter().map(FromToProto::to_pb).collect(),
                state_type: Some(Box::new(state_type.to_pb())),
            }))),
        }
    }
}

impl FromToProto for ex::types::NumberDataType {
    type PB = pb::Number;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::Number) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let num = p
            .num
            .ok_or_else(|| Incompatible::new("Invalid Number: .num can not be None".to_string()))?;

        let x = match num {
            Num::Uint8Type(_) => Self::UInt8,
            Num::Uint16Type(_) => Self::UInt16,
            Num::Uint32Type(_) => Self::UInt32,
            Num::Uint64Type(_) => Self::UInt64,
            Num::Int8Type(_) => Self::Int8,
            Num::Int16Type(_) => Self::Int16,
            Num::Int32Type(_) => Self::Int32,
            Num::Int64Type(_) => Self::Int64,
            Num::Float32Type(_) => Self::Float32,
            Num::Float64Type(_) => Self::Float64,
        };
        Ok(x)
    }

    fn to_pb(&self) -> pb::Number {
        let x = match self {
            ex::types::NumberDataType::UInt8 => Num::Uint8Type(pb::Empty {}),
            ex::types::NumberDataType::UInt16 => Num::Uint16Type(pb::Empty {}),
            ex::types::NumberDataType::UInt32 => Num::Uint32Type(pb::Empty {}),
            ex::types::NumberDataType::UInt64 => Num::Uint64Type(pb::Empty {}),
            ex::types::NumberDataType::Int8 => Num::Int8Type(pb::Empty {}),
            ex::types::NumberDataType::Int16 => Num::Int16Type(pb::Empty {}),
            ex::types::NumberDataType::Int32 => Num::Int32Type(pb::Empty {}),
            ex::types::NumberDataType::Int64 => Num::Int64Type(pb::Empty {}),
            ex::types::NumberDataType::Float32 => Num::Float32Type(pb::Empty {}),
            ex::types::NumberDataType::Float64 => Num::Float64Type(pb::Empty {}),
        };
        pb::Number {
            ver: VER,
            min_reader_ver: MIN_READER_VER,

            num: Some(x),
        }
    }
}

impl FromToProto for ex::types::DecimalDataType {
    type PB = pb::Decimal;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::Decimal) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let num = p.decimal.ok_or_else(|| {
            Incompatible::new("Invalid Decimal: .decimal can not be None".to_string())
        })?;

        let x = match num {
            pb::decimal::Decimal::Decimal128(x) => {
                ex::types::DecimalDataType::Decimal128(ex::types::decimal::DecimalSize::from_pb(x)?)
            }
            pb::decimal::Decimal::Decimal256(x) => {
                ex::types::DecimalDataType::Decimal256(ex::types::decimal::DecimalSize::from_pb(x)?)
            }
        };
        Ok(x)
    }

    fn to_pb(&self) -> pb::Decimal {
        let x = match self {
            ex::types::DecimalDataType::Decimal64(size)
            | ex::types::DecimalDataType::Decimal128(size) => {
                pb::decimal::Decimal::Decimal128(ex::types::decimal::DecimalSize::to_pb(size))
            }
            ex::types::DecimalDataType::Decimal256(size) => {
                pb::decimal::Decimal::Decimal256(ex::types::decimal::DecimalSize::to_pb(size))
            }
        };
        pb::Decimal {
            ver: VER,
            min_reader_ver: MIN_READER_VER,

            decimal: Some(x),
        }
    }
}

impl FromToProto for ex::types::decimal::DecimalSize {
    type PB = pb::DecimalSize;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(ex::types::decimal::DecimalSize::new_unchecked(
            p.precision as _,
            p.scale as _,
        ))
    }

    fn to_pb(&self) -> Self::PB {
        pb::DecimalSize {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            precision: self.precision() as i32,
            scale: self.scale() as i32,
        }
    }
}

impl FromToProtoEnum for ex::VariantDataType {
    type PBEnum = pb::VariantDataType;

    fn from_pb_enum(p: pb::VariantDataType) -> Result<Self, Incompatible> {
        let Some(dt) = p.dt else {
            return Err(Incompatible::new(
                "Invalid VariantDataType: .dt must be Some".to_string(),
            ));
        };
        Ok(match dt {
            variant_data_type::Dt::JsonbT(_) => ex::VariantDataType::Jsonb,
            variant_data_type::Dt::BoolT(_) => ex::VariantDataType::Boolean,
            variant_data_type::Dt::Uint64T(_) => ex::VariantDataType::UInt64,
            variant_data_type::Dt::Int64T(_) => ex::VariantDataType::Int64,
            variant_data_type::Dt::Float64T(_) => ex::VariantDataType::Float64,
            variant_data_type::Dt::StringT(_) => ex::VariantDataType::String,
            variant_data_type::Dt::ArrayT(dt) => {
                ex::VariantDataType::Array(Box::new(ex::VariantDataType::from_pb_enum(*dt)?))
            }
            variant_data_type::Dt::DecimalT(dt) => {
                ex::VariantDataType::Decimal(ex::types::decimal::DecimalDataType::from_pb(dt)?)
            }
            variant_data_type::Dt::BinaryT(_) => ex::VariantDataType::Binary,
            variant_data_type::Dt::DateT(_) => ex::VariantDataType::Date,
            variant_data_type::Dt::TimestampT(_) => ex::VariantDataType::Timestamp,
            variant_data_type::Dt::IntervalT(_) => ex::VariantDataType::Interval,
        })
    }

    fn to_pb_enum(&self) -> pb::VariantDataType {
        let dt = match self {
            VariantDataType::Jsonb => pb::variant_data_type::Dt::JsonbT(pb::Empty {}),
            VariantDataType::Boolean => pb::variant_data_type::Dt::BoolT(pb::Empty {}),
            VariantDataType::UInt64 => pb::variant_data_type::Dt::Uint64T(pb::Empty {}),
            VariantDataType::Int64 => pb::variant_data_type::Dt::Int64T(pb::Empty {}),
            VariantDataType::Float64 => pb::variant_data_type::Dt::Float64T(pb::Empty {}),
            VariantDataType::String => pb::variant_data_type::Dt::StringT(pb::Empty {}),
            VariantDataType::Array(dt) => {
                pb::variant_data_type::Dt::ArrayT(Box::new(dt.to_pb_enum()))
            }
            VariantDataType::Decimal(n) => {
                let x = n.to_pb();
                pb::variant_data_type::Dt::DecimalT(x)
            }
            VariantDataType::Binary => pb::variant_data_type::Dt::BinaryT(pb::Empty {}),
            VariantDataType::Date => pb::variant_data_type::Dt::DateT(pb::Empty {}),
            VariantDataType::Timestamp => pb::variant_data_type::Dt::TimestampT(pb::Empty {}),
            VariantDataType::Interval => pb::variant_data_type::Dt::IntervalT(pb::Empty {}),
        };

        pb::VariantDataType { dt: Some(dt) }
    }
}

impl FromToProto for ex::VirtualDataSchema {
    type PB = pb::VirtualDataSchema;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let fields = p
            .fields
            .into_iter()
            .map(|field| {
                let data_types = field
                    .data_types
                    .into_iter()
                    .map(ex::VariantDataType::from_pb_enum)
                    .collect::<Result<Vec<ex::VariantDataType>, Incompatible>>()?;

                Ok(ex::VirtualDataField {
                    name: field.name,
                    data_types,
                    source_column_id: field.source_column_id,
                    column_id: field.column_id,
                })
            })
            .collect::<Result<Vec<_>, Incompatible>>()?;

        Ok(ex::VirtualDataSchema {
            fields,
            metadata: p.metadata,
            next_column_id: p.next_column_id,
            number_of_blocks: p.number_of_blocks,
        })
    }

    fn to_pb(&self) -> Self::PB {
        let fields = self
            .fields
            .iter()
            .map(|field| {
                let data_types = field
                    .data_types
                    .iter()
                    .map(ex::VariantDataType::to_pb_enum)
                    .collect();

                pb::VirtualDataField {
                    name: field.name.clone(),
                    data_types,
                    source_column_id: field.source_column_id,
                    column_id: field.column_id,
                }
            })
            .collect();

        pb::VirtualDataSchema {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            fields,
            metadata: self.metadata.clone(),
            next_column_id: self.next_column_id,
            number_of_blocks: self.number_of_blocks,
        }
    }
}

impl FromToProto for ex::types::VectorDataType {
    type PB = pb::Vector;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::Vector) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let num = p
            .num
            .ok_or_else(|| Incompatible::new("Invalid Vector: .num can not be None".to_string()))?;
        let num = ex::types::NumberDataType::from_pb(num)?;
        let x = match num {
            ex::types::NumberDataType::Int8 => ex::types::VectorDataType::Int8(p.dimension),
            ex::types::NumberDataType::Float32 => ex::types::VectorDataType::Float32(p.dimension),
            _ => unreachable!(),
        };
        Ok(x)
    }

    fn to_pb(&self) -> pb::Vector {
        let (number_ty, dimension) = match self {
            ex::types::VectorDataType::Int8(d) => (ex::types::NumberDataType::Int8, *d),
            ex::types::VectorDataType::Float32(d) => (ex::types::NumberDataType::Float32, *d),
        };
        let num = number_ty.to_pb();

        pb::Vector {
            ver: VER,
            min_reader_ver: MIN_READER_VER,

            num: Some(num),
            dimension,
        }
    }
}

/// Create a pb::DataType with version-24 data type schema
fn new_pb_dt24(dt24: Dt24) -> pb::DataType {
    pb::DataType {
        ver: VER,
        min_reader_ver: MIN_READER_VER,
        dt: None,
        dt24: Some(dt24),
    }
}
