//
// Copyright 2024 RisingWave Labs
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

//! Convert arrow array from/to js objects.

use anyhow::{Context, Result};
use arrow_array::{array::*, builder::*, ArrowNativeTypeOp};
use arrow_buffer::{i256, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields};
use rquickjs::{
    function::Args, function::Constructor, Ctx, Error, FromJs, Function, IntoJs, Object,
    TypedArray, Value,
};
use std::{borrow::Cow, sync::Arc};

macro_rules! get_jsvalue {
    ($array_type: ty, $ctx:expr, $array:expr, $i:expr) => {{
        let array = $array.as_any().downcast_ref::<$array_type>().unwrap();
        array.value($i).into_js($ctx)
    }};
}

macro_rules! get_date_ms_js_value {
    ($array_type: ty, $ctx:expr, $array:expr, $i:expr) => {{
        let array = $array.as_any().downcast_ref::<$array_type>().unwrap();
        let date_constructor: Constructor = $ctx.globals().get("Date")?;
        let date_ms = array
            .value_as_datetime($i)
            .expect("failed to get date as datetime")
            .and_utc()
            .timestamp_millis();
        date_constructor.construct((date_ms,))?
    }};
}

macro_rules! build_timestamp_array {
    ($builder_type: ty, $date_primitive_type:ty, $ctx:expr, $values:expr, $op:tt, $coeff:expr) => {{
        let date_to_ms_epoch: Function = $ctx
            .eval("(function(x) { return x.getTime() })")
            .context("failed to get date to ms epoch function")?;

        let mut builder = <$builder_type>::with_capacity($values.len());

        for val in $values {
            if val.is_null() || val.is_undefined() {
                builder.append_null();
            } else {
                let date: i64 = date_to_ms_epoch.call((val,))?;
                let date = date $op $coeff;
                builder.append_value(date as $date_primitive_type);
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

macro_rules! get_typed_array {
    ($array_type: ty, $ctx:expr, $array:expr) => {{
        let array = $array.as_any().downcast_ref::<$array_type>().unwrap();
        if let Some(nulls) = array.nulls() {
            let mut values = Vec::with_capacity(array.len());
            for i in 0..array.len() {
                let value = if nulls.is_null(i) {
                    Value::new_null($ctx.clone())
                } else {
                    array.value(i).into_js($ctx)?
                };
                values.push(value);
            }
            values.into_js($ctx)
        } else {
            TypedArray::new($ctx.clone(), array.values().as_ref()).map(|a| a.into_value())
        }
    }};
}

macro_rules! build_array {
    (NullBuilder, $ctx:expr, $values:expr) => {{
        let mut builder = NullBuilder::new();
        for val in $values {
            if val.is_null() || val.is_undefined() {
                builder.append_null();
            } else {
                builder.append_empty_value();
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
    // primitive types
    ($builder_type: ty, $ctx:expr, $values:expr) => {{
        let mut builder = <$builder_type>::with_capacity($values.len());
        for val in $values {
            if val.is_null() || val.is_undefined() {
                builder.append_null();
            } else {
                builder.append_value(FromJs::from_js($ctx, val)?);
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
    // string and bytea
    ($builder_type: ty, $elem_type: ty, $ctx:expr, $values:expr) => {{
        let mut builder = <$builder_type>::with_capacity($values.len(), 1024);
        for val in $values {
            if val.is_null() || val.is_undefined() {
                builder.append_null();
            } else {
                builder.append_value(<$elem_type>::from_js($ctx, val)?);
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
    // view
    ($builder_type: ty, $elem_type: ty, $ctx:expr, $values:expr, $dummy: expr) => {{
        let mut builder = <$builder_type>::with_capacity($values.len());
        for val in $values {
            if val.is_null() || val.is_undefined() {
                builder.append_null();
            } else {
                builder.append_value(<$elem_type>::from_js($ctx, val)?);
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

macro_rules! build_json_array {
    ($array_type: ty, $ctx:expr, $values:expr) => {{
        let mut builder = <$array_type>::with_capacity($values.len(), 1024);
        for val in $values {
            if val.is_null() || val.is_undefined() {
                builder.append_null();
            } else if let Some(s) = $ctx.json_stringify(val)? {
                builder.append_value(s.to_string()?);
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
    ($array_type: ty, $ctx:expr, $values:expr, $view: expr) => {{
        let mut builder = <$array_type>::with_capacity($values.len());
        for val in $values {
            if val.is_null() || val.is_undefined() {
                builder.append_null();
            } else if let Some(s) = $ctx.json_stringify(val)? {
                builder.append_value(s.to_string()?);
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

#[derive(Debug, Clone)]
pub struct Converter {
    arrow_extension_key: Cow<'static, str>,
    json_extension_name: Cow<'static, str>,
    decimal_extension_name: Cow<'static, str>,
}

impl Converter {
    pub(crate) fn new() -> Self {
        Self {
            arrow_extension_key: "ARROW:extension:name".into(),
            json_extension_name: "arrowudf.json".into(),
            decimal_extension_name: "arrowudf.decimal".into(),
        }
    }

    /// Set the key for the arrow extension.
    ///
    /// The default value is `ARROW:extension:name`.
    pub fn set_arrow_extension_key(&mut self, key: &str) {
        self.arrow_extension_key = key.to_string().into();
    }

    /// Set the name for the json extension.
    ///
    /// The default value is `arrowudf.json`.
    pub fn set_json_extension_name(&mut self, name: &str) {
        self.json_extension_name = name.to_string().into();
    }

    /// Set the name for the decimal extension.
    ///
    /// The default value is `arrowudf.decimal`.
    pub fn set_decimal_extension_name(&mut self, name: &str) {
        self.decimal_extension_name = name.to_string().into();
    }

    /// Get array element as a JS Value.
    pub(super) fn get_jsvalue<'a>(
        &self,
        ctx: &Ctx<'a>,
        field: &Field,
        array: &dyn Array,
        i: usize,
    ) -> Result<Value<'a>, Error> {
        if array.is_null(i) {
            return Ok(Value::new_null(ctx.clone()));
        }

        match array.data_type() {
            DataType::Null => Ok(Value::new_null(ctx.clone())),
            DataType::Boolean => get_jsvalue!(BooleanArray, ctx, array, i),
            DataType::Int8 => get_jsvalue!(Int8Array, ctx, array, i),
            DataType::Int16 => get_jsvalue!(Int16Array, ctx, array, i),
            DataType::Int32 => get_jsvalue!(Int32Array, ctx, array, i),
            DataType::Int64 => get_jsvalue!(Int64Array, ctx, array, i),
            DataType::UInt8 => get_jsvalue!(UInt8Array, ctx, array, i),
            DataType::UInt16 => get_jsvalue!(UInt16Array, ctx, array, i),
            DataType::UInt32 => get_jsvalue!(UInt32Array, ctx, array, i),
            DataType::UInt64 => get_jsvalue!(UInt64Array, ctx, array, i),
            DataType::Float32 => get_jsvalue!(Float32Array, ctx, array, i),
            DataType::Float64 => get_jsvalue!(Float64Array, ctx, array, i),
            DataType::Utf8 => match field.metadata().get(self.arrow_extension_key.as_ref()) {
                Some(x) if x == self.json_extension_name.as_ref() => {
                    let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                    ctx.json_parse(array.value(i))
                }
                Some(x) if x == self.decimal_extension_name.as_ref() => {
                    let array = array.as_any().downcast_ref::<StringArray>().unwrap();

                    self.call_bigdecimal(ctx, array.value(i))
                }
                _ => get_jsvalue!(StringArray, ctx, array, i),
            },
            DataType::Binary => match field.metadata().get(self.arrow_extension_key.as_ref()) {
                Some(x) if x == self.json_extension_name.as_ref() => {
                    let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                    ctx.json_parse(array.value(i))
                }
                _ => get_jsvalue!(BinaryArray, ctx, array, i),
            },
            DataType::LargeUtf8 => get_jsvalue!(LargeStringArray, ctx, array, i),
            DataType::LargeBinary => {
                match field.metadata().get(self.arrow_extension_key.as_ref()) {
                    Some(x) if x == self.json_extension_name.as_ref() => {
                        let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                        ctx.json_parse(array.value(i))
                    }
                    _ => get_jsvalue!(LargeBinaryArray, ctx, array, i),
                }
            }
            DataType::Utf8View => get_jsvalue!(StringViewArray, ctx, array, i),
            DataType::BinaryView => match field.metadata().get(self.arrow_extension_key.as_ref()) {
                Some(x) if x == self.json_extension_name.as_ref() => {
                    let array = array.as_any().downcast_ref::<BinaryViewArray>().unwrap();
                    ctx.json_parse(array.value(i))
                }
                _ => get_jsvalue!(BinaryViewArray, ctx, array, i),
            },
            DataType::Decimal128(_, _) => {
                let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let decimal_str = array.value_as_string(i);

                self.call_bigdecimal(ctx, &decimal_str)
            }
            DataType::Decimal256(_, _) => {
                let array = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
                let decimal_str = array.value_as_string(i);

                self.call_bigdecimal(ctx, &decimal_str)
            }
            // TODO: handle tz correctly. requires probably converting tz str into a Chrono Tz
            DataType::Timestamp(unit, _tz) => {
                match unit {
                    // TODO: test this
                    arrow_schema::TimeUnit::Second => {
                        get_date_ms_js_value!(TimestampSecondArray, ctx, array, i)
                    }
                    arrow_schema::TimeUnit::Millisecond => {
                        get_date_ms_js_value!(TimestampMillisecondArray, ctx, array, i)
                    }
                    arrow_schema::TimeUnit::Microsecond => {
                        get_date_ms_js_value!(TimestampMicrosecondArray, ctx, array, i)
                    }
                    arrow_schema::TimeUnit::Nanosecond => {
                        get_date_ms_js_value!(TimestampNanosecondArray, ctx, array, i)
                    }
                }
            }
            DataType::Date32 => {
                get_date_ms_js_value!(Date32Array, ctx, array, i)
            }
            // list
            DataType::List(inner) => {
                let array = array.as_any().downcast_ref::<ListArray>().unwrap();
                let list = array.value(i);
                match inner.data_type() {
                    DataType::Int8 => get_typed_array!(Int8Array, ctx, list),
                    DataType::Int16 => get_typed_array!(Int16Array, ctx, list),
                    DataType::Int32 => get_typed_array!(Int32Array, ctx, list),
                    DataType::Int64 => get_typed_array!(Int64Array, ctx, list),
                    DataType::UInt8 => get_typed_array!(UInt8Array, ctx, list),
                    DataType::UInt16 => get_typed_array!(UInt16Array, ctx, list),
                    DataType::UInt32 => get_typed_array!(UInt32Array, ctx, list),
                    DataType::UInt64 => get_typed_array!(UInt64Array, ctx, list),
                    DataType::Float32 => get_typed_array!(Float32Array, ctx, list),
                    DataType::Float64 => get_typed_array!(Float64Array, ctx, list),
                    _ => {
                        let mut values = Vec::with_capacity(list.len());
                        for j in 0..list.len() {
                            values.push(self.get_jsvalue(ctx, field, list.as_ref(), j)?);
                        }
                        values.into_js(ctx)
                    }
                }
            }
            // large list
            DataType::LargeList(inner) => {
                let array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
                let list = array.value(i);
                match inner.data_type() {
                    DataType::Int8 => get_typed_array!(Int8Array, ctx, list),
                    DataType::Int16 => get_typed_array!(Int16Array, ctx, list),
                    DataType::Int32 => get_typed_array!(Int32Array, ctx, list),
                    DataType::Int64 => get_typed_array!(Int64Array, ctx, list),
                    DataType::UInt8 => get_typed_array!(UInt8Array, ctx, list),
                    DataType::UInt16 => get_typed_array!(UInt16Array, ctx, list),
                    DataType::UInt32 => get_typed_array!(UInt32Array, ctx, list),
                    DataType::UInt64 => get_typed_array!(UInt64Array, ctx, list),
                    DataType::Float32 => get_typed_array!(Float32Array, ctx, list),
                    DataType::Float64 => get_typed_array!(Float64Array, ctx, list),
                    _ => {
                        let mut values = Vec::with_capacity(list.len());
                        for j in 0..list.len() {
                            values.push(self.get_jsvalue(ctx, field, list.as_ref(), j)?);
                        }
                        values.into_js(ctx)
                    }
                }
            }
            DataType::Map(_, _) => {
                let array = array.as_any().downcast_ref::<MapArray>().unwrap();
                let list = array.value(i);
                if list.num_columns() != 2 {
                    return Err(Error::Unknown);
                }
                let fields = list.fields();
                let key_field = &fields[0];
                let value_field = &fields[1];

                let columns = list.columns();
                let keys = &columns[0];
                let values = &columns[1];

                let object = Object::new(ctx.clone())?;
                for j in 0..list.len() {
                    let key = self.get_jsvalue(ctx, key_field, keys, j)?;
                    let value = self.get_jsvalue(ctx, value_field, values, j)?;
                    object.set(key, value)?;
                }
                Ok(object.into_value())
            }
            DataType::Struct(fields) => {
                let array = array.as_any().downcast_ref::<StructArray>().unwrap();
                let object = Object::new(ctx.clone())?;
                for (j, field) in fields.iter().enumerate() {
                    let value = self.get_jsvalue(ctx, field, array.column(j).as_ref(), i)?;
                    object.set(field.name(), value)?;
                }
                Ok(object.into_value())
            }
            _other => Err(Error::Unknown),
        }
    }

    pub(super) fn build_array<'a>(
        &self,
        field: &Field,
        ctx: &Ctx<'a>,
        values: Vec<Value<'a>>,
    ) -> Result<ArrayRef> {
        match field.data_type() {
            DataType::Null => build_array!(NullBuilder, ctx, values),
            DataType::Boolean => build_array!(BooleanBuilder, ctx, values),
            DataType::Int8 => build_array!(Int8Builder, ctx, values),
            DataType::Int16 => build_array!(Int16Builder, ctx, values),
            DataType::Int32 => build_array!(Int32Builder, ctx, values),
            DataType::Int64 => build_array!(Int64Builder, ctx, values),
            DataType::UInt8 => build_array!(UInt8Builder, ctx, values),
            DataType::UInt16 => build_array!(UInt16Builder, ctx, values),
            DataType::UInt32 => build_array!(UInt32Builder, ctx, values),
            DataType::UInt64 => build_array!(UInt64Builder, ctx, values),
            DataType::Float32 => build_array!(Float32Builder, ctx, values),
            DataType::Float64 => build_array!(Float64Builder, ctx, values),
            DataType::Utf8 => match field.metadata().get(self.arrow_extension_key.as_ref()) {
                Some(x) if x == self.json_extension_name.as_ref() => {
                    build_json_array!(StringBuilder, ctx, values)
                }
                Some(x) if x == self.decimal_extension_name.as_ref() => {
                    let mut builder = StringBuilder::with_capacity(values.len(), 1024);
                    let bigdecimal_to_string: Function = ctx
                        .eval("BigDecimal.prototype.toString")
                        .context("failed to get BigDecimal.prototype.string")?;

                    for val in values {
                        if val.is_null() || val.is_undefined() {
                            builder.append_null();
                        } else {
                            let mut args = Args::new(ctx.clone(), 0);
                            args.this(val)?;

                            let string: String = bigdecimal_to_string.call_arg(args).context(
                                "failed to convert BigDecimal to string. make sure you return a BigDecimal value",
                            )?;

                            builder.append_value(string);
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                _ => build_array!(StringBuilder, String, ctx, values),
            },
            DataType::LargeUtf8 => build_array!(LargeStringBuilder, String, ctx, values),
            DataType::Utf8View => build_array!(StringViewBuilder, String, ctx, values, 1),
            DataType::Binary => match field.metadata().get(self.arrow_extension_key.as_ref()) {
                Some(x) if x == self.json_extension_name.as_ref() => {
                    build_json_array!(BinaryBuilder, ctx, values)
                }
                _ => build_array!(BinaryBuilder, Vec::<u8>, ctx, values),
            },
            DataType::LargeBinary => {
                match field.metadata().get(self.arrow_extension_key.as_ref()) {
                    Some(x) if x == self.json_extension_name.as_ref() => {
                        build_json_array!(LargeBinaryBuilder, ctx, values)
                    }
                    _ => build_array!(LargeBinaryBuilder, Vec::<u8>, ctx, values),
                }
            }
            DataType::BinaryView => match field.metadata().get(self.arrow_extension_key.as_ref()) {
                Some(x) if x == self.json_extension_name.as_ref() => {
                    build_json_array!(BinaryViewBuilder, ctx, values, 1)
                }
                _ => build_array!(BinaryViewBuilder, Vec::<u8>, ctx, values, 1),
            },
            DataType::Decimal128(precision, scale) => {
                let mut builder = Decimal128Builder::with_capacity(values.len())
                    .with_precision_and_scale(*precision, *scale)?;

                let bigdecimal_to_precision: Function =
                    self.get_bigdecimal_to_precision_function(ctx)?;

                for val in values {
                    if val.is_null() || val.is_undefined() {
                        builder.append_null();
                    } else {
                        let mut args = Args::new(ctx.clone(), 0);
                        args.this(val)?;
                        args.push_arg(*precision)?;
                        let string: String = bigdecimal_to_precision.call_arg(args).context(
                            "failed to convert BigDecimal to string. make sure you return a BigDecimal value",
                        )?;

                        let decimal_integer = self.decimal_string_to_i128(&string, *scale)?;
                        builder.append_value(decimal_integer);
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Decimal256(precision, scale) => {
                let mut builder = Decimal256Builder::with_capacity(values.len())
                    .with_precision_and_scale(*precision, *scale)?;

                let bigdecimal_to_precision = self.get_bigdecimal_to_precision_function(ctx)?;

                for val in values {
                    if val.is_null() || val.is_undefined() {
                        builder.append_null();
                    } else {
                        let mut args = Args::new(ctx.clone(), 0);
                        args.this(val)?;
                        args.push_arg(*precision)?;
                        let string: String = bigdecimal_to_precision.call_arg(args).context(
                            "failed to convert BigDecimal to string. make sure you return a BigDecimal value",
                        )?;
                        let decimal_integer = self.decimal_string_to_i256(&string, *scale)?;
                        builder.append_value(decimal_integer);
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Timestamp(unit, _tz) => {
                match unit {
                    // TODO denomenator is not quite right because if the fundamental unit is in
                    // milliseconds, then to convert nanoseconds to milliseconds, you need to divide by 1_000_000
                    arrow_schema::TimeUnit::Second => {
                        build_timestamp_array!(TimestampSecondBuilder, i64, ctx, values, /, 1000)
                    }
                    arrow_schema::TimeUnit::Millisecond => {
                        build_timestamp_array!(TimestampMillisecondBuilder, i64, ctx, values, /, 1)
                    }
                    arrow_schema::TimeUnit::Microsecond => {
                        build_timestamp_array!(TimestampMicrosecondBuilder, i64, ctx, values, *, 1000)
                    }
                    arrow_schema::TimeUnit::Nanosecond => {
                        build_timestamp_array!(TimestampNanosecondBuilder, i64, ctx, values, *, 1_000_000)
                    }
                }
            }
            DataType::Date32 => {
                build_timestamp_array!(Date32Builder, i32, ctx, values, /, 1000 * 60 * 60 * 24)
            }
            // list
            DataType::List(inner) => {
                // flatten lists
                let mut flatten_values = vec![];
                let mut offsets = Vec::<i32>::with_capacity(values.len() + 1);
                offsets.push(0);
                for val in &values {
                    if !val.is_null() && !val.is_undefined() {
                        let array = val.as_array().context("failed to convert to array")?;
                        flatten_values.reserve(array.len());
                        for elem in array.iter() {
                            flatten_values.push(elem?);
                        }
                    }
                    offsets.push(flatten_values.len() as i32);
                }
                let values_array = self.build_array(inner, ctx, flatten_values)?;
                let nulls = values
                    .iter()
                    .map(|v| !v.is_null() && !v.is_undefined())
                    .collect();
                Ok(Arc::new(ListArray::new(
                    inner.clone(),
                    OffsetBuffer::new(offsets.into()),
                    values_array,
                    Some(nulls),
                )))
            }
            DataType::LargeList(inner) => {
                // flatten lists
                let mut flatten_values = vec![];
                let mut offsets = Vec::<i64>::with_capacity(values.len() + 1);
                offsets.push(0);
                for val in &values {
                    if !val.is_null() && !val.is_undefined() {
                        let array = val.as_array().context("failed to convert to array")?;
                        flatten_values.reserve(array.len());
                        for elem in array.iter() {
                            flatten_values.push(elem?);
                        }
                    }
                    offsets.push(flatten_values.len() as i64);
                }
                let values_array = self.build_array(inner, ctx, flatten_values)?;
                let nulls = values
                    .iter()
                    .map(|v| !v.is_null() && !v.is_undefined())
                    .collect();
                Ok(Arc::new(LargeListArray::new(
                    inner.clone(),
                    OffsetBuffer::new(offsets.into()),
                    values_array,
                    Some(nulls),
                )))
            }
            DataType::Map(inner, _) => {
                let (key_field, value_field) = match inner.data_type() {
                    DataType::Struct(fields) => {
                        if fields.len() != 2 {
                            return Err(anyhow::anyhow!(
                                "Invalid map inner struct fields length {}",
                                fields.len()
                            ));
                        }
                        (fields[0].clone(), fields[1].clone())
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Invalid map inner datatype {}",
                            inner.data_type()
                        ));
                    }
                };
                let mut flatten_keys = vec![];
                let mut flatten_values = vec![];
                let mut offsets = Vec::<i32>::with_capacity(values.len() + 1);
                offsets.push(0);
                for val in &values {
                    if !val.is_null() && !val.is_undefined() {
                        let object = val.as_object().context("failed to convert to object")?;
                        for key in object.keys() {
                            flatten_keys.push(key?);
                        }
                        for value in object.values() {
                            flatten_values.push(value?);
                        }
                    }
                    offsets.push(flatten_keys.len() as i32);
                }
                let arrays = vec![
                    self.build_array(&key_field, ctx, flatten_keys)?,
                    self.build_array(&value_field, ctx, flatten_values)?,
                ];
                let struct_array =
                    StructArray::new(Fields::from([key_field, value_field]), arrays, None);

                let nulls = values
                    .iter()
                    .map(|v| !v.is_null() && !v.is_undefined())
                    .collect();
                Ok(Arc::new(MapArray::new(
                    inner.clone(),
                    OffsetBuffer::new(offsets.into()),
                    struct_array,
                    Some(nulls),
                    false,
                )))
            }
            DataType::Struct(fields) => {
                let mut arrays = Vec::with_capacity(fields.len());
                for field in fields {
                    let mut field_values = Vec::with_capacity(values.len());
                    for val in &values {
                        let v = if val.is_null() || val.is_undefined() {
                            Value::new_null(ctx.clone())
                        } else {
                            let object = val.as_object().context("expect object")?;
                            object.get(field.name())?
                        };
                        field_values.push(v);
                    }
                    arrays.push(self.build_array(field, ctx, field_values)?);
                }
                let nulls = values
                    .iter()
                    .map(|v| !v.is_null() && !v.is_undefined())
                    .collect();
                Ok(Arc::new(StructArray::new(
                    fields.clone(),
                    arrays,
                    Some(nulls),
                )))
            }
            other => Err(anyhow::anyhow!("Unimplemented datatype {}", other)),
        }
    }

    fn call_bigdecimal<'a>(
        &self,
        ctx: &Ctx<'a>,
        value: &str,
    ) -> rquickjs::Result<rquickjs::Value<'a>> {
        let bigdecimal: Function = ctx.globals().get("BigDecimal")?;
        bigdecimal.call((value,))
    }

    fn get_bigdecimal_to_precision_function<'a>(&self, ctx: &Ctx<'a>) -> Result<Function<'a>> {
        ctx.eval("BigDecimal.prototype.toPrecision")
            .context("failed to get BigDecimal.prototype.toPrecision")
    }

    fn decimal_string_to_i128(&self, s: &str, scale: i8) -> Result<i128> {
        if scale < 0 {
            return Err(anyhow::anyhow!(
                "currently only supports non-negative scale"
            ));
        }

        let (integer, fractional): (i128, i128) = match s.split_once('.') {
            Some((i, f)) => (
                i.parse().context("failed to parse integer part")?,
                (f[..scale as usize])
                    .to_string()
                    .parse()
                    .context("failed to parse fractional part")?,
            ),
            None => (s.parse().context("failed to parse integer part")?, 0),
        };

        Ok((integer * 10_i128.pow(scale as u32)) + fractional)
    }

    fn decimal_string_to_i256(&self, s: &str, scale: i8) -> Result<i256> {
        if scale < 0 {
            return Err(anyhow::anyhow!(
                "currently only supports non-negative scale"
            ));
        }

        // TODO: apply pattern from i128 here
        let (integer, fractional) = match s.split_once('.') {
            Some((i, f)) => (
                i256::from_string(i)
                    .ok_or_else(|| anyhow::anyhow!("failed to parse integer part"))?,
                i256::from_string(&f[..scale as usize])
                    .ok_or_else(|| anyhow::anyhow!("failed to parse fractional part"))?,
            ),
            None => (
                i256::from_string(s)
                    .ok_or_else(|| anyhow::anyhow!("failed to parse integer part"))?,
                i256::ZERO,
            ),
        };

        Ok((integer * i256::from_i128(10).pow_checked(scale as u32)?) + fractional)
    }
}
