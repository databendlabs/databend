// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//\! Serialization and deserialization support for Iceberg values

pub(crate) mod _serde {
    use serde::de::Visitor;
    use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};
    use serde::{Deserialize, Serialize};
    use serde_bytes::ByteBuf;
    use serde_derive::{Deserialize as DeserializeDerive, Serialize as SerializeDerive};

    use crate::spec::values::{Literal, Map, PrimitiveLiteral, Struct};
    use crate::spec::{MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME, PrimitiveType, Type};
    use crate::{Error, ErrorKind};

    #[derive(SerializeDerive, DeserializeDerive, Debug)]
    #[serde(transparent)]
    /// Raw literal representation used for serde. The serialize way is used for Avro serializer.
    pub struct RawLiteral(RawLiteralEnum);

    impl RawLiteral {
        /// Covert literal to raw literal.
        pub fn try_from(literal: Literal, ty: &Type) -> Result<Self, Error> {
            Ok(Self(RawLiteralEnum::try_from(literal, ty)?))
        }

        /// Convert raw literal to literal.
        pub fn try_into(self, ty: &Type) -> Result<Option<Literal>, Error> {
            self.0.try_into(ty)
        }
    }

    #[derive(SerializeDerive, Clone, Debug)]
    #[serde(untagged)]
    enum RawLiteralEnum {
        Null,
        Boolean(bool),
        Int(i32),
        Long(i64),
        Float(f32),
        Double(f64),
        String(String),
        Bytes(ByteBuf),
        List(List),
        StringMap(StringMap),
        Record(Record),
    }

    #[derive(Clone, Debug)]
    struct Record {
        required: Vec<(String, RawLiteralEnum)>,
        optional: Vec<(String, Option<RawLiteralEnum>)>,
    }

    impl Serialize for Record {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer {
            let len = self.required.len() + self.optional.len();
            let mut record = serializer.serialize_struct("", len)?;
            for (k, v) in &self.required {
                record.serialize_field(Box::leak(k.clone().into_boxed_str()), &v)?;
            }
            for (k, v) in &self.optional {
                record.serialize_field(Box::leak(k.clone().into_boxed_str()), &v)?;
            }
            record.end()
        }
    }

    #[derive(Clone, Debug)]
    struct List {
        list: Vec<Option<RawLiteralEnum>>,
        required: bool,
    }

    impl Serialize for List {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer {
            let mut seq = serializer.serialize_seq(Some(self.list.len()))?;
            for value in &self.list {
                if self.required {
                    seq.serialize_element(value.as_ref().ok_or_else(|| {
                        serde::ser::Error::custom(
                            "List element is required, element cannot be null",
                        )
                    })?)?;
                } else {
                    seq.serialize_element(&value)?;
                }
            }
            seq.end()
        }
    }

    #[derive(Clone, Debug)]
    struct StringMap {
        raw: Vec<(String, Option<RawLiteralEnum>)>,
        required: bool,
    }

    impl Serialize for StringMap {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: serde::Serializer {
            let mut map = serializer.serialize_map(Some(self.raw.len()))?;
            for (k, v) in &self.raw {
                if self.required {
                    map.serialize_entry(
                        k,
                        v.as_ref().ok_or_else(|| {
                            serde::ser::Error::custom(
                                "Map element is required, element cannot be null",
                            )
                        })?,
                    )?;
                } else {
                    map.serialize_entry(k, v)?;
                }
            }
            map.end()
        }
    }

    impl<'de> Deserialize<'de> for RawLiteralEnum {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: serde::Deserializer<'de> {
            struct RawLiteralVisitor;
            impl<'de> Visitor<'de> for RawLiteralVisitor {
                type Value = RawLiteralEnum;

                fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    formatter.write_str("expect")
                }

                fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Boolean(v))
                }

                fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Int(v))
                }

                fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Long(v))
                }

                /// Used in json
                fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Long(v as i64))
                }

                fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Float(v))
                }

                fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Double(v))
                }

                fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::String(v.to_string()))
                }

                fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Bytes(ByteBuf::from(v)))
                }

                fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::String(v.to_string()))
                }

                fn visit_unit<E>(self) -> Result<Self::Value, E>
                where E: serde::de::Error {
                    Ok(RawLiteralEnum::Null)
                }

                fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where A: serde::de::MapAccess<'de> {
                    let mut required = Vec::new();
                    while let Some(key) = map.next_key::<String>()? {
                        let value = map.next_value::<RawLiteralEnum>()?;
                        required.push((key, value));
                    }
                    Ok(RawLiteralEnum::Record(Record {
                        required,
                        optional: Vec::new(),
                    }))
                }

                fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where A: serde::de::SeqAccess<'de> {
                    let mut list = Vec::new();
                    while let Some(value) = seq.next_element::<RawLiteralEnum>()? {
                        list.push(Some(value));
                    }
                    Ok(RawLiteralEnum::List(List {
                        list,
                        // `required` only used in serialize, just set default in deserialize.
                        required: false,
                    }))
                }
            }
            deserializer.deserialize_any(RawLiteralVisitor)
        }
    }

    impl RawLiteralEnum {
        pub fn try_from(literal: Literal, ty: &Type) -> Result<Self, Error> {
            let raw = match literal {
                Literal::Primitive(prim) => match prim {
                    PrimitiveLiteral::Boolean(v) => RawLiteralEnum::Boolean(v),
                    PrimitiveLiteral::Int(v) => RawLiteralEnum::Int(v),
                    PrimitiveLiteral::Long(v) => RawLiteralEnum::Long(v),
                    PrimitiveLiteral::Float(v) => RawLiteralEnum::Float(v.0),
                    PrimitiveLiteral::Double(v) => RawLiteralEnum::Double(v.0),
                    PrimitiveLiteral::String(v) => RawLiteralEnum::String(v),
                    PrimitiveLiteral::UInt128(v) => {
                        RawLiteralEnum::Bytes(ByteBuf::from(v.to_be_bytes()))
                    }
                    PrimitiveLiteral::Binary(v) => RawLiteralEnum::Bytes(ByteBuf::from(v)),
                    PrimitiveLiteral::Int128(v) => {
                        RawLiteralEnum::Bytes(ByteBuf::from(v.to_be_bytes()))
                    }
                    PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            "Can't convert AboveMax or BelowMax",
                        ));
                    }
                },
                Literal::Struct(r#struct) => {
                    let mut required = Vec::new();
                    let mut optional = Vec::new();
                    if let Type::Struct(struct_ty) = ty {
                        for (value, field) in r#struct.into_iter().zip(struct_ty.fields()) {
                            if field.required {
                                if let Some(value) = value {
                                    required.push((
                                        field.name.clone(),
                                        RawLiteralEnum::try_from(value, &field.field_type)?,
                                    ));
                                } else {
                                    return Err(Error::new(
                                        ErrorKind::DataInvalid,
                                        "Can't convert null to required field",
                                    ));
                                }
                            } else if let Some(value) = value {
                                optional.push((
                                    field.name.clone(),
                                    Some(RawLiteralEnum::try_from(value, &field.field_type)?),
                                ));
                            } else {
                                optional.push((field.name.clone(), None));
                            }
                        }
                    } else {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Type {ty} should be a struct"),
                        ));
                    }
                    RawLiteralEnum::Record(Record { required, optional })
                }
                Literal::List(list) => {
                    if let Type::List(list_ty) = ty {
                        let list = list
                            .into_iter()
                            .map(|v| {
                                v.map(|v| {
                                    RawLiteralEnum::try_from(v, &list_ty.element_field.field_type)
                                })
                                .transpose()
                            })
                            .collect::<Result<_, Error>>()?;
                        RawLiteralEnum::List(List {
                            list,
                            required: list_ty.element_field.required,
                        })
                    } else {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Type {ty} should be a list"),
                        ));
                    }
                }
                Literal::Map(map) => {
                    if let Type::Map(map_ty) = ty {
                        if let Type::Primitive(PrimitiveType::String) = *map_ty.key_field.field_type
                        {
                            let mut raw = Vec::with_capacity(map.len());
                            for (k, v) in map {
                                if let Literal::Primitive(PrimitiveLiteral::String(k)) = k {
                                    raw.push((
                                        k,
                                        v.map(|v| {
                                            RawLiteralEnum::try_from(
                                                v,
                                                &map_ty.value_field.field_type,
                                            )
                                        })
                                        .transpose()?,
                                    ));
                                } else {
                                    return Err(Error::new(
                                        ErrorKind::DataInvalid,
                                        "literal type is inconsistent with type",
                                    ));
                                }
                            }
                            RawLiteralEnum::StringMap(StringMap {
                                raw,
                                required: map_ty.value_field.required,
                            })
                        } else {
                            let list = map.into_iter().map(|(k,v)| {
                                let raw_k =
                                    RawLiteralEnum::try_from(k, &map_ty.key_field.field_type)?;
                                let raw_v = v
                                    .map(|v| {
                                        RawLiteralEnum::try_from(v, &map_ty.value_field.field_type)
                                    })
                                    .transpose()?;
                                if map_ty.value_field.required {
                                    Ok(Some(RawLiteralEnum::Record(Record {
                                        required: vec![
                                            (MAP_KEY_FIELD_NAME.to_string(), raw_k),
                                            (MAP_VALUE_FIELD_NAME.to_string(), raw_v.ok_or_else(||Error::new(ErrorKind::DataInvalid, "Map value is required, value cannot be null"))?),
                                        ],
                                        optional: vec![],
                                    })))
                                } else {
                                    Ok(Some(RawLiteralEnum::Record(Record {
                                        required: vec![
                                            (MAP_KEY_FIELD_NAME.to_string(), raw_k),
                                        ],
                                        optional: vec![
                                            (MAP_VALUE_FIELD_NAME.to_string(), raw_v)
                                        ],
                                    })))
                                }
                            }).collect::<Result<_, Error>>()?;
                            RawLiteralEnum::List(List {
                                list,
                                required: true,
                            })
                        }
                    } else {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Type {ty} should be a map"),
                        ));
                    }
                }
            };
            Ok(raw)
        }

        pub fn try_into(self, ty: &Type) -> Result<Option<Literal>, Error> {
            let invalid_err = |v: &str| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Unable to convert raw literal ({v}) fail convert to type {ty} for: type mismatch"
                    ),
                )
            };
            let invalid_err_with_reason = |v: &str, reason: &str| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Unable to convert raw literal ({v}) fail convert to type {ty} for: {reason}"
                    ),
                )
            };
            match self {
                RawLiteralEnum::Null => Ok(None),
                RawLiteralEnum::Boolean(v) => Ok(Some(Literal::bool(v))),
                RawLiteralEnum::Int(v) => match ty {
                    Type::Primitive(PrimitiveType::Int) => Ok(Some(Literal::int(v))),
                    Type::Primitive(PrimitiveType::Long) => Ok(Some(Literal::long(i64::from(v)))),
                    Type::Primitive(PrimitiveType::Date) => Ok(Some(Literal::date(v))),
                    _ => Err(invalid_err("int")),
                },
                RawLiteralEnum::Long(v) => match ty {
                    Type::Primitive(PrimitiveType::Int) => Ok(Some(Literal::int(
                        i32::try_from(v).map_err(|_| invalid_err("long"))?,
                    ))),
                    Type::Primitive(PrimitiveType::Date) => Ok(Some(Literal::date(
                        i32::try_from(v).map_err(|_| invalid_err("long"))?,
                    ))),
                    Type::Primitive(PrimitiveType::Long) => Ok(Some(Literal::long(v))),
                    Type::Primitive(PrimitiveType::Time) => Ok(Some(Literal::time(v))),
                    Type::Primitive(PrimitiveType::Timestamp) => Ok(Some(Literal::timestamp(v))),
                    Type::Primitive(PrimitiveType::Timestamptz) => {
                        Ok(Some(Literal::timestamptz(v)))
                    }
                    _ => Err(invalid_err("long")),
                },
                RawLiteralEnum::Float(v) => match ty {
                    Type::Primitive(PrimitiveType::Float) => Ok(Some(Literal::float(v))),
                    Type::Primitive(PrimitiveType::Double) => {
                        Ok(Some(Literal::double(f64::from(v))))
                    }
                    _ => Err(invalid_err("float")),
                },
                RawLiteralEnum::Double(v) => match ty {
                    Type::Primitive(PrimitiveType::Float) => {
                        let v_32 = v as f32;
                        if v_32.is_finite() {
                            let v_64 = f64::from(v_32);
                            if (v_64 - v).abs() > f32::EPSILON as f64 {
                                // there is a precision loss
                                return Err(invalid_err("double"));
                            }
                        }
                        Ok(Some(Literal::float(v_32)))
                    }
                    Type::Primitive(PrimitiveType::Double) => Ok(Some(Literal::double(v))),
                    _ => Err(invalid_err("double")),
                },
                RawLiteralEnum::String(v) => match ty {
                    Type::Primitive(PrimitiveType::String) => Ok(Some(Literal::string(v))),
                    _ => Err(invalid_err("string")),
                },
                RawLiteralEnum::Bytes(v) => match ty {
                    Type::Primitive(PrimitiveType::Binary) => Ok(Some(Literal::binary(v.to_vec()))),
                    Type::Primitive(PrimitiveType::Fixed(expected_len)) => {
                        if v.len() == *expected_len as usize {
                            Ok(Some(Literal::fixed(v.to_vec())))
                        } else {
                            Err(invalid_err_with_reason(
                                "bytes",
                                &format!(
                                    "Fixed type must be exactly {} bytes, got {}",
                                    expected_len,
                                    v.len()
                                ),
                            ))
                        }
                    }
                    Type::Primitive(PrimitiveType::Uuid) => {
                        if v.len() == 16 {
                            let bytes: [u8; 16] = v.as_slice().try_into().map_err(|_| {
                                invalid_err_with_reason("bytes", "UUID must be exactly 16 bytes")
                            })?;
                            Ok(Some(Literal::uuid(uuid::Uuid::from_bytes(bytes))))
                        } else {
                            Err(invalid_err_with_reason(
                                "bytes",
                                "UUID must be exactly 16 bytes",
                            ))
                        }
                    }
                    Type::Primitive(PrimitiveType::Decimal { precision, .. }) => {
                        let required_bytes = Type::decimal_required_bytes(*precision)? as usize;

                        if v.len() == required_bytes {
                            // Pad the bytes to 16 bytes (i128 size) with sign extension
                            let mut padded_bytes = [0u8; 16];
                            let start_idx = 16 - v.len();

                            // Copy the input bytes to the end of the array
                            padded_bytes[start_idx..].copy_from_slice(&v);

                            // Sign extend if the number is negative (MSB is 1)
                            if !v.is_empty() && (v[0] & 0x80) != 0 {
                                // Fill the padding with 0xFF for negative numbers
                                for byte in &mut padded_bytes[..start_idx] {
                                    *byte = 0xFF;
                                }
                            }

                            Ok(Some(Literal::Primitive(PrimitiveLiteral::Int128(
                                i128::from_be_bytes(padded_bytes),
                            ))))
                        } else {
                            Err(invalid_err_with_reason(
                                "bytes",
                                &format!(
                                    "Decimal with precision {} must be exactly {} bytes, got {}",
                                    precision,
                                    required_bytes,
                                    v.len()
                                ),
                            ))
                        }
                    }
                    _ => Err(invalid_err("bytes")),
                },
                RawLiteralEnum::List(v) => match ty {
                    Type::List(ty) => Ok(Some(Literal::List(
                        v.list
                            .into_iter()
                            .map(|v| {
                                if let Some(v) = v {
                                    v.try_into(&ty.element_field.field_type)
                                } else {
                                    Ok(None)
                                }
                            })
                            .collect::<Result<_, Error>>()?,
                    ))),
                    Type::Map(map_ty) => {
                        let key_ty = map_ty.key_field.field_type.as_ref();
                        let value_ty = map_ty.value_field.field_type.as_ref();
                        let mut map = Map::new();
                        for k_v in v.list {
                            let k_v = k_v.ok_or_else(|| invalid_err_with_reason("list","In deserialize, None will be represented as Some(RawLiteral::Null), all element in list must be valid"))?;
                            if let RawLiteralEnum::Record(Record {
                                required,
                                optional: _,
                            }) = k_v
                            {
                                if required.len() != 2 {
                                    return Err(invalid_err_with_reason(
                                        "list",
                                        "Record must contains two element(key and value) of array",
                                    ));
                                }
                                let mut key = None;
                                let mut value = None;
                                required.into_iter().for_each(|(k, v)| {
                                    if k == MAP_KEY_FIELD_NAME {
                                        key = Some(v);
                                    } else if k == MAP_VALUE_FIELD_NAME {
                                        value = Some(v);
                                    }
                                });
                                match (key, value) {
                                    (Some(k), Some(v)) => {
                                        let key = k.try_into(key_ty)?.ok_or_else(|| {
                                            invalid_err_with_reason(
                                                "list",
                                                "Key element in Map must be valid",
                                            )
                                        })?;
                                        let value = v.try_into(value_ty)?;
                                        if map_ty.value_field.required && value.is_none() {
                                            return Err(invalid_err_with_reason(
                                                "list",
                                                "Value element is required in this Map",
                                            ));
                                        }
                                        map.insert(key, value);
                                    }
                                    _ => {
                                        return Err(invalid_err_with_reason(
                                            "list",
                                            "The elements of record in list are not key and value",
                                        ));
                                    }
                                }
                            } else {
                                return Err(invalid_err_with_reason(
                                    "list",
                                    "Map should represented as record array.",
                                ));
                            }
                        }
                        Ok(Some(Literal::Map(map)))
                    }
                    Type::Primitive(PrimitiveType::Uuid) => {
                        if v.list.len() != 16 {
                            return Err(invalid_err_with_reason(
                                "list",
                                "The length of list should be 16",
                            ));
                        }
                        let mut bytes = [0u8; 16];
                        for (i, v) in v.list.iter().enumerate() {
                            if let Some(RawLiteralEnum::Long(v)) = v {
                                bytes[i] = *v as u8;
                            } else {
                                return Err(invalid_err_with_reason(
                                    "list",
                                    "The element of list should be int",
                                ));
                            }
                        }
                        Ok(Some(Literal::uuid(uuid::Uuid::from_bytes(bytes))))
                    }
                    Type::Primitive(PrimitiveType::Decimal {
                        precision: _,
                        scale: _,
                    }) => {
                        if v.list.len() != 16 {
                            return Err(invalid_err_with_reason(
                                "list",
                                "The length of list should be 16",
                            ));
                        }
                        let mut bytes = [0u8; 16];
                        for (i, v) in v.list.iter().enumerate() {
                            if let Some(RawLiteralEnum::Long(v)) = v {
                                bytes[i] = *v as u8;
                            } else {
                                return Err(invalid_err_with_reason(
                                    "list",
                                    "The element of list should be int",
                                ));
                            }
                        }
                        Ok(Some(Literal::decimal(i128::from_be_bytes(bytes))))
                    }
                    Type::Primitive(PrimitiveType::Binary) => {
                        let bytes = v
                            .list
                            .into_iter()
                            .map(|v| {
                                if let Some(RawLiteralEnum::Long(v)) = v {
                                    Ok(v as u8)
                                } else {
                                    Err(invalid_err_with_reason(
                                        "list",
                                        "The element of list should be int",
                                    ))
                                }
                            })
                            .collect::<Result<Vec<_>, Error>>()?;
                        Ok(Some(Literal::binary(bytes)))
                    }
                    Type::Primitive(PrimitiveType::Fixed(size)) => {
                        if v.list.len() != *size as usize {
                            return Err(invalid_err_with_reason(
                                "list",
                                "The length of list should be equal to size",
                            ));
                        }
                        let bytes = v
                            .list
                            .into_iter()
                            .map(|v| {
                                if let Some(RawLiteralEnum::Long(v)) = v {
                                    Ok(v as u8)
                                } else {
                                    Err(invalid_err_with_reason(
                                        "list",
                                        "The element of list should be int",
                                    ))
                                }
                            })
                            .collect::<Result<Vec<_>, Error>>()?;
                        Ok(Some(Literal::fixed(bytes)))
                    }
                    _ => Err(invalid_err("list")),
                },
                RawLiteralEnum::Record(Record {
                    required,
                    optional: _,
                }) => match ty {
                    Type::Struct(struct_ty) => {
                        let iters: Vec<Option<Literal>> = required
                            .into_iter()
                            .map(|(field_name, value)| {
                                let field = struct_ty
                                    .field_by_name(field_name.as_str())
                                    .ok_or_else(|| {
                                        invalid_err_with_reason(
                                            "record",
                                            &format!("field {} is not exist", &field_name),
                                        )
                                    })?;
                                let value = value.try_into(&field.field_type)?;
                                Ok(value)
                            })
                            .collect::<Result<_, Error>>()?;
                        Ok(Some(Literal::Struct(Struct::from_iter(iters))))
                    }
                    Type::Map(map_ty) => {
                        if *map_ty.key_field.field_type != Type::Primitive(PrimitiveType::String) {
                            return Err(invalid_err_with_reason(
                                "record",
                                "Map key must be string",
                            ));
                        }
                        let mut map = Map::new();
                        for (k, v) in required {
                            let value = v.try_into(&map_ty.value_field.field_type)?;
                            if map_ty.value_field.required && value.is_none() {
                                return Err(invalid_err_with_reason(
                                    "record",
                                    "Value element is required in this Map",
                                ));
                            }
                            map.insert(Literal::string(k), value);
                        }
                        Ok(Some(Literal::Map(map)))
                    }
                    _ => Err(invalid_err("record")),
                },
                RawLiteralEnum::StringMap(_) => Err(invalid_err("string map")),
            }
        }
    }
}
