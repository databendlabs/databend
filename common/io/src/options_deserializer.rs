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

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;

use serde::de;
use serde::de::DeserializeSeed;
use serde::de::EnumAccess;
use serde::de::Error;
use serde::de::MapAccess;
use serde::de::VariantAccess;
use serde::de::Visitor;
use serde::forward_to_deserialize_any;
use serde::Deserializer;

/// This type represents errors that can occur when deserializing.
#[derive(Debug, Eq, PartialEq)]
pub struct OptionsDeserializerError(pub String);

impl de::Error for OptionsDeserializerError {
    #[inline]
    fn custom<T: Display>(msg: T) -> Self {
        OptionsDeserializerError(msg.to_string())
    }
}

impl std::error::Error for OptionsDeserializerError {
    #[inline]
    fn description(&self) -> &str {
        "options deserializer error"
    }
}

impl fmt::Display for OptionsDeserializerError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OptionsDeserializerError(msg) => write!(f, "{}", msg),
        }
    }
}

macro_rules! unsupported_type {
    ($trait_fn:ident, $name:literal) => {
        fn $trait_fn<V>(self, _: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
            Err(OptionsDeserializerError::custom(concat!(
                "unsupported type: ",
                $name
            )))
        }
    };
}

pub struct OptionsDeserializer<'de> {
    options: &'de HashMap<String, String>,
}

impl<'de> OptionsDeserializer<'de> {
    #[inline]
    pub fn new(options: &'de HashMap<String, String>) -> Self {
        OptionsDeserializer { options }
    }
}

impl<'de> Deserializer<'de> for OptionsDeserializer<'de> {
    type Error = OptionsDeserializerError;

    unsupported_type!(deserialize_any, "'any'");
    unsupported_type!(deserialize_unit, "()");
    unsupported_type!(deserialize_bytes, "bytes");
    unsupported_type!(deserialize_byte_buf, "bytes");
    unsupported_type!(deserialize_option, "Option<T>");
    unsupported_type!(deserialize_identifier, "identifier");
    unsupported_type!(deserialize_ignored_any, "ignored_any");
    unsupported_type!(deserialize_bool, "bool");
    unsupported_type!(deserialize_i8, "i8");
    unsupported_type!(deserialize_i16, "i16");
    unsupported_type!(deserialize_i32, "i32");
    unsupported_type!(deserialize_i64, "i64");
    unsupported_type!(deserialize_u8, "i8");
    unsupported_type!(deserialize_u16, "i16");
    unsupported_type!(deserialize_u32, "i32");
    unsupported_type!(deserialize_u64, "i64");
    unsupported_type!(deserialize_f32, "f32");
    unsupported_type!(deserialize_f64, "f64");
    unsupported_type!(deserialize_char, "char");
    unsupported_type!(deserialize_str, "char");
    unsupported_type!(deserialize_string, "string");
    unsupported_type!(deserialize_seq, "seq");

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de> {
        visitor.visit_map(MapDeserializer {
            options: self.options.iter(),
            value: None,
        })
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(OptionsDeserializerError::custom(
            "unsupported type: unit_struct",
        ))
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(OptionsDeserializerError::custom(
            "unsupported type: newtype_struct",
        ))
    }

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de> {
        Err(OptionsDeserializerError::custom("unsupported type: tuple"))
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(OptionsDeserializerError::custom(
            "unsupported type: tuple_struct",
        ))
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(OptionsDeserializerError::custom("unsupported type: enum"))
    }
}

struct MapDeserializer<'de> {
    options: std::collections::hash_map::Iter<'de, String, String>,
    value: Option<&'de String>,
}

impl<'de> MapAccess<'de> for MapDeserializer<'de> {
    type Error = OptionsDeserializerError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where K: DeserializeSeed<'de> {
        match self.options.next() {
            Some((key, value)) => {
                self.value = Some(value);
                seed.deserialize(KeyDeserializer { key }).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where V: DeserializeSeed<'de> {
        match self.value.take() {
            Some(value) => seed.deserialize(ValueDeserializer { value }),
            None => Err(serde::de::Error::custom("value is missing")),
        }
    }
}

struct KeyDeserializer<'de> {
    key: &'de str,
}

macro_rules! parse_key {
    ($trait_fn:ident) => {
        fn $trait_fn<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
            visitor.visit_str(&self.key.to_lowercase())
        }
    };
}

impl<'de> Deserializer<'de> for KeyDeserializer<'de> {
    type Error = OptionsDeserializerError;

    parse_key!(deserialize_identifier);
    parse_key!(deserialize_str);
    parse_key!(deserialize_string);

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de> {
        Err(OptionsDeserializerError::custom("Unexpected"))
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char bytes
        byte_buf option unit unit_struct seq tuple
        tuple_struct map newtype_struct struct enum ignored_any
    }
}

macro_rules! parse_value {
    ($trait_fn:ident, $visit_fn:ident, $ty:literal) => {
        fn $trait_fn<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where V: Visitor<'de> {
            let v = self.value.parse().map_err(|e| {
                OptionsDeserializerError::custom(format!(
                    "can not parse `{:?}` to a `{}`, err: {}",
                    self.value, $ty, e
                ))
            })?;
            visitor.$visit_fn(v)
        }
    };
}

pub struct ValueDeserializer<'de> {
    value: &'de str,
}

impl<'de> Deserializer<'de> for ValueDeserializer<'de> {
    type Error = OptionsDeserializerError;

    unsupported_type!(deserialize_any, "any");
    unsupported_type!(deserialize_seq, "seq");
    unsupported_type!(deserialize_map, "map");
    unsupported_type!(deserialize_identifier, "identifier");

    //special case for boolean
    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de> {
        let v = self.value.parse();
        match v {
            Ok(v) => visitor.visit_bool(v),
            Err(e) => match self.value {
                "1" => visitor.visit_bool(true),
                "0" => visitor.visit_bool(false),
                _ => Err(OptionsDeserializerError::custom(format!(
                    "can not parse `{:?}` to a `{}`, error: {}",
                    self.value, "bool", e
                ))),
            },
        }
    }

    parse_value!(deserialize_i8, visit_i8, "i8");
    parse_value!(deserialize_i16, visit_i16, "i16");
    parse_value!(deserialize_i32, visit_i32, "i16");
    parse_value!(deserialize_i64, visit_i64, "i64");
    parse_value!(deserialize_u8, visit_u8, "u8");
    parse_value!(deserialize_u16, visit_u16, "u16");
    parse_value!(deserialize_u32, visit_u32, "u32");
    parse_value!(deserialize_u64, visit_u64, "u64");
    parse_value!(deserialize_f32, visit_f32, "f32");
    parse_value!(deserialize_f64, visit_f64, "f64");
    parse_value!(deserialize_string, visit_string, "String");
    parse_value!(deserialize_byte_buf, visit_string, "String");
    parse_value!(deserialize_char, visit_char, "char");

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de> {
        visitor.visit_borrowed_str(self.value)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de> {
        visitor.visit_borrowed_bytes(self.value.as_bytes())
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de> {
        visitor.visit_some(self)
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de> {
        Err(OptionsDeserializerError::custom("unsupported type: tuple"))
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(OptionsDeserializerError::custom(
            "unsupported type: tuple struct",
        ))
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(OptionsDeserializerError::custom("unsupported type: struct"))
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_enum(EnumDeserializer { value: self.value })
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de> {
        visitor.visit_unit()
    }
}

struct EnumDeserializer<'de> {
    value: &'de str,
}

impl<'de> EnumAccess<'de> for EnumDeserializer<'de> {
    type Error = OptionsDeserializerError;
    type Variant = UnitVariant;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where V: de::DeserializeSeed<'de> {
        Ok((
            seed.deserialize(KeyDeserializer { key: self.value })?,
            UnitVariant,
        ))
    }
}

struct UnitVariant;

impl<'de> VariantAccess<'de> for UnitVariant {
    type Error = OptionsDeserializerError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value, Self::Error>
    where T: DeserializeSeed<'de> {
        Err(OptionsDeserializerError::custom("not supported"))
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where V: Visitor<'de> {
        Err(OptionsDeserializerError::custom("not supported"))
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(OptionsDeserializerError::custom("not supported"))
    }
}
