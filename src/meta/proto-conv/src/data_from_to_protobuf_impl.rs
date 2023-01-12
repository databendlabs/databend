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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use std::str::FromStr;

use chrono::DateTime;
use chrono::Utc;
use common_datavalues as dv;
use common_protos::pb;
use common_protos::pb::data_type::Dt;
use num::FromPrimitive;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for dv::DataSchema {
    type PB = pb::DataSchema;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DataSchema) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let mut fs = Vec::with_capacity(p.fields.len());
        for f in p.fields.into_iter() {
            fs.push(dv::DataField::from_pb(f)?);
        }

        let v = Self::new_from(fs, p.metadata);
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DataSchema, Incompatible> {
        let mut fs = Vec::with_capacity(self.fields().len());
        for f in self.fields().iter() {
            fs.push(f.to_pb()?);
        }

        let p = pb::DataSchema {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            fields: fs,
            metadata: self.meta().clone(),
        };
        Ok(p)
    }
}

impl FromToProto for dv::DataField {
    type PB = pb::DataField;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DataField) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = dv::DataField::new(
            &p.name,
            dv::DataTypeImpl::from_pb(p.data_type.ok_or_else(|| Incompatible {
                reason: "DataField.data_type can not be None".to_string(),
            })?)?,
        )
        .with_default_expr(p.default_expr);
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DataField, Incompatible> {
        let p = pb::DataField {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name().clone(),
            default_expr: self.default_expr().cloned(),
            data_type: Some(self.data_type().to_pb()?),
        };
        Ok(p)
    }
}

impl FromToProto for dv::DataTypeImpl {
    type PB = pb::DataType;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DataType) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let dt = match p.dt {
            None => {
                return Err(Incompatible {
                    reason: "DataType is None".to_string(),
                });
            }
            Some(x) => x,
        };

        match dt {
            Dt::NullType(_) => Ok(dv::DataTypeImpl::Null(dv::NullType {})),
            Dt::NullableType(x) => Ok(dv::DataTypeImpl::Nullable(dv::NullableType::from_pb(
                x.as_ref().clone(),
            )?)),
            Dt::BoolType(_) => Ok(dv::DataTypeImpl::Boolean(dv::BooleanType {})),
            Dt::Int8Type(_) => Ok(dv::DataTypeImpl::Int8(Default::default())),
            Dt::Int16Type(_) => Ok(dv::DataTypeImpl::Int16(Default::default())),
            Dt::Int32Type(_) => Ok(dv::DataTypeImpl::Int32(Default::default())),
            Dt::Int64Type(_) => Ok(dv::DataTypeImpl::Int64(Default::default())),
            Dt::Uint8Type(_) => Ok(dv::DataTypeImpl::UInt8(Default::default())),
            Dt::Uint16Type(_) => Ok(dv::DataTypeImpl::UInt16(Default::default())),
            Dt::Uint32Type(_) => Ok(dv::DataTypeImpl::UInt32(Default::default())),
            Dt::Uint64Type(_) => Ok(dv::DataTypeImpl::UInt64(Default::default())),
            Dt::Float32Type(_) => Ok(dv::DataTypeImpl::Float32(Default::default())),
            Dt::Float64Type(_) => Ok(dv::DataTypeImpl::Float64(Default::default())),
            Dt::DateType(_) => Ok(dv::DataTypeImpl::Date(dv::DateType {})),
            Dt::TimestampType(x) => Ok(dv::DataTypeImpl::Timestamp(dv::TimestampType::from_pb(x)?)),
            Dt::StringType(_) => Ok(dv::DataTypeImpl::String(dv::StringType {})),
            Dt::StructType(x) => Ok(dv::DataTypeImpl::Struct(dv::StructType::from_pb(x)?)),
            Dt::ArrayType(x) => Ok(dv::DataTypeImpl::Array(dv::ArrayType::from_pb(
                x.as_ref().clone(),
            )?)),
            Dt::VariantType(_) => Ok(dv::DataTypeImpl::Variant(dv::VariantType {})),
            Dt::VariantArrayType(_) => Ok(dv::DataTypeImpl::VariantArray(dv::VariantArrayType {})),
            Dt::VariantObjectType(_) => {
                Ok(dv::DataTypeImpl::VariantObject(dv::VariantObjectType {}))
            }
            Dt::IntervalType(x) => Ok(dv::DataTypeImpl::Interval(dv::IntervalType::from_pb(x)?)),
        }
    }

    fn to_pb(&self) -> Result<pb::DataType, Incompatible> {
        match self {
            dv::DataTypeImpl::Null(_) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::NullType(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::Nullable(x) => {
                let inn = x.to_pb()?;

                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::NullableType(Box::new(inn))),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::Boolean(_) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::BoolType(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::Int8(_) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::Int8Type(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::Int16(_) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::Int16Type(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::Int32(_) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::Int32Type(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::Int64(_) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::Int64Type(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::UInt8(_) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::Uint8Type(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::UInt16(_) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::Uint16Type(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::UInt32(_) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::Uint32Type(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::UInt64(_) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::Uint64Type(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::Float32(_) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::Float32Type(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::Float64(_) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::Float64Type(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::Date(_x) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::DateType(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::Timestamp(x) => {
                let inn = x.to_pb()?;

                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::TimestampType(inn)),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::String(_x) => {
                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::StringType(pb::Empty {})),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::Struct(x) => {
                let inn = x.to_pb()?;

                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::StructType(inn)),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::Array(x) => {
                let inn = x.to_pb()?;

                let v = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::ArrayType(Box::new(inn))),
                    dt24: None,
                };
                Ok(v)
            }
            dv::DataTypeImpl::Variant(x) => {
                let inn = x.to_pb()?;

                let p = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::VariantType(inn)),
                    dt24: None,
                };
                Ok(p)
            }
            dv::DataTypeImpl::VariantArray(x) => {
                let inn = x.to_pb()?;

                let p = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::VariantArrayType(inn)),
                    dt24: None,
                };
                Ok(p)
            }
            dv::DataTypeImpl::VariantObject(x) => {
                let inn = x.to_pb()?;

                let p = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::VariantObjectType(inn)),
                    dt24: None,
                };
                Ok(p)
            }
            dv::DataTypeImpl::Interval(x) => {
                let inn = x.to_pb()?;

                let p = pb::DataType {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    dt: Some(Dt::IntervalType(inn)),
                    dt24: None,
                };
                Ok(p)
            }
        }
    }
}

impl FromToProto for dv::NullableType {
    type PB = pb::NullableType;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::NullableType) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let inner = p.inner.ok_or_else(|| Incompatible {
            reason: "NullableType.inner can not be None".to_string(),
        })?;

        let inner_dt = dv::DataTypeImpl::from_pb(inner.as_ref().clone())?;

        Ok(dv::NullableType::create(inner_dt))
    }

    fn to_pb(&self) -> Result<pb::NullableType, Incompatible> {
        let inner = self.inner_type();
        let inner_pb_type = inner.to_pb()?;

        let p = pb::NullableType {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            inner: Some(Box::new(inner_pb_type)),
        };

        Ok(p)
    }
}

impl FromToProto for dv::TimestampType {
    type PB = pb::Timestamp;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::Timestamp) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(dv::TimestampType::default())
    }

    fn to_pb(&self) -> Result<pb::Timestamp, Incompatible> {
        let p = pb::Timestamp {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
        };

        Ok(p)
    }
}

impl FromToProto for dv::StructType {
    type PB = pb::Struct;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::Struct) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let names = p.names.clone();

        let mut types = Vec::with_capacity(p.types.len());
        for t in p.types.into_iter() {
            types.push(dv::DataTypeImpl::from_pb(t)?);
        }
        if names.is_empty() {
            Ok(dv::StructType::create(None, types))
        } else {
            debug_assert!(
                names.len() == types.len(),
                "Size of names must match size of types"
            );
            Ok(dv::StructType::create(Some(names), types))
        }
    }

    fn to_pb(&self) -> Result<pb::Struct, Incompatible> {
        let names = match self.names() {
            Some(names) => names.clone(),
            None => Vec::new(),
        };
        let mut types = Vec::with_capacity(self.types().len());

        for t in self.types().iter() {
            types.push(t.to_pb()?);
        }

        let p = pb::Struct {
            ver: VER,
            min_reader_ver: MIN_READER_VER,

            names,
            types,
        };

        Ok(p)
    }
}

impl FromToProto for dv::ArrayType {
    type PB = pb::Array;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::Array) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let inner = p.inner.ok_or_else(|| Incompatible {
            reason: "Array.inner can not be None".to_string(),
        })?;

        let inner_dt = dv::DataTypeImpl::from_pb(inner.as_ref().clone())?;

        Ok(dv::ArrayType::create(inner_dt))
    }

    fn to_pb(&self) -> Result<pb::Array, Incompatible> {
        let inner = self.inner_type();
        let inner_pb_type = inner.to_pb()?;

        let p = pb::Array {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            inner: Some(Box::new(inner_pb_type)),
        };

        Ok(p)
    }
}

impl FromToProto for dv::VariantArrayType {
    type PB = pb::VariantArray;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::VariantArray) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {})
    }

    fn to_pb(&self) -> Result<pb::VariantArray, Incompatible> {
        let p = pb::VariantArray {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
        };
        Ok(p)
    }
}

impl FromToProto for dv::VariantObjectType {
    type PB = pb::VariantObject;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::VariantObject) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {})
    }

    fn to_pb(&self) -> Result<pb::VariantObject, Incompatible> {
        let p = pb::VariantObject {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
        };
        Ok(p)
    }
}

impl FromToProto for dv::IntervalKind {
    type PB = pb::IntervalKind;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }
    fn from_pb(p: pb::IntervalKind) -> Result<Self, Incompatible>
    where Self: Sized {
        let dv_kind = match p {
            pb::IntervalKind::Year => dv::IntervalKind::Year,
            pb::IntervalKind::Quarter => dv::IntervalKind::Quarter,
            pb::IntervalKind::Month => dv::IntervalKind::Month,
            pb::IntervalKind::Day => dv::IntervalKind::Day,
            pb::IntervalKind::Hour => dv::IntervalKind::Hour,
            pb::IntervalKind::Minute => dv::IntervalKind::Minute,
            pb::IntervalKind::Second => dv::IntervalKind::Second,
            pb::IntervalKind::Doy => dv::IntervalKind::Doy,
            pb::IntervalKind::Dow => dv::IntervalKind::Dow,
        };

        Ok(dv_kind)
    }

    fn to_pb(&self) -> Result<pb::IntervalKind, Incompatible> {
        let pb_kind = match self {
            dv::IntervalKind::Year => pb::IntervalKind::Year,
            dv::IntervalKind::Quarter => pb::IntervalKind::Quarter,
            dv::IntervalKind::Month => pb::IntervalKind::Month,
            dv::IntervalKind::Day => pb::IntervalKind::Day,
            dv::IntervalKind::Hour => pb::IntervalKind::Hour,
            dv::IntervalKind::Minute => pb::IntervalKind::Minute,
            dv::IntervalKind::Second => pb::IntervalKind::Second,
            dv::IntervalKind::Doy => pb::IntervalKind::Doy,
            dv::IntervalKind::Dow => pb::IntervalKind::Dow,
        };
        Ok(pb_kind)
    }
}
impl FromToProto for dv::IntervalType {
    type PB = pb::IntervalType;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::IntervalType) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let pb_kind: pb::IntervalKind =
            FromPrimitive::from_i32(p.kind).ok_or_else(|| Incompatible {
                reason: format!("invalid IntervalType: {}", p.kind),
            })?;

        let dv_kind = dv::IntervalKind::from_pb(pb_kind)?;
        Ok(Self::new(dv_kind))
    }

    fn to_pb(&self) -> Result<pb::IntervalType, Incompatible> {
        let pb_kind = self.kind().to_pb()?;
        let p = pb::IntervalType {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            kind: pb_kind as i32,
        };
        Ok(p)
    }
}

impl FromToProto for dv::VariantType {
    type PB = pb::Variant;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::Variant) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {})
    }

    fn to_pb(&self) -> Result<pb::Variant, Incompatible> {
        let p = pb::Variant {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
        };
        Ok(p)
    }
}

impl FromToProto for DateTime<Utc> {
    type PB = String;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }

    fn from_pb(p: String) -> Result<Self, Incompatible> {
        let v = DateTime::<Utc>::from_str(&p).map_err(|e| Incompatible {
            reason: format!("DateTime error: {}", e),
        })?;
        Ok(v)
    }

    fn to_pb(&self) -> Result<String, Incompatible> {
        let p = self.to_string();
        Ok(p)
    }
}
