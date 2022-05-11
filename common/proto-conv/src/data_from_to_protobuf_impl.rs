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

use common_datavalues as dv;
use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use common_protos::pb;
use common_protos::pb::data_type::Dt;
use num::FromPrimitive;

use crate::check_ver;
use crate::FromToProto;
use crate::Incompatible;
use crate::VER;

impl FromToProto<pb::DataSchema> for dv::DataSchema {
    fn from_pb(p: pb::DataSchema) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

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
            fields: fs,
            metadata: self.meta().clone(),
        };
        Ok(p)
    }
}

impl FromToProto<pb::DataField> for dv::DataField {
    fn from_pb(p: pb::DataField) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

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
            name: self.name().clone(),
            default_expr: self.default_expr().cloned(),
            data_type: Some(self.data_type().to_pb()?),
        };
        Ok(p)
    }
}

impl FromToProto<pb::DataType> for dv::DataTypeImpl {
    fn from_pb(p: pb::DataType) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let dt = match p.dt {
            None => {
                return Err(Incompatible {
                    reason: "DataType is None".to_string(),
                })
            }
            Some(x) => x,
        };

        match dt {
            Dt::NullableType(x) => Ok(dv::DataTypeImpl::Nullable(dv::NullableType::from_pb(
                x.as_ref().clone(),
            )?)),
            Dt::BoolType(_) => Ok(dv::DataTypeImpl::Boolean(dv::BooleanType {})),
            Dt::Int8Type(_) => Ok(dv::DataTypeImpl::Int8(dv::Int8Type::default())),
            Dt::Int16Type(_) => Ok(dv::DataTypeImpl::Int16(dv::Int16Type::default())),
            Dt::Int32Type(_) => Ok(dv::DataTypeImpl::Int32(dv::Int32Type::default())),
            Dt::Int64Type(_) => Ok(dv::DataTypeImpl::Int64(dv::Int64Type::default())),
            Dt::Uint8Type(_) => Ok(dv::DataTypeImpl::UInt8(dv::UInt8Type::default())),
            Dt::Uint16Type(_) => Ok(dv::DataTypeImpl::UInt16(dv::UInt16Type::default())),
            Dt::Uint32Type(_) => Ok(dv::DataTypeImpl::UInt32(dv::UInt32Type::default())),
            Dt::Uint64Type(_) => Ok(dv::DataTypeImpl::UInt64(dv::UInt64Type::default())),
            Dt::Float32Type(_) => Ok(dv::DataTypeImpl::Float32(dv::Float32Type::default())),
            Dt::Float64Type(_) => Ok(dv::DataTypeImpl::Float64(dv::Float64Type::default())),
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
                todo!()
            }
            dv::DataTypeImpl::Nullable(x) => {
                let inn = x.to_pb()?;

                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::NullableType(Box::new(inn))),
                };
                Ok(v)
            }
            dv::DataTypeImpl::Boolean(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::BoolType(pb::Empty {})),
                };
                Ok(v)
            }
            dv::DataTypeImpl::Int8(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Int8Type(pb::Empty {})),
                };
                Ok(v)
            }
            dv::DataTypeImpl::Int16(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Int16Type(pb::Empty {})),
                };
                Ok(v)
            }
            dv::DataTypeImpl::Int32(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Int32Type(pb::Empty {})),
                };
                Ok(v)
            }
            dv::DataTypeImpl::Int64(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Int64Type(pb::Empty {})),
                };
                Ok(v)
            }
            dv::DataTypeImpl::UInt8(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Uint8Type(pb::Empty {})),
                };
                Ok(v)
            }
            dv::DataTypeImpl::UInt16(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Uint16Type(pb::Empty {})),
                };
                Ok(v)
            }
            dv::DataTypeImpl::UInt32(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Uint32Type(pb::Empty {})),
                };
                Ok(v)
            }
            dv::DataTypeImpl::UInt64(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Uint64Type(pb::Empty {})),
                };
                Ok(v)
            }
            dv::DataTypeImpl::Float32(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Float32Type(pb::Empty {})),
                };
                Ok(v)
            }
            dv::DataTypeImpl::Float64(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Float64Type(pb::Empty {})),
                };
                Ok(v)
            }
            dv::DataTypeImpl::Date(_x) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::DateType(pb::Empty {})),
                };
                Ok(v)
            }
            dv::DataTypeImpl::Timestamp(x) => {
                let inn = x.to_pb()?;

                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::TimestampType(inn)),
                };
                Ok(v)
            }
            dv::DataTypeImpl::String(_x) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::StringType(pb::Empty {})),
                };
                Ok(v)
            }
            dv::DataTypeImpl::Struct(x) => {
                let inn = x.to_pb()?;

                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::StructType(inn)),
                };
                Ok(v)
            }
            dv::DataTypeImpl::Array(x) => {
                let inn = x.to_pb()?;

                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::ArrayType(Box::new(inn))),
                };
                Ok(v)
            }
            dv::DataTypeImpl::Variant(x) => {
                let inn = x.to_pb()?;

                let p = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::VariantType(inn)),
                };
                Ok(p)
            }
            dv::DataTypeImpl::VariantArray(x) => {
                let inn = x.to_pb()?;

                let p = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::VariantArrayType(inn)),
                };
                Ok(p)
            }
            dv::DataTypeImpl::VariantObject(x) => {
                let inn = x.to_pb()?;

                let p = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::VariantObjectType(inn)),
                };
                Ok(p)
            }
            dv::DataTypeImpl::Interval(x) => {
                let inn = x.to_pb()?;

                let p = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::IntervalType(inn)),
                };
                Ok(p)
            }
        }
    }
}

impl FromToProto<pb::NullableType> for dv::NullableType {
    fn from_pb(p: pb::NullableType) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver)?;

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
            inner: Some(Box::new(inner_pb_type)),
        };

        Ok(p)
    }
}

impl FromToProto<pb::Timestamp> for dv::TimestampType {
    fn from_pb(p: pb::Timestamp) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver)?;
        let v = dv::TimestampType::create(p.precision as usize);
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::Timestamp, Incompatible> {
        let p = pb::Timestamp {
            ver: VER,
            precision: self.precision() as u64,
            // tz: self.tz().cloned(),
        };

        Ok(p)
    }
}

impl FromToProto<pb::Struct> for dv::StructType {
    fn from_pb(p: pb::Struct) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver)?;
        let names = p.names.clone();

        let mut types = Vec::with_capacity(p.types.len());
        for t in p.types.into_iter() {
            types.push(dv::DataTypeImpl::from_pb(t)?);
        }

        Ok(dv::StructType::create(names, types))
    }

    fn to_pb(&self) -> Result<pb::Struct, Incompatible> {
        let names = self.names().clone();

        let mut types = Vec::with_capacity(self.types().len());

        for t in self.types().iter() {
            types.push(t.to_pb()?);
        }

        let p = pb::Struct {
            ver: VER,

            names,
            types,
        };

        Ok(p)
    }
}

impl FromToProto<pb::Array> for dv::ArrayType {
    fn from_pb(p: pb::Array) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver)?;

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
            inner: Some(Box::new(inner_pb_type)),
        };

        Ok(p)
    }
}

impl FromToProto<pb::VariantArray> for dv::VariantArrayType {
    fn from_pb(p: pb::VariantArray) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver)?;

        Ok(Self {})
    }

    fn to_pb(&self) -> Result<pb::VariantArray, Incompatible> {
        let p = pb::VariantArray { ver: VER };
        Ok(p)
    }
}

impl FromToProto<pb::VariantObject> for dv::VariantObjectType {
    fn from_pb(p: pb::VariantObject) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver)?;

        Ok(Self {})
    }

    fn to_pb(&self) -> Result<pb::VariantObject, Incompatible> {
        let p = pb::VariantObject { ver: VER };
        Ok(p)
    }
}

impl FromToProto<pb::IntervalKind> for dv::IntervalKind {
    fn from_pb(p: pb::IntervalKind) -> Result<Self, Incompatible>
    where Self: Sized {
        let dv_kind = match p {
            pb::IntervalKind::Year => dv::IntervalKind::Year,
            pb::IntervalKind::Month => dv::IntervalKind::Month,
            pb::IntervalKind::Day => dv::IntervalKind::Day,
            pb::IntervalKind::Hour => dv::IntervalKind::Hour,
            pb::IntervalKind::Minute => dv::IntervalKind::Minute,
            pb::IntervalKind::Second => dv::IntervalKind::Second,
        };

        Ok(dv_kind)
    }

    fn to_pb(&self) -> Result<pb::IntervalKind, Incompatible> {
        let pb_kind = match self {
            dv::IntervalKind::Year => pb::IntervalKind::Year,
            dv::IntervalKind::Month => pb::IntervalKind::Month,
            dv::IntervalKind::Day => pb::IntervalKind::Day,
            dv::IntervalKind::Hour => pb::IntervalKind::Hour,
            dv::IntervalKind::Minute => pb::IntervalKind::Minute,
            dv::IntervalKind::Second => pb::IntervalKind::Second,
        };
        Ok(pb_kind)
    }
}
impl FromToProto<pb::IntervalType> for dv::IntervalType {
    fn from_pb(p: pb::IntervalType) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver)?;

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
            kind: pb_kind as i32,
        };
        Ok(p)
    }
}

impl FromToProto<pb::Variant> for dv::VariantType {
    fn from_pb(p: pb::Variant) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver)?;

        Ok(Self {})
    }

    fn to_pb(&self) -> Result<pb::Variant, Incompatible> {
        let p = pb::Variant { ver: VER };
        Ok(p)
    }
}

impl FromToProto<String> for DateTime<Utc> {
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
