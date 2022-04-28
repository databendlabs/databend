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
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common_datavalues as dv;
use common_datavalues::DataTypeImpl;
use common_meta_types as mt;
use common_protos::pb;
use common_protos::pb::data_type::Dt;
use common_protos::pb::Variant;

use crate::protos::from_to_protobuf::FromToProto;
use crate::protos::from_to_protobuf::Incompatible;

const VER: u64 = 1;
const OLDEST_COMPATIBLE_VER: u64 = 1;

impl FromToProto<pb::DatabaseInfo> for mt::DatabaseInfo {
    fn from_pb(p: pb::DatabaseInfo) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let meta = match p.meta {
            None => {
                return Err(Incompatible {
                    reason: "DatabaseInfo.meta can not be None".to_string(),
                })
            }
            Some(x) => x,
        };

        let v = Self {
            database_id: p.db_id,
            db: p.db_name,
            meta: mt::DatabaseMeta::from_pb(meta)?,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DatabaseInfo, Incompatible> {
        let p = pb::DatabaseInfo {
            ver: VER,
            db_id: self.database_id,
            db_name: self.db.clone(),
            meta: Some(self.meta.to_pb()?),
        };
        Ok(p)
    }
}

impl FromToProto<pb::DatabaseNameIdent> for mt::DatabaseNameIdent {
    fn from_pb(p: pb::DatabaseNameIdent) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let v = Self {
            tenant: p.tenant,
            db_name: p.db_name,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DatabaseNameIdent, Incompatible> {
        let p = pb::DatabaseNameIdent {
            ver: VER,
            tenant: self.tenant.clone(),
            db_name: self.db_name.clone(),
        };
        Ok(p)
    }
}

impl FromToProto<pb::DatabaseIdent> for mt::DatabaseIdent {
    fn from_pb(p: pb::DatabaseIdent) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let v = Self {
            db_id: p.db_id,
            seq: p.seq,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DatabaseIdent, Incompatible> {
        let p = pb::DatabaseIdent {
            ver: VER,
            db_id: self.db_id,
            seq: self.seq,
        };
        Ok(p)
    }
}

impl FromToProto<pb::DatabaseMeta> for mt::DatabaseMeta {
    fn from_pb(p: pb::DatabaseMeta) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let v = Self {
            engine: p.engine,
            engine_options: p.engine_options,
            options: p.options,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            updated_on: DateTime::<Utc>::from_pb(p.updated_on)?,
            comment: p.comment,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DatabaseMeta, Incompatible> {
        let p = pb::DatabaseMeta {
            ver: VER,
            engine: self.engine.clone(),
            engine_options: self.engine_options.clone(),
            options: self.options.clone(),
            created_on: self.created_on.to_pb()?,
            updated_on: self.updated_on.to_pb()?,
            comment: self.comment.clone(),
        };
        Ok(p)
    }
}

impl FromToProto<pb::TableInfo> for mt::TableInfo {
    fn from_pb(p: pb::TableInfo) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let ident = match p.ident {
            None => {
                return Err(Incompatible {
                    reason: "TableInfo.ident can not be None".to_string(),
                })
            }
            Some(x) => x,
        };
        let v = Self {
            ident: mt::TableIdent::from_pb(ident)?,
            desc: p.desc,
            name: p.name,
            meta: mt::TableMeta::from_pb(p.meta.ok_or_else(|| Incompatible {
                reason: "TableInfo.meta can not be None".to_string(),
            })?)?,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableInfo, Incompatible> {
        let p = pb::TableInfo {
            ver: VER,
            ident: Some(self.ident.to_pb()?),
            desc: self.desc.clone(),
            name: self.name.clone(),
            meta: Some(self.meta.to_pb()?),
        };
        Ok(p)
    }
}

impl FromToProto<pb::TableNameIdent> for mt::TableNameIndent {
    fn from_pb(p: pb::TableNameIdent) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let v = Self {
            tenant: p.tenant,
            db_name: p.db_name,
            table_name: p.table_name,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableNameIdent, Incompatible> {
        let p = pb::TableNameIdent {
            ver: VER,
            tenant: self.tenant.clone(),
            db_name: self.db_name.clone(),
            table_name: self.table_name.clone(),
        };
        Ok(p)
    }
}

impl FromToProto<pb::TableIdent> for mt::TableIdent {
    fn from_pb(p: pb::TableIdent) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let v = Self {
            table_id: p.table_id,
            version: p.seq,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableIdent, Incompatible> {
        let p = pb::TableIdent {
            ver: VER,
            table_id: self.table_id,
            seq: self.version,
        };

        Ok(p)
    }
}

impl FromToProto<pb::TableMeta> for mt::TableMeta {
    fn from_pb(p: pb::TableMeta) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let schema = match p.schema {
            None => {
                return Err(Incompatible {
                    reason: "TableMeta.schema can not be None".to_string(),
                })
            }
            Some(x) => x,
        };

        let v = Self {
            schema: Arc::new(dv::DataSchema::from_pb(schema)?),
            engine: p.engine,
            engine_options: p.engine_options,
            options: p.options,
            order_keys: p.order_keys,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            updated_on: DateTime::<Utc>::from_pb(p.updated_on)?,
            comment: p.comment,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableMeta, Incompatible> {
        let p = pb::TableMeta {
            ver: VER,
            schema: Some(self.schema.to_pb()?),
            engine: self.engine.clone(),
            engine_options: self.engine_options.clone(),
            options: self.options.clone(),
            order_keys: self.order_keys.clone(),
            created_on: self.created_on.to_pb()?,
            updated_on: self.updated_on.to_pb()?,
            comment: self.comment.clone(),
        };
        Ok(p)
    }
}

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
            default_expr: self.default_expr().clone(),
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
        }
    }

    fn to_pb(&self) -> Result<pb::DataType, Incompatible> {
        match self {
            DataTypeImpl::Null(_) => {
                todo!()
            }
            DataTypeImpl::Nullable(x) => {
                let inn = x.to_pb()?;

                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::NullableType(Box::new(inn))),
                };
                Ok(v)
            }
            DataTypeImpl::Boolean(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::BoolType(pb::Empty {})),
                };
                Ok(v)
            }
            DataTypeImpl::Int8(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Int8Type(pb::Empty {})),
                };
                Ok(v)
            }
            DataTypeImpl::Int16(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Int16Type(pb::Empty {})),
                };
                Ok(v)
            }
            DataTypeImpl::Int32(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Int32Type(pb::Empty {})),
                };
                Ok(v)
            }
            DataTypeImpl::Int64(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Int64Type(pb::Empty {})),
                };
                Ok(v)
            }
            DataTypeImpl::UInt8(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Uint8Type(pb::Empty {})),
                };
                Ok(v)
            }
            DataTypeImpl::UInt16(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Uint16Type(pb::Empty {})),
                };
                Ok(v)
            }
            DataTypeImpl::UInt32(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Uint32Type(pb::Empty {})),
                };
                Ok(v)
            }
            DataTypeImpl::UInt64(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Uint64Type(pb::Empty {})),
                };
                Ok(v)
            }
            DataTypeImpl::Float32(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Float32Type(pb::Empty {})),
                };
                Ok(v)
            }
            DataTypeImpl::Float64(_) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::Float64Type(pb::Empty {})),
                };
                Ok(v)
            }
            DataTypeImpl::Date(_x) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::DateType(pb::Empty {})),
                };
                Ok(v)
            }
            DataTypeImpl::Timestamp(x) => {
                let inn = x.to_pb()?;

                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::TimestampType(inn)),
                };
                Ok(v)
            }
            DataTypeImpl::String(_x) => {
                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::StringType(pb::Empty {})),
                };
                Ok(v)
            }
            DataTypeImpl::Struct(x) => {
                let inn = x.to_pb()?;

                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::StructType(inn)),
                };
                Ok(v)
            }
            DataTypeImpl::Array(x) => {
                let inn = x.to_pb()?;

                let v = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::ArrayType(Box::new(inn))),
                };
                Ok(v)
            }
            DataTypeImpl::Variant(x) => {
                let inn = x.to_pb()?;

                let p = pb::DataType {
                    ver: VER,
                    dt: Some(Dt::VariantType(inn)),
                };
                Ok(p)
            }
            DataTypeImpl::VariantArray(_x) => {
                todo!()
            }
            DataTypeImpl::VariantObject(_x) => {
                todo!()
            }
            DataTypeImpl::Interval(_x) => {
                todo!()
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
        let v = dv::TimestampType::create(p.precision as usize, p.tz);
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::Timestamp, Incompatible> {
        let p = pb::Timestamp {
            ver: VER,
            precision: self.precision() as u64,
            tz: self.tz().cloned(),
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

impl FromToProto<pb::Variant> for dv::VariantType {
    fn from_pb(p: pb::Variant) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver)?;

        Ok(Self {})
    }

    fn to_pb(&self) -> Result<Variant, Incompatible> {
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

fn check_ver(ver: u64) -> Result<(), Incompatible> {
    if ver > VER || ver < OLDEST_COMPATIBLE_VER {
        return Err(Incompatible {
            reason: format!(
                "ver={} is not compatible with [{}, {}]",
                ver, OLDEST_COMPATIBLE_VER, VER
            ),
        });
    }
    Ok(())
}
