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

//! This mod is the key point about `User` compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use std::collections::BTreeMap;

use common_datavalues as dv;
use common_protos::pb;
use serde_json::Map;
use serde_json::Number;
use serde_json::Value;

use crate::check_ver;
use crate::FromToProto;
use crate::Incompatible;

const DATA_VER: u64 = 1;
const OLDEST_DATA_COMPATIBLE_VER: u64 = 1;

impl FromToProto<pb::VariantValue> for dv::VariantValue {
    fn from_pb(p: pb::VariantValue) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, DATA_VER, OLDEST_DATA_COMPATIBLE_VER)?;

        Ok(convert_pb_variant_value_to_value(p.value)?.into())
    }

    fn to_pb(&self) -> Result<pb::VariantValue, Incompatible> {
        convert_value_to_pb_variant_value(&self.0)
    }
}

fn convert_value_to_pb_variant_value(value: &Value) -> Result<pb::VariantValue, Incompatible> {
    match value {
        Value::Null => Ok(pb::VariantValue {
            ver: DATA_VER,
            value: Some(pb::variant_value::Value::Null(pb::Empty {})),
        }),
        Value::Bool(v) => Ok(pb::VariantValue {
            ver: DATA_VER,
            value: Some(pb::variant_value::Value::BoolValue(*v)),
        }),
        Value::Number(v) => {
            let value = if v.is_i64() {
                Some(pb::variant_value::Value::I64Value(v.as_i64().ok_or_else(
                    || Incompatible {
                        reason: format!("invalid int64 value"),
                    },
                )?))
            } else if v.is_u64() {
                Some(pb::variant_value::Value::U64Value(v.as_u64().ok_or_else(
                    || Incompatible {
                        reason: format!("invalid uint64 value"),
                    },
                )?))
            } else {
                Some(pb::variant_value::Value::F64Value(v.as_f64().ok_or_else(
                    || Incompatible {
                        reason: format!("invalid float64 value"),
                    },
                )?))
            };
            return Ok(pb::VariantValue {
                ver: DATA_VER,
                value,
            });
        }
        Value::String(v) => Ok(pb::VariantValue {
            ver: DATA_VER,
            value: Some(pb::variant_value::Value::StringValue(v.clone())),
        }),
        Value::Array(v) => {
            let mut array = vec![];
            for v in v.iter() {
                array.push(convert_value_to_pb_variant_value(v)?);
            }
            return Ok(pb::VariantValue {
                ver: DATA_VER,
                value: Some(pb::variant_value::Value::ArrayValue(
                    pb::variant_value::Array { value: array },
                )),
            });
        }
        Value::Object(v) => {
            let mut obj = BTreeMap::new();
            for (k, v) in v.iter() {
                obj.insert(k.clone(), convert_value_to_pb_variant_value(v)?);
            }
            return Ok(pb::VariantValue {
                ver: DATA_VER,
                value: Some(pb::variant_value::Value::ObjectValue(
                    pb::variant_value::Object { obj },
                )),
            });
        }
    }
}

fn convert_pb_variant_value_to_value(
    pb_value: Option<pb::variant_value::Value>,
) -> Result<Value, Incompatible> {
    match pb_value {
        Some(pb::variant_value::Value::Null(_)) => Ok(Value::Null.into()),
        Some(pb::variant_value::Value::BoolValue(v)) => Ok(Value::Bool(v).into()),
        Some(pb::variant_value::Value::I64Value(v)) => Ok(Value::Number(v.into()).into()),
        Some(pb::variant_value::Value::U64Value(v)) => Ok(Value::Number(v.into()).into()),
        Some(pb::variant_value::Value::F64Value(v)) => Ok(Value::Number(
            Number::from_f64(v).ok_or_else(|| Incompatible {
                reason: format!("invalid float64 value"),
            })?,
        )
        .into()),
        Some(pb::variant_value::Value::StringValue(v)) => Ok(Value::String(v).into()),
        Some(pb::variant_value::Value::ArrayValue(v)) => {
            let mut array = vec![];
            for value in v.value.iter() {
                array.push(convert_pb_variant_value_to_value(value.value.clone())?);
            }
            Ok(Value::Array(array).into())
        }
        Some(pb::variant_value::Value::ObjectValue(v)) => {
            let mut obj = Map::new();
            for (key, value) in v.obj.iter() {
                obj.insert(
                    key.clone(),
                    convert_pb_variant_value_to_value(value.value.clone())?,
                );
            }
            Ok(Value::Object(obj).into())
        }
        None => Err(Incompatible {
            reason: "pb::variant_value::Value can not be None".to_string(),
        }),
    }
}

impl FromToProto<pb::DataValue> for dv::DataValue {
    fn from_pb(p: pb::DataValue) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, DATA_VER, OLDEST_DATA_COMPATIBLE_VER)?;

        Ok(convert_pb_data_value_to_value(p.value)?)
    }

    fn to_pb(&self) -> Result<pb::DataValue, Incompatible> {
        convert_value_to_pb_data_value(&self)
    }
}

fn convert_value_to_pb_data_value(value: &dv::DataValue) -> Result<pb::DataValue, Incompatible> {
    match value {
        dv::DataValue::Null => Ok(pb::DataValue {
            ver: DATA_VER,
            value: Some(pb::data_value::Value::Null(pb::Empty {})),
        }),
        dv::DataValue::Boolean(v) => Ok(pb::DataValue {
            ver: DATA_VER,
            value: Some(pb::data_value::Value::BoolValue(*v)),
        }),
        dv::DataValue::Int64(v) => Ok(pb::DataValue {
            ver: DATA_VER,
            value: Some(pb::data_value::Value::I64Value(*v)),
        }),
        dv::DataValue::UInt64(v) => Ok(pb::DataValue {
            ver: DATA_VER,
            value: Some(pb::data_value::Value::U64Value(*v)),
        }),
        dv::DataValue::Float64(v) => Ok(pb::DataValue {
            ver: DATA_VER,
            value: Some(pb::data_value::Value::F64Value(*v)),
        }),
        dv::DataValue::String(v) => Ok(pb::DataValue {
            ver: DATA_VER,
            value: Some(pb::data_value::Value::StringValue(v.clone())),
        }),
        dv::DataValue::Array(v) => {
            let mut array = vec![];
            for v in v.iter() {
                array.push(convert_value_to_pb_data_value(v)?);
            }
            Ok(pb::DataValue {
                ver: DATA_VER,
                value: Some(pb::data_value::Value::ArrayValue(
                    pb::data_value::ArrayValue { values: array },
                )),
            })
        }
        dv::DataValue::Struct(v) => {
            let mut array = vec![];
            for v in v.iter() {
                array.push(convert_value_to_pb_data_value(v)?);
            }
            Ok(pb::DataValue {
                ver: DATA_VER,
                value: Some(pb::data_value::Value::StructValue(
                    pb::data_value::StructValue { values: array },
                )),
            })
        }
        dv::DataValue::Variant(v) => Ok(pb::DataValue {
            ver: DATA_VER,
            value: Some(pb::data_value::Value::VariantValue(
                dv::VariantValue::to_pb(v)?,
            )),
        }),
    }
}

fn convert_pb_data_value_to_value(
    pb_value: Option<pb::data_value::Value>,
) -> Result<dv::DataValue, Incompatible> {
    match pb_value {
        Some(pb::data_value::Value::Null(_)) => Ok(dv::DataValue::Null),
        Some(pb::data_value::Value::BoolValue(v)) => Ok(dv::DataValue::Boolean(v)),
        Some(pb::data_value::Value::I64Value(v)) => Ok(dv::DataValue::Int64(v)),
        Some(pb::data_value::Value::U64Value(v)) => Ok(dv::DataValue::UInt64(v)),
        Some(pb::data_value::Value::F64Value(v)) => Ok(dv::DataValue::Float64(v)),
        Some(pb::data_value::Value::StringValue(v)) => Ok(dv::DataValue::String(v)),
        Some(pb::data_value::Value::ArrayValue(v)) => {
            let mut array = vec![];
            for value in v.values.iter() {
                array.push(convert_pb_data_value_to_value(value.value.clone())?);
            }
            Ok(dv::DataValue::Array(array))
        }
        Some(pb::data_value::Value::StructValue(v)) => {
            let mut array = vec![];
            for value in v.values.iter() {
                array.push(convert_pb_data_value_to_value(value.value.clone())?);
            }
            Ok(dv::DataValue::Struct(array))
        }
        Some(pb::data_value::Value::VariantValue(v)) => {
            Ok(dv::DataValue::Variant(dv::VariantValue::from_pb(v)?))
        }

        None => Err(Incompatible {
            reason: "pb::data_value::Value can not be None".to_string(),
        }),
    }
}
