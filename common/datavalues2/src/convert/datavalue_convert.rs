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
// limitations under the License.pub use data_type::*;

use common_datavalues::prelude::DataValue as OldDataValue;

use crate::DataValue;

impl From<OldDataValue> for DataValue {
    fn from(old: OldDataValue) -> Self {
        if old.is_null() {
            return DataValue::Null;
        }

        let vv = old.clone();

        match old {
            OldDataValue::Null => DataValue::Null,
            OldDataValue::Boolean(v) => DataValue::Boolean(v.unwrap()),

            OldDataValue::Int8(_)
            | OldDataValue::Int16(_)
            | OldDataValue::Int32(_)
            | OldDataValue::Int64(_) => DataValue::Int64(vv.as_i64().unwrap()),

            OldDataValue::UInt8(_)
            | OldDataValue::UInt16(_)
            | OldDataValue::UInt32(_)
            | OldDataValue::UInt64(_) => DataValue::UInt64(vv.as_u64().unwrap()),

            OldDataValue::Float32(_) | OldDataValue::Float64(_) => {
                DataValue::Float64(vv.as_f64().unwrap())
            }
            OldDataValue::String(v) => DataValue::String(v.unwrap()),
            OldDataValue::List(v, _) => {
                let v = v.unwrap();
                let vs: Vec<DataValue> = v.iter().map(|i| i.clone().into()).collect();
                DataValue::Array(vs)
            }

            OldDataValue::Struct(v) => {
                let vs: Vec<DataValue> = v.iter().map(|i| i.clone().into()).collect();
                DataValue::Struct(vs)
            }
        }
    }
}
