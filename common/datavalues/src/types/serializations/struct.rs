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

use common_exception::Result;

use crate::prelude::*;

pub struct StructSerializer {
    pub fields: Vec<DataField>,
}

impl TypeSerializer for StructSerializer {
    fn serialize_value(&self, value: &DataValue) -> Result<String> {
        todo!()
        // if let DataValue::Struct(vals) = value {
        //     let mut res = String::new();
        //     res.push('(');
        //     let mut first = true;
        //     vals.iter()
        //         .zip(self.fields.iter())
        //         .for_each(|(val, field)| {
        //             if !first {
        //                 res.push(',');
        //             }
        //             first = false;

        //             let data_type = field.data_type();
        //             let serializer = data_type.create_serializer();
        //             let s = serializer.serialize_value(val).unwrap();
        //             if matches!(
        //                 data_type,
        //                 DataType::String
        //                     | DataType::Date16
        //                     | DataType::Date32
        //                     | DataType::DateTime32(_)
        //             ) {
        //                 res.push_str(&format!("'{}'", s));
        //             } else {
        //                 res.push_str(&s);
        //             }
        //         });
        //     res.push(')');
        //     Ok(res)
        // } else {
        //     Err(ErrorCode::BadBytes("Incorrect Struct value"))
        // }
    }

    fn serialize_column(&self, _column: &DataColumn) -> Result<Vec<String>> {
        unimplemented!()
    }
}
