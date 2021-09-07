// Copyright 2020 Datafuse Labs.
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
use common_datavalues::DataType;
use common_exception::Result;

use crate::scalars::CastFunction;
use crate::scalars::FactoryFuncRef;

#[derive(Clone)]
pub struct ToCastFunction;

impl ToCastFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();

        macro_rules! register_cast_funcs {
            ( $($name:ident), *) => {{
               $(
                let name = format!("to{}", DataType::$name);
                map.insert(name.into(), |display_name| {
                    CastFunction::create(display_name.to_string(), DataType::$name)
                });
               )*
            }};
        }

        {
            register_cast_funcs! {
                Null,
                Boolean,
                UInt8,
                UInt16,
                UInt32,
                UInt64,
                Int8,
                Int16,
                Int32,
                Int64,
                Float32,
                Float64,
                Date16,
                Date32,
                String
            }
            // aliases
            map.insert("toDate".into(), |display_name| {
                CastFunction::create(display_name.to_string(), DataType::Date16)
            });
            map.insert("toDateTime".into(), |display_name| {
                CastFunction::create(display_name.to_string(), DataType::DateTime32(None))
            });
            map.insert("toDateTime32".into(), |display_name| {
                CastFunction::create(display_name.to_string(), DataType::DateTime32(None))
            });
        }

        Ok(())
    }
}
