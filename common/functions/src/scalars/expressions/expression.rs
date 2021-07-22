// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
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
                Utf8,
                Date32,
                Date64,
                Binary
            }
            // aliases
            map.insert("tostring".into(), |display_name| {
                CastFunction::create(display_name.to_string(), DataType::Utf8)
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common_datavalues::DataType;

    #[test]
    fn it_works() {
        println!("{}", DataType::Utf8);
    }
}
