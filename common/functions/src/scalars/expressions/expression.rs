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

use crate::scalars::function2_factory::Factory2Creator;
use crate::scalars::function2_factory::Function2Description;
use crate::scalars::function2_factory::Function2Factory;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::CastFunction;

#[derive(Clone)]
pub struct ToCastFunction;

impl ToCastFunction {
    fn cast_function_creator(type_name: &'static str) -> Result<Function2Description> {
        let mut features = FunctionFeatures::default()
            .deterministic()
            .monotonicity()
            .num_arguments(1);

        if type_name.eq_ignore_ascii_case("Boolean") {
            features = features.bool_function();
        }

        let function_creator: Factory2Creator =
            Box::new(move |display_name| CastFunction::create(display_name, type_name));

        Ok(Function2Description::creator(function_creator).features(features))
    }

    pub fn register(factory: &mut Function2Factory) {
        let to_names = vec![
            "Null",
            "Boolean",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "Float32",
            "Float64",
            "Date16",
            "Date32",
            "String",
            "Date",
            "DateTime",
            "DateTime32",
            "DateTime64",
        ];

        for name in to_names {
            let to_name = format!("to{}", name);
            factory.register(&to_name, Self::cast_function_creator(name).unwrap());
        }
    }
}
