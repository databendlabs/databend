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

use crate::scalars::function_factory::Factory2Creator;
use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFactory;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::CastFunction;

#[derive(Clone)]
pub struct ToCastFunction;

impl ToCastFunction {
    fn cast_function_creator(type_name: &'static str) -> Result<FunctionDescription> {
        let mut features = FunctionFeatures::default().deterministic().monotonicity();

        // TODO(zhyass): complete DateTime, e.g. toDateTime64(1640019661000, 3, 'UTC').
        features = match type_name {
            "Boolean" => features.num_arguments(1).bool_function(),
            "DateTime" | "DateTime32" => features.variadic_arguments(1, 2),
            "DateTime64" => features.variadic_arguments(1, 3),
            _ => features.num_arguments(1),
        };

        let function_creator: Factory2Creator =
            Box::new(move |display_name| CastFunction::create(display_name, type_name));

        Ok(FunctionDescription::creator(function_creator).features(features))
    }

    pub fn register(factory: &mut FunctionFactory) {
        let names = vec![
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

        for name in names {
            let to_name = format!("to{}", name);
            factory.register(&to_name, Self::cast_function_creator(name).unwrap());
        }
    }
}
