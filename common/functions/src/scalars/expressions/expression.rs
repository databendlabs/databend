use common_datavalues::TypeFactory;
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

use crate::scalars::function_factory::FactoryCreator;
use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFactory;
use crate::scalars::CastFunction;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct ToCastFunction;

impl ToCastFunction {
    fn cast_function_creator(type_name: &'static str) -> Result<FunctionDescription> {
        let mut features = FunctionFeatures::default()
            .deterministic()
            .monotonicity()
            .disable_passthrough_null();

        // TODO(zhyass): complete DateTime
        features = match type_name {
            "Boolean" => features.num_arguments(1).bool_function(),
            "Timestamp" | "DateTime" => features.variadic_arguments(1, 3),
            _ => features.num_arguments(1),
        };

        let function_creator: FactoryCreator = Box::new(move |display_name, args| {
            CastFunction::create(display_name, type_name, args[0].clone())
        });

        Ok(FunctionDescription::creator(function_creator).features(features))
    }

    pub fn register(factory: &mut FunctionFactory) {
        let type_factory = TypeFactory::instance();

        for name in type_factory.register_names() {
            let to_name = format!("to_{}", name.to_lowercase());
            factory.register(&to_name, Self::cast_function_creator(name).unwrap());
        }
    }
}
