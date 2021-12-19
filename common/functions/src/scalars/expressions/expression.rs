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
use common_datavalues::DataType;

use crate::scalars::function_factory::FactoryCreator;
use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFactory;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::CastFunction;

#[derive(Clone)]
pub struct ToCastFunction;

impl ToCastFunction {
    fn cast_function_creator(to_type: DataType) -> FunctionDescription {
        let mut features = FunctionFeatures::default().deterministic().monotonicity();
        if to_type == DataType::Boolean {
            features = features.bool_function();
        }

        let function_creator: FactoryCreator = Box::new(move |display_name| {
            CastFunction::create(display_name.to_string(), to_type.clone())
        });

        FunctionDescription::creator(function_creator).features(features)
    }

    pub fn register(factory: &mut FunctionFactory) {
        factory.register("toNull", Self::cast_function_creator(DataType::Null));
        factory.register("toBoolean", Self::cast_function_creator(DataType::Boolean));
        factory.register("toUInt8", Self::cast_function_creator(DataType::UInt8));
        factory.register("toUInt16", Self::cast_function_creator(DataType::UInt16));
        factory.register("toUInt32", Self::cast_function_creator(DataType::UInt32));
        factory.register("toUInt64", Self::cast_function_creator(DataType::UInt64));
        factory.register("toInt8", Self::cast_function_creator(DataType::Int8));
        factory.register("toInt16", Self::cast_function_creator(DataType::Int16));
        factory.register("toInt32", Self::cast_function_creator(DataType::Int32));
        factory.register("toInt64", Self::cast_function_creator(DataType::Int64));
        factory.register("toFloat32", Self::cast_function_creator(DataType::Float32));
        factory.register("toFloat64", Self::cast_function_creator(DataType::Float64));
        factory.register("toDate16", Self::cast_function_creator(DataType::Date16));
        factory.register("toDate32", Self::cast_function_creator(DataType::Date32));
        factory.register("toString", Self::cast_function_creator(DataType::String));

        // aliases
        factory.register("toDate", Self::cast_function_creator(DataType::Date16));
        factory.register(
            "toDateTime",
            Self::cast_function_creator(DataType::DateTime32(None)),
        );
        factory.register(
            "toDateTime32",
            Self::cast_function_creator(DataType::DateTime32(None)),
        );
        // TODO support precision parameter
        factory.register(
            "toDateTime64",
            Self::cast_function_creator(DataType::DateTime64(3, None)),
        );
    }
}
