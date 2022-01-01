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
        let mut features = FunctionFeatures::default()
            .deterministic()
            .monotonicity()
            .num_arguments(1);
        if to_type.is_boolean() {
            features = features.bool_function();
        }

        let function_creator: FactoryCreator = Box::new(move |display_name| {
            CastFunction::create(display_name.to_string(), to_type.clone())
        });

        FunctionDescription::creator(function_creator).features(features)
    }

    pub fn register(factory: &mut FunctionFactory) {
        factory.register("toNull", Self::cast_function_creator(DataType::Null));
        // it is ok to pass in a non-nullable type, the function may enforce to nullable.
        factory.register(
            "toBoolean",
            Self::cast_function_creator(DataType::Boolean(false)),
        );
        factory.register(
            "toUInt8",
            Self::cast_function_creator(DataType::UInt8(false)),
        );
        factory.register(
            "toUInt16",
            Self::cast_function_creator(DataType::UInt16(false)),
        );
        factory.register(
            "toUInt32",
            Self::cast_function_creator(DataType::UInt32(false)),
        );
        factory.register(
            "toUInt64",
            Self::cast_function_creator(DataType::UInt64(false)),
        );
        factory.register("toInt8", Self::cast_function_creator(DataType::Int8(false)));
        factory.register(
            "toInt16",
            Self::cast_function_creator(DataType::Int16(false)),
        );
        factory.register(
            "toInt32",
            Self::cast_function_creator(DataType::Int32(false)),
        );
        factory.register(
            "toInt64",
            Self::cast_function_creator(DataType::Int64(false)),
        );
        factory.register(
            "toFloat32",
            Self::cast_function_creator(DataType::Float32(false)),
        );
        factory.register(
            "toFloat64",
            Self::cast_function_creator(DataType::Float64(false)),
        );
        factory.register(
            "toDate16",
            Self::cast_function_creator(DataType::Date16(false)),
        );
        factory.register(
            "toDate32",
            Self::cast_function_creator(DataType::Date32(false)),
        );
        factory.register(
            "toString",
            Self::cast_function_creator(DataType::String(false)),
        );

        // aliases
        factory.register(
            "toDate",
            Self::cast_function_creator(DataType::Date16(false)),
        );
        factory.register(
            "toDateTime",
            Self::cast_function_creator(DataType::DateTime32(false, None)),
        );
        factory.register(
            "toDateTime32",
            Self::cast_function_creator(DataType::DateTime32(false, None)),
        );
        // TODO support precision parameter
        factory.register(
            "toDateTime64",
            Self::cast_function_creator(DataType::DateTime64(false, 3, None)),
        );
    }
}
