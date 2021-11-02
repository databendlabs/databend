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

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use lazy_static::lazy_static;

use crate::scalars::ArithmeticFunction;
use crate::scalars::ComparisonFunction;
use crate::scalars::ConditionalFunction;
use crate::scalars::DateFunction;
use crate::scalars::Function;
use crate::scalars::HashesFunction;
use crate::scalars::LogicFunction;
use crate::scalars::MathsFunction;
use crate::scalars::NullableFunction;
use crate::scalars::OtherFunction;
use crate::scalars::StringFunction;
use crate::scalars::ToCastFunction;
use crate::scalars::UdfFunction;

pub type FactoryCreator = Box<dyn Fn(&str) -> Result<Box<dyn Function>> + Send + Sync>;

#[derive(Clone)]
pub struct FunctionFeatures {
    pub is_deterministic: bool,
    pub negative_function_name: Option<String>,
    pub is_bool_func: bool,
}

impl FunctionFeatures {
    pub fn default() -> FunctionFeatures {
        FunctionFeatures {
            is_deterministic: false,
            negative_function_name: None,
            is_bool_func: false,
        }
    }

    pub fn deterministic(mut self) -> FunctionFeatures {
        self.is_deterministic = true;
        self
    }

    pub fn negative_function(mut self, negative_name: &str) -> FunctionFeatures {
        self.negative_function_name = Some(negative_name.to_string());
        self
    }

    pub fn bool_function(mut self) -> FunctionFeatures {
        self.is_bool_func = true;
        self
    }
}

pub struct FunctionDescription {
    features: FunctionFeatures,
    function_creator: FactoryCreator,
    // TODO(Winter): function document, this is very interesting.
    // TODO(Winter): We can support the SHOW FUNCTION DOCUMENT `function_name` or MAN FUNCTION `function_name` query syntax.
}

impl FunctionDescription {
    pub fn creator(creator: FactoryCreator) -> FunctionDescription {
        FunctionDescription {
            function_creator: creator,
            features: FunctionFeatures::default(),
        }
    }

    pub fn features(mut self, features: FunctionFeatures) -> FunctionDescription {
        self.features = features;
        self
    }
}

pub struct FunctionFactory {
    case_insensitive_desc: HashMap<String, FunctionDescription>,
}

lazy_static! {
    static ref FUNCTION_FACTORY: Arc<FunctionFactory> = {
        let mut function_factory = FunctionFactory::create();
        ArithmeticFunction::register(&mut function_factory);
        ComparisonFunction::register(&mut function_factory);
        LogicFunction::register(&mut function_factory);
        NullableFunction::register(&mut function_factory);
        StringFunction::register(&mut function_factory);
        UdfFunction::register(&mut function_factory);
        HashesFunction::register(&mut function_factory);
        ToCastFunction::register(&mut function_factory);
        ConditionalFunction::register(&mut function_factory);
        DateFunction::register(&mut function_factory);
        OtherFunction::register(&mut function_factory);
        MathsFunction::register(&mut function_factory);

        Arc::new(function_factory)
    };
}

impl FunctionFactory {
    pub(in crate::scalars::function_factory) fn create() -> FunctionFactory {
        FunctionFactory {
            case_insensitive_desc: Default::default(),
        }
    }

    pub fn instance() -> &'static FunctionFactory {
        FUNCTION_FACTORY.as_ref()
    }

    pub fn register(&mut self, name: &str, desc: FunctionDescription) {
        let case_insensitive_desc = &mut self.case_insensitive_desc;
        case_insensitive_desc.insert(name.to_lowercase(), desc);
    }

    pub fn get(&self, name: impl AsRef<str>) -> Result<Box<dyn Function>> {
        let origin_name = name.as_ref();
        let lowercase_name = origin_name.to_lowercase();
        match self.case_insensitive_desc.get(&lowercase_name) {
            // TODO(Winter): we should write similar function names into error message if function name is not found.
            None => Err(ErrorCode::UnknownFunction(format!(
                "Unsupported Function: {}",
                origin_name
            ))),
            Some(desc) => (desc.function_creator)(origin_name),
        }
    }

    pub fn get_features(&self, name: impl AsRef<str>) -> Result<FunctionFeatures> {
        let origin_name = name.as_ref();
        let lowercase_name = origin_name.to_lowercase();
        match self.case_insensitive_desc.get(&lowercase_name) {
            // TODO(Winter): we should write similar function names into error message if function name is not found.
            None => Err(ErrorCode::UnknownFunction(format!(
                "Unsupported Function: {}",
                origin_name
            ))),
            Some(desc) => Ok(desc.features.clone()),
        }
    }

    pub fn check(&self, name: impl AsRef<str>) -> bool {
        let origin_name = name.as_ref();
        let lowercase_name = origin_name.to_lowercase();
        self.case_insensitive_desc.contains_key(&lowercase_name)
    }

    pub fn registered_names(&self) -> Vec<String> {
        self.case_insensitive_desc
            .keys()
            .cloned()
            .collect::<Vec<_>>()
    }
}
