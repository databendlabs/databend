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

use std::collections::HashMap;
use std::sync::Arc;

use common_datavalues::DataTypePtr;
use common_exception::ErrorCode;
use common_exception::Result;
use once_cell::sync::Lazy;

use super::function::Function;
use super::ArithmeticFunction;
use super::ComparisonFunction;
use super::ConditionalFunction;
use super::ContextFunction;
use super::FunctionAdapter;
use super::FunctionFeatures;
use super::HashesFunction;
use super::LogicFunction;
use super::MathsFunction;
use super::OtherFunction;
use super::SemiStructuredFunction;
use super::StringFunction;
use super::ToCastFunction;
use super::TupleClassFunction;
use crate::scalars::DateFunction;
use crate::scalars::UUIDFunction;

pub type FactoryCreator =
    Box<dyn Fn(&str, &[&DataTypePtr]) -> Result<Box<dyn Function>> + Send + Sync>;

pub struct FunctionDescription {
    pub(crate) features: FunctionFeatures,
    pub function_creator: FactoryCreator,
}

impl FunctionDescription {
    pub fn creator(creator: FactoryCreator) -> FunctionDescription {
        FunctionDescription {
            function_creator: creator,
            features: FunctionFeatures::default(),
        }
    }

    #[must_use]
    pub fn features(mut self, features: FunctionFeatures) -> FunctionDescription {
        self.features = features;
        self
    }
}

pub struct FunctionFactory {
    case_insensitive_desc: HashMap<String, FunctionDescription>,
}

static FUNCTION_FACTORY: Lazy<Arc<FunctionFactory>> = Lazy::new(|| {
    let mut function_factory = FunctionFactory::create();

    ArithmeticFunction::register(&mut function_factory);
    ToCastFunction::register(&mut function_factory);
    TupleClassFunction::register(&mut function_factory);
    ComparisonFunction::register(&mut function_factory);
    ContextFunction::register(&mut function_factory);
    SemiStructuredFunction::register(&mut function_factory);
    StringFunction::register(&mut function_factory);
    HashesFunction::register(&mut function_factory);
    ConditionalFunction::register(&mut function_factory);
    LogicFunction::register(&mut function_factory);
    DateFunction::register(&mut function_factory);
    OtherFunction::register(&mut function_factory);
    UUIDFunction::register(&mut function_factory);
    MathsFunction::register(&mut function_factory);

    Arc::new(function_factory)
});

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

    pub fn get(&self, name: impl AsRef<str>, args: &[&DataTypePtr]) -> Result<Box<dyn Function>> {
        let origin_name = name.as_ref();
        let lowercase_name = origin_name.to_lowercase();

        // TODO(Winter): we should write similar function names into error message if function name is not found.
        match self.case_insensitive_desc.get(&lowercase_name) {
            Some(desc) => FunctionAdapter::try_create(desc, origin_name, args),
            None => Err(ErrorCode::UnknownFunction(format!(
                "Unsupported Function: {}",
                origin_name
            ))),
        }
    }

    pub fn get_features(&self, name: impl AsRef<str>) -> Result<FunctionFeatures> {
        let origin_name = name.as_ref();
        let lowercase_name = origin_name.to_lowercase();

        // TODO(Winter): we should write similar function names into error message if function name is not found.
        match self.case_insensitive_desc.get(&lowercase_name) {
            Some(desc) => Ok(desc.features.clone()),
            None => Err(ErrorCode::UnknownFunction(format!(
                "Unsupported Function: {}",
                origin_name
            ))),
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

    pub fn registered_features(&self) -> Vec<FunctionFeatures> {
        self.case_insensitive_desc
            .values()
            .into_iter()
            .map(|v| &v.features)
            .cloned()
            .collect::<Vec<_>>()
    }
}
