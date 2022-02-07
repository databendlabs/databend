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

use common_datavalues::prelude::DataField as OldDataField;
use common_datavalues::DataTypeAndNullable;
use common_datavalues2::DataField;
use common_exception::ErrorCode;
use common_exception::Result;
use once_cell::sync::Lazy;

use super::Function2Convertor;
use super::Function2Factory;
use crate::scalars::DateFunction;
use crate::scalars::Function;
use crate::scalars::MathsFunction;
use crate::scalars::NullableFunction;
use crate::scalars::StringFunction;
use crate::scalars::UdfFunction;

pub type FactoryCreator = Box<dyn Fn(&str) -> Result<Box<dyn Function>> + Send + Sync>;

#[derive(Clone)]
pub struct FunctionFeatures {
    pub is_deterministic: bool,
    pub negative_function_name: Option<String>,
    pub is_bool_func: bool,
    pub is_context_func: bool,
    pub maybe_monotonic: bool,
    // The number of arguments the function accepts.
    pub num_arguments: usize,
    // (1, 2) means we only accept [1, 2] arguments
    // None means it's not variadic function.
    pub variadic_arguments: Option<(usize, usize)>,
}

impl FunctionFeatures {
    pub fn default() -> FunctionFeatures {
        FunctionFeatures {
            is_deterministic: false,
            negative_function_name: None,
            is_bool_func: false,
            is_context_func: false,
            maybe_monotonic: false,
            num_arguments: 0,
            variadic_arguments: None,
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

    pub fn context_function(mut self) -> FunctionFeatures {
        self.is_context_func = true;
        self
    }

    pub fn monotonicity(mut self) -> FunctionFeatures {
        self.maybe_monotonic = true;
        self
    }

    pub fn num_arguments(mut self, num_arguments: usize) -> FunctionFeatures {
        self.num_arguments = num_arguments;
        self
    }

    pub fn variadic_arguments(mut self, min: usize, max: usize) -> FunctionFeatures {
        self.variadic_arguments = Some((min, max));
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

static FUNCTION_FACTORY: Lazy<Arc<FunctionFactory>> = Lazy::new(|| {
    let mut function_factory = FunctionFactory::create();

    NullableFunction::register(&mut function_factory);
    StringFunction::register(&mut function_factory);
    UdfFunction::register(&mut function_factory);
    DateFunction::register(&mut function_factory);
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

    pub fn get(
        &self,
        name: impl AsRef<str>,
        args: &[DataTypeAndNullable],
    ) -> Result<Box<dyn Function>> {
        let origin_name = name.as_ref();
        let lowercase_name = origin_name.to_lowercase();

        // TODO: remove the codes, and create a new Function2Adapter for arithmetics.
        let new_args = args
            .iter()
            .map(|arg| OldDataField::new("xx", arg.data_type().clone(), arg.is_nullable()))
            .collect::<Vec<_>>();
        let mut types = vec![];
        let fs: Vec<DataField> = new_args.iter().map(|f| f.clone().into()).collect();
        for t in fs.iter() {
            types.push(t.data_type());
        }
        let factory2 = Function2Factory::instance();
        if let Ok(v) = factory2.get(origin_name, &types) {
            let adapter = Function2Convertor::create(v);
            return Ok(adapter);
        }

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

        let factory2 = Function2Factory::instance();
        if let Ok(v) = factory2.get_features(origin_name) {
            return Ok(v);
        }

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
        let function2_factory = Function2Factory::instance();
        let func_names = function2_factory.registered_names();
        self.case_insensitive_desc
            .keys()
            .chain(func_names.iter())
            .cloned()
            .collect::<Vec<_>>()
    }
}
