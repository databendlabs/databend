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

use common_exception::ErrorCode;
use common_exception::Result;
use once_cell::sync::Lazy;

use crate::functions::admins::AdminFunction;
use crate::functions::systems::SystemFunction;
use crate::functions::Function;

pub type FactoryCreator = Box<dyn Fn() -> Result<Box<dyn Function>> + Send + Sync>;

#[derive(Clone)]
pub struct FunctionFeatures {
    // The number of arguments the function accepts.
    pub num_arguments: usize,
    // (1, 2) means we only accept [1, 2] arguments
    // None means it's not variadic function.
    pub variadic_arguments: Option<(usize, usize)>,
}

impl FunctionFeatures {
    pub fn default() -> FunctionFeatures {
        FunctionFeatures {
            num_arguments: 0,
            variadic_arguments: None,
        }
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
    descs: HashMap<String, FunctionDescription>,
}

static FUNCTION_FACTORY: Lazy<Arc<FunctionFactory>> = Lazy::new(|| {
    let mut factory = FunctionFactory::create();
    SystemFunction::register(&mut factory);
    AdminFunction::register(&mut factory);
    Arc::new(factory)
});

impl FunctionFactory {
    pub fn create() -> FunctionFactory {
        FunctionFactory {
            descs: Default::default(),
        }
    }

    pub fn instance() -> &'static FunctionFactory {
        FUNCTION_FACTORY.as_ref()
    }

    pub fn register(&mut self, name: &str, desc: FunctionDescription) {
        let descs = &mut self.descs;
        descs.insert(name.to_lowercase(), desc);
    }

    pub fn get_features(&self, name: impl AsRef<str>) -> Result<FunctionFeatures> {
        let origin_name = name.as_ref();
        let name = origin_name.to_lowercase();
        match self.descs.get(&name) {
            Some(desc) => Ok(desc.features.clone()),
            None => Err(ErrorCode::UnknownFunction(format!(
                "Unsupported Function: {}",
                origin_name
            ))),
        }
    }

    pub fn get(&self, name: impl AsRef<str>) -> Result<Box<dyn Function>> {
        let origin_name = name.as_ref();
        let name = origin_name.to_lowercase();
        match self.descs.get(&name) {
            Some(desc) => {
                let inner = (desc.function_creator)()?;
                Ok(inner)
            }
            None => Err(ErrorCode::UnknownFunction(format!(
                "Unsupported Function: {}",
                origin_name
            ))),
        }
    }
}
