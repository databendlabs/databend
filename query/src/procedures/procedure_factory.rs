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

use crate::procedures::admins::AdminProcedure;
use crate::procedures::systems::SystemProcedure;
use crate::procedures::Procedure;

pub type Factory2Creator = Box<dyn Fn() -> Result<Box<dyn Procedure>> + Send + Sync>;

#[derive(Clone)]
pub struct ProcedureFeatures {
    // The number of arguments the function accepts.
    pub num_arguments: usize,
    // (1, 2) means we only accept [1, 2] arguments
    // None means it's not variadic function.
    pub variadic_arguments: Option<(usize, usize)>,
}

impl ProcedureFeatures {
    pub fn default() -> ProcedureFeatures {
        ProcedureFeatures {
            num_arguments: 0,
            variadic_arguments: None,
        }
    }

    pub fn num_arguments(mut self, num_arguments: usize) -> ProcedureFeatures {
        self.num_arguments = num_arguments;
        self
    }

    pub fn variadic_arguments(mut self, min: usize, max: usize) -> ProcedureFeatures {
        self.variadic_arguments = Some((min, max));
        self
    }
}

pub struct ProcedureDescription {
    features: ProcedureFeatures,
    procedure_creator: Factory2Creator,
}

impl ProcedureDescription {
    pub fn creator(creator: Factory2Creator) -> ProcedureDescription {
        ProcedureDescription {
            procedure_creator: creator,
            features: ProcedureFeatures::default(),
        }
    }

    #[must_use]
    pub fn features(mut self, features: ProcedureFeatures) -> ProcedureDescription {
        self.features = features;
        self
    }
}

pub struct ProcedureFactory {
    descs: HashMap<String, ProcedureDescription>,
}

static FUNCTION_FACTORY: Lazy<Arc<ProcedureFactory>> = Lazy::new(|| {
    let mut factory = ProcedureFactory::create();
    SystemProcedure::register(&mut factory);
    AdminProcedure::register(&mut factory);
    Arc::new(factory)
});

impl ProcedureFactory {
    pub fn create() -> ProcedureFactory {
        ProcedureFactory {
            descs: Default::default(),
        }
    }

    pub fn instance() -> &'static ProcedureFactory {
        FUNCTION_FACTORY.as_ref()
    }

    pub fn register(&mut self, name: &str, desc: ProcedureDescription) {
        let descs = &mut self.descs;
        descs.insert(name.to_lowercase(), desc);
    }

    pub fn get_features(&self, name: impl AsRef<str>) -> Result<ProcedureFeatures> {
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

    pub fn get(&self, name: impl AsRef<str>) -> Result<Box<dyn Procedure>> {
        let origin_name = name.as_ref();
        let name = origin_name.to_lowercase();
        match self.descs.get(&name) {
            Some(desc) => {
                let inner = (desc.procedure_creator)()?;
                Ok(inner)
            }
            None => Err(ErrorCode::UnknownFunction(format!(
                "Unsupported Function: {}",
                origin_name
            ))),
        }
    }
}
