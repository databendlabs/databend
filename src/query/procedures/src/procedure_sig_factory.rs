// Copyright 2021 Datafuse Labs
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

use crate::admins::AdminProcedureSig;
use crate::systems::SystemProcedureSig;
use crate::ProcedureSignature;

pub type Factory2Creator = Box<dyn Fn() -> Result<Box<dyn ProcedureSignature>> + Send + Sync>;

pub struct ProcedureSigFactory {
    creators: HashMap<String, Factory2Creator>,
}

static FUNCTION_SIGNATURE_FACTORY: Lazy<Arc<ProcedureSigFactory>> = Lazy::new(|| {
    let mut factory = ProcedureSigFactory::create();
    SystemProcedureSig::register(&mut factory);
    AdminProcedureSig::register(&mut factory);
    Arc::new(factory)
});

impl ProcedureSigFactory {
    pub fn create() -> ProcedureSigFactory {
        ProcedureSigFactory {
            creators: Default::default(),
        }
    }

    pub fn instance() -> &'static ProcedureSigFactory {
        FUNCTION_SIGNATURE_FACTORY.as_ref()
    }

    pub fn register(&mut self, name: &str, creator: Factory2Creator) {
        let creators = &mut self.creators;
        creators.insert(name.to_lowercase(), creator);
    }

    pub fn get(&self, name: impl AsRef<str>) -> Result<Box<dyn ProcedureSignature>> {
        let origin_name = name.as_ref();
        let name = origin_name.to_lowercase();
        match self.creators.get(&name) {
            Some(creator) => {
                let inner = creator()?;
                Ok(inner)
            }
            None => Err(ErrorCode::UnknownFunction(format!(
                "Unsupported Function: {}",
                origin_name
            ))),
        }
    }
}
