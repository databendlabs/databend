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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use indexmap::IndexMap;
use lazy_static::lazy_static;
use unicase::UniCase;

use crate::scalars::ArithmeticFunction;
use crate::scalars::ComparisonFunction;
use crate::scalars::ConditionalFunction;
use crate::scalars::DateFunction;
use crate::scalars::Function;
use crate::scalars::HashesFunction;
use crate::scalars::LogicFunction;
use crate::scalars::StringFunction;
use crate::scalars::ToCastFunction;
use crate::scalars::UdfFunction;

pub struct FunctionFactory;
pub type FactoryFunc = fn(name: &str) -> Result<Box<dyn Function>>;

type Key = UniCase<String>;
pub type FactoryFuncRef = Arc<RwLock<IndexMap<Key, FactoryFunc>>>;

lazy_static! {
    static ref FACTORY: FactoryFuncRef = {
        let map: FactoryFuncRef = Arc::new(RwLock::new(IndexMap::new()));
        ArithmeticFunction::register(map.clone()).unwrap();
        ComparisonFunction::register(map.clone()).unwrap();
        LogicFunction::register(map.clone()).unwrap();
        StringFunction::register(map.clone()).unwrap();
        UdfFunction::register(map.clone()).unwrap();
        HashesFunction::register(map.clone()).unwrap();
        ToCastFunction::register(map.clone()).unwrap();
        ConditionalFunction::register(map.clone()).unwrap();
        DateFunction::register(map.clone()).unwrap();

        map
    };
}

impl FunctionFactory {
    pub fn get(name: impl AsRef<str>) -> Result<Box<dyn Function>> {
        let name = name.as_ref();
        let map = FACTORY.read();
        let key: Key = name.into();
        let creator = map
            .get(&key)
            .ok_or_else(|| ErrorCode::UnknownFunction(format!("Unsupported Function: {}", name)))?;
        (creator)(name)
    }

    pub fn check(name: impl AsRef<str>) -> bool {
        let name = name.as_ref();
        let key: Key = name.into();
        let map = FACTORY.read();
        map.contains_key(&key)
    }

    pub fn registered_names() -> Vec<String> {
        let map = FACTORY.read();
        map.keys().into_iter().map(|x| x.to_string()).collect()
    }
}
