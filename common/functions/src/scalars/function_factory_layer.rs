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



use super::FunctionDescription;
use super::FunctionFactory;
use super::TypedFunctionDescription;

pub struct FunctionFactoryLayer<'a> {
    factory: &'a mut FunctionFactory,
    category: Option<&'static str>,
}

impl<'a> FunctionFactoryLayer<'a> {
    pub fn with_layer(factory: &'a mut FunctionFactory) -> Self {
        Self {
            factory,
            category: None,
        }
    }
}

impl FunctionFactoryLayer<'_> {
    pub fn category(mut self, category: &'static str) -> Self {
        self.category = Some(category);
        self
    }

    pub fn register(&mut self, name: &str, desc: FunctionDescription) {
        let mut features = desc.features.clone();
        if let Some(category) = self.category {
            features.category = category;
        }
        let desc = desc.features(features);
        self.factory.register(name, desc);
    }

    pub fn register_typed(&mut self, name: &str, desc: TypedFunctionDescription) {
        let mut features = desc.features.clone();
        if let Some(category) = self.category {
            features.category = category;
        }
        let desc = desc.features(features);
        self.factory.register_typed(name, desc);
    }
}
