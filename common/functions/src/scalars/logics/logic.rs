// Copyright 2022 Datafuse Labs.
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

use super::xor::LogicXorFunction;
use super::LogicAndFunction;
use super::LogicNotFunction;
use super::LogicOrFunction;
use crate::scalars::Function2Factory;

#[derive(Clone)]
pub struct LogicFunction;

impl LogicFunction {
    pub fn register(factory: &mut Function2Factory) {
        factory.register("and", LogicAndFunction::desc());
        factory.register("or", LogicOrFunction::desc());
        factory.register("not", LogicNotFunction::desc());
        factory.register("xor", LogicXorFunction::desc());
    }
}
