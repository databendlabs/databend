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

use super::uuid_creator::UUIDZeroFunction;
use super::uuid_creator::UUIDv4Function;
use super::uuid_verifier::UUIDIsEmptyFunction;
use super::uuid_verifier::UUIDIsNotEmptyFunction;
use crate::scalars::FunctionFactory;

pub struct UUIDFunction;

impl UUIDFunction {
    pub fn register2(factory: &mut FunctionFactory) {
        factory.register("generateUUIDv4", UUIDv4Function::desc());
        factory.register("zeroUUID", UUIDZeroFunction::desc());
        factory.register("isemptyUUID", UUIDIsEmptyFunction::desc());
        factory.register("isnotemptyUUID", UUIDIsNotEmptyFunction::desc());
    }
}
