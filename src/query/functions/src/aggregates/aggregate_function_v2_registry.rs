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

use std::sync::LazyLock;

use aggregate_function_v2_impl::adaptors_v2 as v2;

use super::aggregate_function_v2_impl;

static REGISTRY: LazyLock<v2::AggregateFunctionRegistry> = LazyLock::new(|| {
    let mut registry = v2::AggregateFunctionRegistry::empty();
    aggregate_function_v2_impl::register_functions(&mut registry);
    registry
});

pub struct AggregateFunctionV2Registry;

impl AggregateFunctionV2Registry {
    pub fn instance() -> &'static v2::AggregateFunctionRegistry {
        &REGISTRY
    }
}

pub fn instance() -> &'static v2::AggregateFunctionRegistry {
    AggregateFunctionV2Registry::instance()
}
