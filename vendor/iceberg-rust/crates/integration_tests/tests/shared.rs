// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::{Arc, OnceLock};

use ctor::dtor;
use iceberg_integration_tests::{TestFixture, set_test_fixture};

pub mod shared_tests;

static DOCKER_CONTAINERS: OnceLock<Arc<TestFixture>> = OnceLock::new();

pub fn get_shared_containers() -> &'static Arc<TestFixture> {
    DOCKER_CONTAINERS.get_or_init(|| Arc::new(set_test_fixture("shared_tests")))
}

#[dtor]
fn shutdown() {
    if let Some(fixture) = DOCKER_CONTAINERS.get() {
        fixture._docker_compose.down()
    }
}
