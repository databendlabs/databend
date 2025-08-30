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

use databend_common_frozen_api::{frozen_api, FrozenAPI};

// Case 1: Simple 2-layer dependency
#[derive(FrozenAPI)]
struct Inner {
    value: u32,
}

#[frozen_api("34273a20")]
#[derive(FrozenAPI)]
struct Outer {
    inner: Inner,
    count: u64,
}

// Case 2: Complex multi-dependency
#[derive(FrozenAPI)]
struct Config {
    host: String,
    port: u16,
}

#[derive(FrozenAPI)]
struct Item {
    id: u64,
    name: String,
}

#[frozen_api("d4a2adea")]
#[derive(FrozenAPI)]
struct Service {
    config: Config,
    items: Vec<Item>,
    enabled: bool,
}

// Case 3: Three-layer dependency chain
#[derive(FrozenAPI)]
struct DatabaseConfig {
    host: String,
    port: u16,
    database: String,
}

#[derive(FrozenAPI)]
struct ServiceLayer {
    db: DatabaseConfig,
    timeout: u32,
}

#[frozen_api("b9dc60f9")]
#[derive(FrozenAPI)]
struct Application {
    service: ServiceLayer,
    version: String,
}

fn main() {}