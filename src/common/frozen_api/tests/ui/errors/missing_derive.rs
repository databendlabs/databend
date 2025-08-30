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

// Layer 1: Has FrozenAPI derive - OK
#[derive(FrozenAPI)]
struct Config {
    host: String,
    port: u16,
}

// Layer 2: Missing FrozenAPI derive - BROKEN LINK
struct BadService {
    config: Config,  // Config is OK
    timeout: u32,
}

// Layer 3: Should fail because BadService in the chain lacks FrozenAPI
#[frozen_api("dummy")]
#[derive(FrozenAPI)]
struct Application {
    service: BadService,  // ERROR: BadService missing __FROZEN_API_HASH
    version: String,
}

fn main() {}