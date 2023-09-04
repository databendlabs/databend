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

use std::sync::Mutex;
use std::sync::MutexGuard;

use lazy_static::lazy_static;
use prometheus_client::registry::Registry;

lazy_static! {
    pub static ref REGISTRY: Mutex<Registry> = Mutex::new(Registry::default());
}

pub fn load_global_prometheus_registry() -> MutexGuard<'static, Registry> {
    REGISTRY.lock().unwrap()
}
