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

// Scenario: Bottom layer C was modified, and we properly update all hashes

// Layer 1: Modified C with new field
#[derive(FrozenAPI)]
struct C {
    a: i32,
    x: u8, // New field - this changes C's hash
}

// Layer 2: B depends on C - hash automatically includes C's new structure
#[derive(FrozenAPI)]
struct B {
    pub c: C,
}

// Layer 3: A with UPDATED hash that reflects C's change
#[frozen_api("eb7c00b7")] // Correct hash that includes C's modification
#[derive(FrozenAPI)]
struct A {
    pub b: B,
}

fn main() {
    // This demonstrates proper handling of transitive changes
    let _ = A::__FROZEN_API_HASH;
}