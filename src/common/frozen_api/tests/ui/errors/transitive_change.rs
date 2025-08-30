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

// Bottom layer: C has been modified (new field added)
#[derive(FrozenAPI)]
struct C {
    a: i32,
    x: u8, // change: new field added - breaks hash chain
}

// Middle layer: B depends on C
#[derive(FrozenAPI)]
struct B {
    pub c: C,
}

// Top layer: A is frozen with old hash from before C was changed
// This should fail because C's change propagates up through B to A
#[frozen_api("567762e0")] // Old hash - should fail due to C's change
#[derive(FrozenAPI)]
struct A {
    pub b: B,
}

fn main() {
    // Try to access the hash - this forces compilation of the entire chain
    let _ = A::__FROZEN_API_HASH;
}