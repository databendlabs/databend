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

#![cfg(debug_assertions)]

use databend_common_base::base::GlobalInstance;

#[test]
fn test_try_get_unset_type() {
    #[derive(Clone)]
    struct Unset;

    // A type that was never set is absent, whether or not the registry is
    // initiated: `try_get` must not panic like `get` does.
    assert!(GlobalInstance::try_get::<Unset>().is_none());

    let thread_name = std::thread::current().name().unwrap().to_string();
    GlobalInstance::init_testing(&thread_name);
    assert!(GlobalInstance::try_get::<Unset>().is_none());
    GlobalInstance::set(Unset);
    assert!(GlobalInstance::try_get::<Unset>().is_some());
    GlobalInstance::drop_testing(&thread_name);
    assert!(GlobalInstance::try_get::<Unset>().is_none());
}
