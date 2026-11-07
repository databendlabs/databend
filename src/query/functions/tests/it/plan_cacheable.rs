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

use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::PLAN_CACHEABLE_QUERY_TIME_FUNCTIONS;
use databend_common_functions::is_cacheable_function;

#[test]
fn test_query_time_functions_are_plan_cacheable() {
    // Every entry is a non-deterministic builtin: the list exists to exempt exactly these
    // functions from the non-deterministic exclusion.
    for name in PLAN_CACHEABLE_QUERY_TIME_FUNCTIONS {
        let name = name.into_inner();
        assert!(BUILTIN_FUNCTIONS.contains(name), "{name} is not a builtin");
        assert!(
            BUILTIN_FUNCTIONS
                .get_property(name)
                .unwrap()
                .non_deterministic,
            "{name} is not non-deterministic"
        );
        assert!(
            is_cacheable_function(name),
            "{name} should be plan cacheable"
        );
    }
}

#[test]
fn test_other_non_deterministic_functions_are_not_plan_cacheable() {
    for name in ["rand", "gen_random_uuid", "uuid"] {
        assert!(
            BUILTIN_FUNCTIONS
                .get_property(name)
                .unwrap()
                .non_deterministic
        );
        assert!(!is_cacheable_function(name), "{name} must stay uncacheable");
    }
}

#[test]
fn test_deterministic_builtin_is_plan_cacheable() {
    assert!(is_cacheable_function("subtract_hours"));
    assert!(is_cacheable_function("length"));
}
