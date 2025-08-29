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

use databend_common_frozen_api::frozen_api;

#[test]
fn test_simple_struct_protection() {
    #[frozen_api("e0d8bb1f")]
    #[allow(dead_code)]
    struct SimpleData {
        id: u64,
        name: String,
    }

    let _data = SimpleData {
        id: 1,
        name: "test".to_string(),
    };
}

#[test]
fn test_tuple_struct_protection() {
    #[frozen_api("e43f61aa")]
    #[allow(dead_code)]
    struct Point(f32, f32, f32);

    let _point = Point(1.0, 2.0, 3.0);
}

#[test]
fn test_unit_struct_protection() {
    #[frozen_api("cfc0cb65")]
    #[allow(dead_code)]
    struct Marker;

    let _marker = Marker;
}

#[test]
fn test_nested_dependency_protection() {
    #[frozen_api("12c1712d")]
    #[allow(dead_code)]
    struct Inner {
        value: i32,
    }

    #[frozen_api("c2eb818d")]
    #[allow(dead_code)]
    struct Container {
        inner: Inner,
        data: Vec<String>,
    }

    let _container = Container {
        inner: Inner { value: 42 },
        data: vec!["test".to_string()],
    };
}

#[test]
fn test_complex_dependency_chain() {
    #[frozen_api("c7068d36")]
    #[allow(dead_code)]
    struct Database {
        connection: String,
        timeout: u64,
    }

    #[frozen_api("d0dc919b")]
    #[allow(dead_code)]
    struct Cache {
        size: usize,
        ttl: u64,
    }

    #[frozen_api("3e66fd99")]
    #[allow(dead_code)]
    struct Service {
        db: Database,
        cache: Cache,
        name: String,
    }

    #[frozen_api("1281030d")]
    #[allow(dead_code)]
    struct Application {
        service: Service,
        version: String,
        debug: bool,
    }

    let _app = Application {
        service: Service {
            db: Database {
                connection: "localhost".to_string(),
                timeout: 5000,
            },
            cache: Cache {
                size: 1000,
                ttl: 300,
            },
            name: "api".to_string(),
        },
        version: "1.0.0".to_string(),
        debug: false,
    };
}
