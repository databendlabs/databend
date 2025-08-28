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

#![allow(dead_code)]

use databend_common_frozen_api::frozen_api;

#[test]
fn basic_struct() {
    #[frozen_api("78811ee5")]
    struct Basic {
        field1: i32,
        field2: String,
    }

    let _instance = Basic {
        field1: 42,
        field2: "test".to_string(),
    };
}

#[test]
fn tuple_struct() {
    #[frozen_api("12a671fa")]
    struct Tuple(i32, String, Vec<u8>);

    let _instance = Tuple(42, "test".to_string(), vec![1, 2, 3]);
}

#[test]
fn unit_struct() {
    #[frozen_api("926c27e7")]
    struct Unit;

    let _instance = Unit;
}

#[test]
fn generic_struct() {
    #[frozen_api("d31f73e0")]
    struct Generic<T, U> {
        field1: T,
        field2: Option<U>,
    }

    let _instance: Generic<i32, String> = Generic {
        field1: 42,
        field2: Some("test".to_string()),
    };
}

#[test]
fn visibility_modifiers() {
    #[frozen_api("f7993ee1")]
    pub struct Visibility {
        pub public_field: i32,
        private_field: String,
        pub(crate) crate_field: bool,
    }

    let _instance = Visibility {
        public_field: 42,
        private_field: "private".to_string(),
        crate_field: true,
    };
}

#[test]
fn field_name_changes_hash() {
    #[frozen_api("9282ee56")]
    struct FieldA {
        field1: i32,
    }

    #[frozen_api("15835ff4")]
    struct FieldB {
        field2: i32,
    }

    let _a = FieldA { field1: 1 };
    let _b = FieldB { field2: 1 };
}

#[test]
fn field_type_changes_hash() {
    #[frozen_api("5f6a91fa")]
    struct TypeI32 {
        field: i32,
    }

    #[frozen_api("c96ef359")]
    struct TypeU32 {
        field: u32,
    }

    let _i = TypeI32 { field: 1 };
    let _u = TypeU32 { field: 1 };
}

#[test]
fn struct_name_changes_hash() {
    #[frozen_api("f42aba5f")]
    struct Name1 {
        field: i32,
        data: String,
    }

    #[frozen_api("23dbac8b")]
    struct Name2 {
        field: i32,
        data: String,
    }

    let _n1 = Name1 {
        field: 1,
        data: "test".to_string(),
    };
    let _n2 = Name2 {
        field: 1,
        data: "test".to_string(),
    };
}

#[test]
fn nested_struct_changes_outer_hash() {
    struct Inner {
        value: u32,
    }

    #[frozen_api("1f238053")]
    struct Outer {
        inner: Inner,
        count: u64,
    }

    let _instance = Outer {
        inner: Inner { value: 42 },
        count: 100,
    };
}

#[test]
fn vec_nested() {
    struct Item {
        id: u64,
        name: String,
    }

    #[frozen_api("4743823b")]
    struct Container {
        items: Vec<Item>,
        capacity: usize,
    }

    let _instance = Container {
        items: vec![Item {
            id: 1,
            name: "test".to_string(),
        }],
        capacity: 10,
    };
}

#[test]
fn option_nested() {
    struct Config {
        host: String,
        port: u16,
    }

    #[frozen_api("96e3e1bb")]
    struct Service {
        config: Option<Config>,
        active: bool,
    }

    let _instance = Service {
        config: Some(Config {
            host: "localhost".to_string(),
            port: 8080,
        }),
        active: true,
    };
}

#[test]
fn deep_nesting() {
    struct L1 {
        data: String,
    }
    struct L2 {
        l1: L1,
        index: u32,
    }

    #[frozen_api("f67aa6b6")]
    struct L3 {
        l2: L2,
        metadata: Vec<u8>,
    }

    let _instance = L3 {
        l2: L2 {
            l1: L1 {
                data: "deep".to_string(),
            },
            index: 42,
        },
        metadata: vec![1, 2, 3],
    };
}

#[test]
fn generic_nested() {
    struct Wrapper<T> {
        value: T,
        timestamp: u64,
    }

    #[frozen_api("389aeddb")]
    struct Document {
        string_wrapper: Wrapper<String>,
        number_wrapper: Wrapper<i64>,
        version: u32,
    }

    let _instance = Document {
        string_wrapper: Wrapper {
            value: "text".to_string(),
            timestamp: 123,
        },
        number_wrapper: Wrapper {
            value: 42,
            timestamp: 123,
        },
        version: 1,
    };
}

#[test]
fn enum_nested() {
    enum Status {
        Active,
        Inactive,
    }
    struct Record {
        id: u64,
        status: Status,
    }

    #[frozen_api("662ff356")]
    struct Database {
        records: Vec<Record>,
        last_updated: u64,
    }

    let _instance = Database {
        records: vec![Record {
            id: 1,
            status: Status::Active,
        }],
        last_updated: 123456,
    };
}

#[test]
fn result_nested() {
    struct Error {
        code: u32,
        message: String,
    }

    #[frozen_api("e2c3ad29")]
    struct Response {
        result: Result<Vec<u8>, Error>,
        timestamp: u64,
    }

    let _instance = Response {
        result: Ok(vec![1, 2, 3]),
        timestamp: 123456,
    };
}

#[test]
fn tuple_with_nested() {
    struct Point {
        x: f64,
        y: f64,
    }

    #[frozen_api("20672b7d")]
    struct Line(Point, Point, String);

    let _instance = Line(
        Point { x: 0.0, y: 0.0 },
        Point { x: 1.0, y: 1.0 },
        "diagonal".to_string(),
    );
}

#[test]
fn real_world_table_meta() {
    struct Location {
        path: String,
        offset: u64,
    }

    #[frozen_api("6067529c")]
    struct TableMeta {
        size: u64,
        location: Location,
        hll: Option<Vec<u8>>,
        row_count: u64,
    }

    let _instance = TableMeta {
        size: 1024,
        location: Location {
            path: "/data/table.parquet".to_string(),
            offset: 0,
        },
        hll: None,
        row_count: 1000,
    };
}

#[test]
fn serialization_compat() {
    #[frozen_api("0bba0e09")]
    struct SerializableData {
        pub size: u64,
        pub location: (String, u64),
        pub hll: Option<Vec<u8>>,
        pub row_count: u64,
    }

    let _instance = SerializableData {
        size: 512,
        location: ("/stats/table1.hll".to_string(), 1024),
        hll: Some(vec![0x01, 0x02, 0x03]),
        row_count: 5000,
    };
}

#[test]
fn protocol_message() {
    struct Header {
        version: u32,
        timestamp: u64,
    }
    enum MessageType {
        Query(String),
        Response(Vec<u8>),
        Error { code: u32, message: String },
    }

    #[frozen_api("1b6c6acf")]
    struct ProtocolMessage {
        header: Header,
        payload: MessageType,
        checksum: u64,
    }

    let _instance = ProtocolMessage {
        header: Header {
            version: 1,
            timestamp: 123456,
        },
        payload: MessageType::Query("SELECT * FROM users".to_string()),
        checksum: 0xDEADBEEF,
    };
}

#[test]
fn version_evolution() {
    struct ConfigV1 {
        host: String,
        port: u16,
    }

    #[frozen_api("39236606")]
    struct ServiceV1 {
        config: ConfigV1,
        timeout: u32,
    }

    let _instance = ServiceV1 {
        config: ConfigV1 {
            host: "localhost".to_string(),
            port: 8080,
        },
        timeout: 5000,
    };
}

#[test]
fn complex_dependency_chain() {
    struct DatabaseConfig {
        host: String,
        port: u16,
    }
    struct ConnectionPool {
        config: DatabaseConfig,
        max_connections: u32,
    }
    struct QueryEngine {
        pool: ConnectionPool,
        cache_size: usize,
    }

    #[frozen_api("4d6a10fb")]
    struct DatabaseService {
        engine: QueryEngine,
        metrics_enabled: bool,
    }

    let _service = DatabaseService {
        engine: QueryEngine {
            pool: ConnectionPool {
                config: DatabaseConfig {
                    host: "localhost".to_string(),
                    port: 5432,
                },
                max_connections: 10,
            },
            cache_size: 1024,
        },
        metrics_enabled: true,
    };
}
