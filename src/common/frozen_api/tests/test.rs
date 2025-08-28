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
    #[frozen_api("376b1848")]
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
    #[frozen_api("f25d36ba")]
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
    #[frozen_api("cac2134b")]
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
    #[frozen_api("c5c50a84")]
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
    #[frozen_api("175adb33")]
    struct FieldA {
        field1: i32,
    }

    #[frozen_api("1a403660")]
    struct FieldB {
        field2: i32,
    }

    let _a = FieldA { field1: 1 };
    let _b = FieldB { field2: 1 };
}

#[test]
fn field_type_changes_hash() {
    #[frozen_api("add6df26")]
    struct TypeI32 {
        field: i32,
    }

    #[frozen_api("3dde7f4f")]
    struct TypeU32 {
        field: u32,
    }

    let _i = TypeI32 { field: 1 };
    let _u = TypeU32 { field: 1 };
}

#[test]
fn struct_name_changes_hash() {
    #[frozen_api("30648509")]
    struct Name1 {
        field: i32,
        data: String,
    }

    #[frozen_api("ae81ce78")]
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

    #[frozen_api("814d310b")]
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

    #[frozen_api("1ae0afae")]
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

    #[frozen_api("bceeab5b")]
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

    #[frozen_api("9843b79c")]
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

    #[frozen_api("71fa3797")]
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

    #[frozen_api("ade91003")]
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

    #[frozen_api("7d6934c1")]
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

    #[frozen_api("807b305b")]
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

    #[frozen_api("30223d0a")]
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
    #[frozen_api("194415a8")]
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

    #[frozen_api("ec367338")]
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

    #[frozen_api("fa38edf9")]
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

    #[frozen_api("05253420")]
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

// =============================================================================
// Indirect Dependency Detection Tests
// =============================================================================

#[test]
fn indirect_dependency_protected_nested_types() {
    // When both outer and inner types are protected with frozen_api,
    // changes to inner type will be detected at compile time
    #[frozen_api("9f97212c")]
    struct InnerTypeProtected {
        value: i32,
    }

    #[frozen_api("cbe37faf")]
    struct OuterTypeProtected {
        inner: InnerTypeProtected,
        name: String,
    }

    let _instance = OuterTypeProtected {
        inner: InnerTypeProtected { value: 42 },
        name: "test".to_string(),
    };
}

#[test]
fn indirect_dependency_unprotected_inner_limitation() {
    // LIMITATION: When inner type is not protected with frozen_api,
    // changes to inner type structure will NOT be detected.
    // The outer type hash only includes the type NAME "InnerTypeUnprotected",
    // not its actual field definitions.
    struct InnerTypeUnprotected {
        value: i32,
        // Adding fields here would NOT trigger outer type hash change
        // This demonstrates the current limitation of the implementation
    }

    #[frozen_api("11656fca")]
    struct OuterTypeWithUnprotected {
        inner: InnerTypeUnprotected,
        name: String,
    }

    let _instance = OuterTypeWithUnprotected {
        inner: InnerTypeUnprotected { value: 42 },
        name: "test".to_string(),
    };
}

#[test]
fn indirect_dependency_multilevel_protection() {
    // Demonstrate multiple levels of protection
    #[frozen_api("5f625c26")]
    struct Level1 {
        data: String,
    }

    #[frozen_api("bf96b58b")]
    struct Level2 {
        level1: Level1,
        index: u32,
    }

    #[frozen_api("37d113d0")]
    struct Level3 {
        level2: Level2,
        metadata: Vec<u8>,
    }

    let _instance = Level3 {
        level2: Level2 {
            level1: Level1 {
                data: "deep".to_string(),
            },
            index: 42,
        },
        metadata: vec![1, 2, 3],
    };
}

#[test]
fn indirect_dependency_mixed_protection() {
    // Mixed scenario: some types protected, some not
    struct UnprotectedConfig {
        host: String,
        port: u16,
    }

    #[frozen_api("853ba070")]
    struct ProtectedConnection {
        timeout: u32,
        retries: u8,
    }

    #[frozen_api("7b40377e")]
    struct ServiceWithMixedDeps {
        config: UnprotectedConfig,       // Changes here won't be detected
        connection: ProtectedConnection, // Changes here will be detected
        name: String,
    }

    let _instance = ServiceWithMixedDeps {
        config: UnprotectedConfig {
            host: "localhost".to_string(),
            port: 8080,
        },
        connection: ProtectedConnection {
            timeout: 5000,
            retries: 3,
        },
        name: "test-service".to_string(),
    };
}

#[test]
fn indirect_dependency_generic_types() {
    // Test how generic types affect hashing
    #[frozen_api("8b7f25fa")]
    struct GenericContainer<T> {
        data: Vec<T>,
        count: usize,
    }

    #[frozen_api("2a116712")]
    struct ServiceWithGenerics {
        string_container: GenericContainer<String>,
        number_container: GenericContainer<i32>,
    }

    let _instance = ServiceWithGenerics {
        string_container: GenericContainer {
            data: vec!["test".to_string()],
            count: 1,
        },
        number_container: GenericContainer {
            data: vec![42],
            count: 1,
        },
    };
}

#[test]
fn indirect_dependency_enum_protection() {
    // Demonstrate enum protection (though frozen_api is primarily for structs)
    struct Config {
        value: String,
    }

    #[frozen_api("2cbcd7af")]
    struct ServiceWithEnum {
        config: Config,
        status: ServiceStatus,
    }

    enum ServiceStatus {
        Running,
        Stopped,
        Error(String),
    }

    let _instance = ServiceWithEnum {
        config: Config {
            value: "test".to_string(),
        },
        status: ServiceStatus::Running,
    };
}

// =============================================================================
// Deep Destructive Change Detection Test
// =============================================================================

#[test]
fn deep_destructive_change_detection_demo() {
    // This test demonstrates what happens when nested protected types change
    // and how the protection cascades through dependency chains

    // Level 1: Core data type (simulating critical storage format)
    #[frozen_api("20f3d311")]
    struct CoreData {
        version: u32,
        payload: Vec<u8>,
    }

    // Level 2: Service configuration (depends on CoreData)
    #[frozen_api("2dc8c3bf")]
    struct ServiceConfig {
        core: CoreData,
        timeout: u64,
        retries: u8,
    }

    // Level 3: Application state (depends on ServiceConfig)
    #[frozen_api("2090b83c")]
    struct AppState {
        config: ServiceConfig,
        active_connections: u32,
        last_heartbeat: u64,
    }

    let _app = AppState {
        config: ServiceConfig {
            core: CoreData {
                version: 1,
                payload: vec![0x01, 0x02, 0x03],
            },
            timeout: 5000,
            retries: 3,
        },
        active_connections: 10,
        last_heartbeat: 123456789,
    };

    // DEMONSTRATION SCENARIOS:
    //
    // Scenario 1: If CoreData changes (e.g., add `checksum: u32` field):
    // - CoreData hash changes: "dummy" -> "new_hash_1"
    // - ServiceConfig hash changes: "dummy" -> "new_hash_2" (because it contains CoreData)
    // - AppState hash changes: "dummy" -> "new_hash_3" (because it contains ServiceConfig)
    // - Requires updating ALL THREE hash values to allow the change
    //
    // Scenario 2: If only ServiceConfig changes (e.g., add `max_concurrent: u32`):
    // - CoreData hash stays same (no change)
    // - ServiceConfig hash changes: "dummy" -> "new_hash_4"
    // - AppState hash changes: "dummy" -> "new_hash_5" (indirect dependency)
    // - Requires updating TWO hash values
    //
    // Scenario 3: If only AppState changes (e.g., add `startup_time: u64`):
    // - CoreData and ServiceConfig hashes stay same
    // - AppState hash changes: "dummy" -> "new_hash_6"
    // - Requires updating ONE hash value
    //
    // This demonstrates CASCADING PROTECTION: changes to lower-level types
    // force explicit acknowledgment at ALL dependent levels.
}

#[test]
fn deep_unprotected_vs_protected_comparison() {
    // Compare behavior with and without protection to show the difference

    // UNPROTECTED nested type - changes won't be detected by outer
    struct UnprotectedInner {
        data: String,
        // Adding fields here won't affect OuterWithUnprotected hash
    }

    #[frozen_api("d80fafe6")]
    struct OuterWithUnprotected {
        inner: UnprotectedInner, // Only type name "UnprotectedInner" affects hash
        metadata: u64,
    }

    // PROTECTED nested type - changes WILL be detected by outer
    #[frozen_api("575d4a98")]
    struct ProtectedInner {
        data: String,
        // Adding fields here WILL change both hashes
    }

    #[frozen_api("1ddd7b53")]
    struct OuterWithProtected {
        inner: ProtectedInner, // Full structure definition affects hash
        metadata: u64,
    }

    let _unprotected_example = OuterWithUnprotected {
        inner: UnprotectedInner {
            data: "test".to_string(),
        },
        metadata: 12345,
    };

    let _protected_example = OuterWithProtected {
        inner: ProtectedInner {
            data: "test".to_string(),
        },
        metadata: 12345,
    };

    // KEY INSIGHT: The hash computation difference
    // UnprotectedInner contributes: "UnprotectedInner" (just the name)
    // ProtectedInner contributes: Full token stream of the struct definition
    //
    // This means:
    // - Changes to UnprotectedInner fields = OuterWithUnprotected hash unchanged (BAD)
    // - Changes to ProtectedInner fields = OuterWithProtected hash changed (GOOD)
}
