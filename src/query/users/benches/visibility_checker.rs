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

use databend_common_meta_app::principal::OwnershipInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserInfo;
use databend_common_users::GrantObjectVisibilityChecker;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;

fn main() {
    divan::main();
}

/// Generate a ID (timestamp + node + sequence)
fn generate_id(seed: u64) -> u64 {
    let timestamp = (seed / 1000) << 22;
    let node = (seed % 100) << 12;
    let sequence = seed % 4096;
    timestamp | node | sequence
}

/// Create realistic test data with random distribution
/// Creates more realistic test data: random number of tables per database
fn create_realistic_test_data(
    total_tables: usize,
    num_databases: usize,
    num_catalogs: usize,
) -> (UserInfo, Vec<RoleInfo>, Vec<OwnershipInfo>) {
    let user = UserInfo::new_no_auth("test_user", "%");
    let mut roles = Vec::new();
    let mut ownerships = Vec::new();

    let catalog_names = ["default", "prod", "staging", "analytics"];
    let catalogs = &catalog_names[..num_catalogs.min(4)];

    let mut rng = StdRng::seed_from_u64(42); // Fixed seed for reproducibility
    let mut counter = 0u64;

    // Use Zipf distribution to simulate realistic scenario: few DBs have many tables, most DBs have few tables
    // Generate random table distribution
    let mut tables_per_db = vec![0usize; num_databases];
    for _ in 0..total_tables {
        // Use power-law distribution: top 20% DBs contain 80% tables
        let db_idx = if rng.gen::<f64>() < 0.8 {
            // 80% of tables go to top 20% of DBs
            rng.gen_range(0..(num_databases / 5).max(1))
        } else {
            // 20% of tables go to remaining 80% of DBs
            rng.gen_range(0..num_databases)
        };
        tables_per_db[db_idx] += 1;
    }

    // Generate all objects
    for catalog in catalogs {
        for (db_idx, &num_tables) in tables_per_db.iter().enumerate() {
            let db_id = generate_id(counter);
            counter += 1;

            // Add database ownership
            ownerships.push(OwnershipInfo {
                role: format!("role_{}", db_idx % 10),
                object: OwnershipObject::Database {
                    catalog_name: catalog.to_string(),
                    db_id,
                },
            });

            // Add tables for this database
            for table_idx in 0..num_tables {
                let table_id = generate_id(counter);
                counter += 1;

                ownerships.push(OwnershipInfo {
                    role: format!("role_{}", (db_idx + table_idx) % 10),
                    object: OwnershipObject::Table {
                        catalog_name: catalog.to_string(),
                        db_id,
                        table_id,
                    },
                });
            }
        }
    }

    // Create roles
    for i in 0..10 {
        let role = RoleInfo::new(&format!("role_{}", i), None);
        roles.push(role);
    }

    (user, roles, ownerships)
}

// Benchmark: Realistic scenario - 1 million tables + 5000 databases
// Simulates production environment workload with 1M tables and 5K databases
#[divan::bench]
fn realistic_1m_tables_5k_dbs(bencher: divan::Bencher) {
    // 1 million tables, 5000 databases, 1 catalog
    let (user, roles, ownerships) = create_realistic_test_data(1_000_000, 5_000, 1);

    bencher.bench(|| {
        divan::black_box(GrantObjectVisibilityChecker::new(
            &user,
            &roles,
            &ownerships,
        ))
    });
}

/// Helper function to create SHOW TABLES test data
/// Creates test data for SHOW TABLES benchmarks
fn create_show_tables_test_data_optimized(
    num_tables: usize,
) -> (UserInfo, Vec<RoleInfo>, Vec<OwnershipInfo>) {
    let user = UserInfo::new_no_auth("a", "%");
    let mut roles = Vec::new();
    let mut ownerships = Vec::new();

    let role = RoleInfo::new("test", None);
    roles.push(role);

    let catalog_name = "default";
    let db_id = generate_id(1);

    // Create database ownership
    ownerships.push(OwnershipInfo {
        role: "test".to_string(),
        object: OwnershipObject::Database {
            catalog_name: catalog_name.to_string(),
            db_id,
        },
    });

    // Create specified number of table ownerships
    for i in 0..num_tables {
        let table_id = generate_id(1000 + i as u64);
        ownerships.push(OwnershipInfo {
            role: "test".to_string(),
            object: OwnershipObject::Table {
                catalog_name: catalog_name.to_string(),
                db_id,
                table_id,
            },
        });
    }

    (user, roles, ownerships)
}

// Benchmark: SHOW TABLES scenario - 100K tables in single database
// Simulates SHOW TABLES scenario: 100K tables in a single database
#[divan::bench]
fn show_tables_100k_tables(bencher: divan::Bencher) {
    let (user, roles, ownerships) = create_show_tables_test_data_optimized(100_000);

    bencher.bench(|| {
        // Tests construction time of GrantObjectVisibilityChecker
        // In real SHOW TABLES, this is constructed once per session creation
        divan::black_box(GrantObjectVisibilityChecker::new(
            &user,
            &roles,
            &ownerships,
        ))
    });
}

// Benchmark: Single table visibility check after construction
// Simulates the common case: checker is constructed once, then used for single table checks
#[divan::bench(args = [100_000, 1_000_000])]
fn show_tables_single_check(bencher: divan::Bencher, num_tables: usize) {
    let (user, roles, ownerships) = create_show_tables_test_data_optimized(num_tables);

    let catalog_name = "default";
    let db_id = generate_id(1);

    // Get a table ID to check (middle of the range for realistic lookup)
    let table_id = generate_id(1000 + (num_tables / 2) as u64);

    bencher.bench(|| {
        let checker = divan::black_box(GrantObjectVisibilityChecker::new(
            &user,
            &roles,
            &ownerships,
        ));

        // Check visibility of a single table (the common case)
        divan::black_box(checker.check_table_visibility(
            catalog_name,
            "db1",
            "dummy",
            db_id,
            table_id,
        ))
    });
}

// Benchmark: Amortized cost of single table check (checker pre-constructed)
// This measures just the check_table_visibility call cost, not construction
#[divan::bench(args = [100_000, 1_000_000])]
fn table_visibility_check_only(bencher: divan::Bencher, num_tables: usize) {
    let (user, roles, ownerships) = create_show_tables_test_data_optimized(num_tables);
    let checker = GrantObjectVisibilityChecker::new(&user, &roles, &ownerships);

    let catalog_name = "default";
    let db_id = generate_id(1);
    let table_id = generate_id(1000 + (num_tables / 2) as u64);

    bencher.bench(|| {
        divan::black_box(checker.check_table_visibility(
            catalog_name,
            "db1",
            "dummy",
            db_id,
            table_id,
        ))
    });
}
