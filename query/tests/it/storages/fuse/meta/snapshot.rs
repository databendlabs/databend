//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::ops::Add;

use common_datavalues::DataSchema;
use databend_query::storages::fuse::meta::TableSnapshot;
use uuid::Uuid;

fn default_snapshot() -> TableSnapshot {
    let uuid = Uuid::new_v4();
    let schema = DataSchema::empty();
    let stats = Default::default();
    TableSnapshot::new(uuid, &None, None, schema, stats, vec![])
}

#[test]
fn snapshot_timestamp_is_some() {
    let s = default_snapshot();
    assert!(s.timestamp.is_some());
}

#[test]
fn snapshot_timestamp_monotonic_increase() {
    let prev = default_snapshot();
    let schema = DataSchema::empty();
    let uuid = Uuid::new_v4();
    let current = TableSnapshot::new(
        uuid,
        &prev.timestamp,
        prev.prev_snapshot_id.clone(),
        schema,
        Default::default(),
        vec![],
    );
    let current_ts = current.timestamp.unwrap();
    let prev_ts = prev.timestamp.unwrap();
    assert!(current_ts > prev_ts)
}

#[test]
fn snapshot_timestamp_time_skew_tolerance() {
    let mut prev = default_snapshot();
    let schema = DataSchema::empty();
    let uuid = Uuid::new_v4();

    // simulating a stalled clock
    prev.timestamp = Some(prev.timestamp.unwrap().add(chrono::Duration::days(1)));

    let current = TableSnapshot::new(
        uuid,
        &prev.timestamp,
        prev.prev_snapshot_id.clone(),
        schema,
        Default::default(),
        vec![],
    );
    let current_ts = current.timestamp.unwrap();
    let prev_ts = prev.timestamp.unwrap();
    assert!(current_ts > prev_ts)
}
