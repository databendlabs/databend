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

use databend_common_meta_types::raft_types::Membership;
use databend_common_meta_types::raft_types::StoredMembership;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::Node;
use databend_common_meta_types::UpsertKV;
use map_api::mvcc::ScopedSet;
use maplit::btreemap;
use openraft::testing::log_id;
use state_machine_api::UserKey;

use crate::leveled_store::leveled_map::LeveledMap;
use crate::sm_v003::SMV003;

/// Create multi levels store:
///
/// l2 |         c(D) d
/// l1 |    b(D) c        e
/// l0 | a  b    c    d              // db
pub(crate) async fn build_3_levels_leveled_map() -> anyhow::Result<LeveledMap> {
    let lm = LeveledMap::default();
    lm.with_sys_data(|sd| {
        *sd.last_membership_mut() = StoredMembership::new(
            Some(log_id(1, 1, 1)),
            Membership::new_with_defaults(vec![], []),
        );
        *sd.last_applied_mut() = Some(log_id(1, 1, 1));
        *sd.nodes_mut() = btreemap! {1=>Node::new("1", Endpoint::new("1", 1))};
    });

    let mut view = lm.to_view();

    // internal_seq: 0
    view.set(user_key("a"), Some((None, b("a0"))));
    view.set(user_key("b"), Some((None, b("b0"))));
    view.set(user_key("c"), Some((None, b("c0"))));
    view.set(user_key("d"), Some((None, b("d0"))));

    view.commit().await?;

    lm.freeze_writable_without_permit();

    lm.with_sys_data(|sd| {
        *sd.last_membership_mut() = StoredMembership::new(
            Some(log_id(2, 2, 2)),
            Membership::new_with_defaults(vec![], []),
        );
        *sd.last_applied_mut() = Some(log_id(2, 2, 2));
        *sd.nodes_mut() = btreemap! {2=>Node::new("2", Endpoint::new("2", 2))};
    });
    let mut view = lm.to_view();

    // internal_seq: 4
    view.set(user_key("b"), None);
    view.set(user_key("c"), Some((None, b("c1"))));
    view.set(user_key("e"), Some((None, b("e1"))));
    view.commit().await?;

    lm.freeze_writable_without_permit();

    lm.with_sys_data(|sd| {
        *sd.last_membership_mut() = StoredMembership::new(
            Some(log_id(3, 3, 3)),
            Membership::new_with_defaults(vec![], []),
        );
        *sd.last_applied_mut() = Some(log_id(3, 3, 3));
        *sd.nodes_mut() = btreemap! {3=>Node::new("3", Endpoint::new("3", 3))};
    });

    let mut view = lm.to_view();

    // internal_seq: 6
    view.set(user_key("c"), None);
    view.set(user_key("d"), Some((None, b("d2"))));
    view.commit().await?;

    Ok(lm)
}

/// The subscript is internal_seq:
///
///    | kv             | expire
///    | ---            | ---
/// l1 | a₄       c₃    |               10,1₄ -> ø    15,4₄ -> a  20,3₃ -> c
/// ------------------------------------------------------------
/// l0 | a₁  b₂         |  5,2₂ -> b    10,1₁ -> a
pub(crate) async fn build_2_levels_leveled_map_with_expire() -> anyhow::Result<LeveledMap> {
    let mut sm = SMV003::default();

    let mut a = sm.new_applier().await;
    a.upsert_kv(&UpsertKV::update("a", b"a0").with_expire_sec(10))
        .await?;
    a.upsert_kv(&UpsertKV::update("b", b"b0").with_expire_sec(5))
        .await?;

    a.commit().await?;

    sm.map_mut().freeze_writable_without_permit();

    let mut a = sm.new_applier().await;
    a.upsert_kv(&UpsertKV::update("c", b"c0").with_expire_sec(20))
        .await?;
    a.upsert_kv(&UpsertKV::update("a", b"a1").with_expire_sec(15))
        .await?;
    a.commit().await?;

    Ok(sm.leveled_map().clone())
}

fn b(x: impl ToString) -> Vec<u8> {
    x.to_string().as_bytes().to_vec()
}

fn user_key(s: impl ToString) -> UserKey {
    UserKey::new(s)
}
