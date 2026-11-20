// Copyright 2026 Datafuse Labs
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

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::runtime::metrics::register_counter_family;

type Labels = Vec<(&'static str, u64)>;

fn listed(family: &databend_common_base::runtime::metrics::FamilyCounter<Labels>) -> Option<u64> {
    family.get(&vec![("label", 1)]).map(|c| c.get())
}

/// A member counts into the metric `get_or_create` finds.
#[test]
fn test_member_counts_into_its_label_set() {
    let family = register_counter_family::<Labels>(&GlobalUniq::unique());
    let member = family.member(vec![("label", 1)]);
    member.inc_by(2);
    family.get_or_create(&vec![("label", 1)]).inc_by(3);
    assert_eq!((member.get(), listed(&family)), (5, Some(5)));
}

/// After the family is cleared (`TRUNCATE system.metrics`), a member lists its metric again
/// on its next use, counting from zero.
#[test]
fn test_member_is_listed_again_after_clear() {
    let family = register_counter_family::<Labels>(&GlobalUniq::unique());
    let member = family.member(vec![("label", 1)]);
    member.inc_by(7);
    family.clear();
    assert_eq!(listed(&family), None);
    member.inc_by(1);
    assert_eq!((member.get(), listed(&family)), (1, Some(1)));

    // Removing the label set alone works the same.
    assert!(family.remove(&vec![("label", 1)]));
    member.inc_by(4);
    assert_eq!(listed(&family), Some(4));
}

/// If the label set is listed again through `get_or_create` before the member's next use, it is
/// listed with the member's metric, which keeps its fast path.
#[test]
fn test_label_set_listed_again_takes_the_member_metric() {
    let family = register_counter_family::<Labels>(&GlobalUniq::unique());
    let member = family.member(vec![("label", 1)]);
    member.inc_by(7);
    family.clear();
    family.get_or_create(&vec![("label", 1)]).inc_by(2);
    member.inc_by(1);
    member.inc_by(1);
    assert_eq!((member.get(), listed(&family)), (4, Some(4)));
    member.with(|metric| {
        let listed = family.get(&vec![("label", 1)]).unwrap();
        assert!(std::ptr::eq(metric, &*listed));
    });
}

/// Members of one label set share its metric, also when created while it is not listed.
#[test]
fn test_members_of_a_label_set_share_its_metric() {
    let family = register_counter_family::<Labels>(&GlobalUniq::unique());
    let first = family.member(vec![("label", 1)]);
    first.inc_by(1);
    family.clear();
    let second = family.member(vec![("label", 1)]);
    second.inc_by(2);
    first.inc_by(3);
    assert_eq!(
        (first.get(), second.get(), listed(&family)),
        (5, 5, Some(5))
    );
}
