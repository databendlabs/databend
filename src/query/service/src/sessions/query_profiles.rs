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

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use databend_common_pipeline::core::PlanProfile;
use parking_lot::Mutex;

/// Plan ids are only unique within a single execution unit (one physical plan tree). A query can
/// produce several units: the main query, one executor per materialized CTE fill, the iterations of
/// a recursive CTE, post-execution hooks, etc. Each unit numbers its plans from 0, so plan ids
/// collide across units. We therefore key profiles by `(group_id, plan_id)`.
///
/// - Profiles sharing a `group_id` (recursive CTE iterations, distributed fragments of the same
///   plan across nodes) merge by plan id, which is exactly the desired behaviour.
/// - Profiles in different groups stay separate and are exported as independent plan trees.
type QueryProfileKey = (u64, Option<u32>);

#[derive(Default)]
pub struct QueryProfiles {
    profiles: Mutex<HashMap<QueryProfileKey, PlanProfile>>,
}

impl QueryProfiles {
    /// Merge a batch of profiles (all carrying their own `group_id`) into the store.
    pub fn add(&self, profiles: &HashMap<u32, PlanProfile>) {
        let mut merged_profiles = self.profiles.lock();

        for query_profile in profiles.values() {
            match merged_profiles.entry((query_profile.group_id, query_profile.id)) {
                Entry::Vacant(v) => {
                    v.insert(query_profile.clone());
                }
                Entry::Occupied(mut v) => {
                    v.get_mut().merge(query_profile);
                }
            };
        }
    }

    /// Return the profiles of a single group, keyed by their original (un-offset) plan id.
    ///
    /// EXPLAIN ANALYZE renders one physical plan tree and looks up each node by its own plan id,
    /// so it must read a group without the query-wide id remapping that `get` applies.
    pub fn get_by_group(&self, group_id: u64) -> Vec<PlanProfile> {
        self.profiles
            .lock()
            .iter()
            .filter(|((stored_group_id, _), _)| *stored_group_id == group_id)
            .map(|(_, profile)| profile.clone())
            .collect()
    }

    /// Export every group as a single flat list with a query-wide unique plan id space.
    ///
    /// Each group keeps its internal tree shape (parent links); groups are laid out in disjoint id
    /// ranges by offsetting their plan ids, so the result reads as several independent plan trees
    /// that never collide. Groups are ordered by `group_id` for deterministic output.
    pub fn get(&self) -> Vec<PlanProfile> {
        let profiles = self.profiles.lock();

        let mut by_group: HashMap<u64, Vec<PlanProfile>> = HashMap::new();
        for ((group_id, _), profile) in profiles.iter() {
            by_group.entry(*group_id).or_default().push(profile.clone());
        }

        let mut group_ids = by_group.keys().copied().collect::<Vec<_>>();
        group_ids.sort_unstable();

        let mut next_id = 0;
        let mut remapped = Vec::with_capacity(profiles.len());
        for group_id in group_ids {
            let Some(mut group_profiles) = by_group.remove(&group_id) else {
                continue;
            };

            group_profiles.sort_by_key(|profile| profile.id);
            let max_plan_id = group_profiles
                .iter()
                .filter_map(|profile| profile.id)
                .max()
                .unwrap_or(0);
            let id_offset = next_id;
            next_id += max_plan_id + 1;

            for profile in &mut group_profiles {
                profile.id = profile.id.map(|id| id + id_offset);
                profile.parent_id = profile.parent_id.map(|id| id + id_offset);
            }
            remapped.extend(group_profiles);
        }

        remapped
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use databend_common_base::runtime::profile::ProfileStatisticsName;

    use super::*;

    fn profile(group_id: u64, plan_id: u32, parent_id: Option<u32>, rows: usize) -> PlanProfile {
        let mut statistics = [0; std::mem::variant_count::<ProfileStatisticsName>()];
        statistics[0] = rows;
        PlanProfile {
            id: Some(plan_id),
            name: Some(format!("plan-{plan_id}")),
            parent_id,
            group_id,
            title: Arc::new(format!("plan {plan_id}")),
            labels: Arc::new(vec![]),
            statistics,
            metrics: BTreeMap::new(),
            errors: vec![],
        }
    }

    fn batch(profiles: Vec<PlanProfile>) -> HashMap<u32, PlanProfile> {
        profiles.into_iter().map(|p| (p.id.unwrap(), p)).collect()
    }

    // Same (group, plan_id) across batches — e.g. recursive CTE iterations or distributed
    // fragments of one plan on several nodes — must accumulate their statistics.
    #[test]
    fn merges_within_a_group_by_plan_id() {
        let profiles = QueryProfiles::default();
        profiles.add(&batch(vec![profile(7, 0, None, 1)]));
        profiles.add(&batch(vec![profile(7, 0, None, 2)]));

        let group = profiles.get_by_group(7);
        assert_eq!(group.len(), 1);
        assert_eq!(group[0].id, Some(0));
        assert_eq!(group[0].statistics[0], 3);
    }

    // The same plan_id in different groups (e.g. main query vs a materialized CTE fill) must stay
    // separate instead of being merged together.
    #[test]
    fn keeps_groups_isolated() {
        let profiles = QueryProfiles::default();
        profiles.add(&batch(vec![profile(1, 0, None, 10)]));
        profiles.add(&batch(vec![profile(2, 0, None, 20)]));

        assert_eq!(profiles.get_by_group(1)[0].statistics[0], 10);
        assert_eq!(profiles.get_by_group(2)[0].statistics[0], 20);
        assert_eq!(profiles.get().len(), 2);
    }

    // `get_by_group` returns original plan ids (for EXPLAIN's per-tree lookup), unaffected by other
    // groups in the store.
    #[test]
    fn get_by_group_keeps_original_ids() {
        let profiles = QueryProfiles::default();
        profiles.add(&batch(vec![profile(1, 0, None, 1), profile(1, 1, Some(0), 2)]));
        profiles.add(&batch(vec![profile(2, 0, None, 4)]));

        let mut group = profiles.get_by_group(1);
        group.sort_by_key(|p| p.id);
        assert_eq!(group.iter().map(|p| p.id).collect::<Vec<_>>(), vec![
            Some(0),
            Some(1)
        ]);
        assert_eq!(group[1].parent_id, Some(0));
    }

    // `get` lays groups out in disjoint plan-id ranges (by offset) while preserving each group's
    // internal parent links, producing several independent trees in one id space.
    #[test]
    fn flattens_groups_into_disjoint_id_ranges() {
        let profiles = QueryProfiles::default();
        profiles.add(&batch(vec![profile(1, 0, None, 1), profile(1, 1, Some(0), 2)]));
        profiles.add(&batch(vec![profile(2, 0, None, 4), profile(2, 1, Some(0), 8)]));

        let mut all = profiles.get();
        all.sort_by_key(|p| p.statistics[0]);

        // group 1 -> ids {0,1}; group 2 -> ids offset by (max plan id of group 1)+1 = 2 -> {2,3}.
        assert_eq!(all.iter().map(|p| p.id).collect::<Vec<_>>(), vec![
            Some(0),
            Some(1),
            Some(2),
            Some(3)
        ]);
        // Parent links stay within each group's own range; the two roots remain parentless.
        assert_eq!(all.iter().map(|p| p.parent_id).collect::<Vec<_>>(), vec![
            None,
            Some(0),
            None,
            Some(2)
        ]);
    }
}

