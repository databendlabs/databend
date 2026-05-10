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

type ProfilesByPlanId = HashMap<Option<u32>, PlanProfile>;
type Profiles = HashMap<Option<String>, ProfilesByPlanId>;

#[derive(Default)]
pub struct QueryProfiles {
    profiles: Profiles,
    execution_ranges: ExecutionRangeState,
}

#[derive(Default)]
struct ExecutionRangeState {
    ranges: HashMap<String, ProfileExecutionRange>,
    next_id: u32,
}

#[derive(Clone, Copy)]
struct ProfileExecutionRange {
    offset: u32,
    id_count: u32,
}

impl QueryProfiles {
    pub fn get(&self) -> Vec<PlanProfile> {
        let mut profiles = HashMap::new();

        for (namespace, namespace_profiles) in self.profiles.iter() {
            match namespace {
                Some(profile_execution_id) => {
                    let Some(range) = self
                        .execution_ranges
                        .ranges
                        .get(profile_execution_id)
                        .copied()
                    else {
                        continue;
                    };
                    merge_into(
                        &mut profiles,
                        namespace_profiles
                            .values()
                            .map(|profile| offset_profile(profile, range.offset)),
                    );
                }
                None => {
                    merge_into(&mut profiles, namespace_profiles.values().cloned());
                }
            }
        }

        profiles.into_values().collect()
    }

    pub fn get_for_execution(&self, profile_execution_id: &str) -> HashMap<u32, PlanProfile> {
        let mut profiles = self.get_with_execution_id(profile_execution_id);
        if let Some(unscoped_profiles) = self.profiles.get(&None) {
            for profile in unscoped_profiles.values() {
                let Some(id) = profile.id else {
                    continue;
                };
                match profiles.entry(id) {
                    Entry::Vacant(v) => {
                        v.insert(profile.clone());
                    }
                    Entry::Occupied(mut v) => {
                        v.get_mut().merge(profile);
                    }
                };
            }
        }
        profiles
    }

    pub fn get_with_execution_id(&self, profile_execution_id: &str) -> HashMap<u32, PlanProfile> {
        let Some(execution_profiles) = self.profiles.get(&Some(profile_execution_id.to_string()))
        else {
            return HashMap::new();
        };

        let mut profiles = HashMap::new();
        for profile in execution_profiles.values() {
            let Some(id) = profile.id else {
                continue;
            };
            profiles.insert(id, profile.clone());
        }
        profiles
    }

    pub fn add(&mut self, profiles: &HashMap<u32, PlanProfile>) {
        merge_into(
            self.profiles.entry(None).or_default(),
            profiles.values().cloned(),
        );
    }

    pub fn add_with_execution(
        &mut self,
        profile_execution_id: &str,
        profiles: &HashMap<u32, PlanProfile>,
    ) {
        if self
            .execution_range(profile_execution_id, profiles)
            .is_none()
        {
            return;
        };

        merge_into(
            self.profiles
                .entry(Some(profile_execution_id.to_string()))
                .or_default(),
            profiles.values().cloned(),
        );
    }

    fn execution_range(
        &mut self,
        profile_execution_id: &str,
        profiles: &HashMap<u32, PlanProfile>,
    ) -> Option<ProfileExecutionRange> {
        let id_count = Self::id_count(profiles)?;
        Some(self.reserve_execution_range(profile_execution_id, id_count))
    }

    fn id_count(profiles: &HashMap<u32, PlanProfile>) -> Option<u32> {
        profiles
            .values()
            .filter_map(|profile| profile.id)
            .max()
            .map(|id| id + 1)
    }

    fn reserve_execution_range(
        &mut self,
        profile_execution_id: &str,
        id_count: u32,
    ) -> ProfileExecutionRange {
        let state = &mut self.execution_ranges;
        if let Some(range) = state.ranges.get(profile_execution_id).copied() {
            if id_count <= range.id_count {
                return range;
            }

            let current_end = range.offset + range.id_count;
            if state.next_id == current_end {
                state.next_id = range.offset + id_count;
                let new_range = ProfileExecutionRange {
                    offset: range.offset,
                    id_count,
                };
                state
                    .ranges
                    .insert(profile_execution_id.to_string(), new_range);
                return new_range;
            }

            let offset = state.next_id;
            state.next_id += id_count;
            let new_range = ProfileExecutionRange { offset, id_count };
            state
                .ranges
                .insert(profile_execution_id.to_string(), new_range);
            return new_range;
        }

        let offset = state.next_id;
        state.next_id += id_count;
        let range = ProfileExecutionRange { offset, id_count };
        state.ranges.insert(profile_execution_id.to_string(), range);
        range
    }
}

fn offset_profile(profile: &PlanProfile, offset: u32) -> PlanProfile {
    let mut profile = profile.clone();
    if offset != 0 {
        profile.id = profile.id.map(|id| id + offset);
        profile.parent_id = profile.parent_id.map(|id| id + offset);
    }
    profile
}

fn merge_into(merged_profiles: &mut ProfilesByPlanId, profiles: impl Iterator<Item = PlanProfile>) {
    for query_profile in profiles {
        match merged_profiles.entry(query_profile.id) {
            Entry::Vacant(v) => {
                v.insert(query_profile);
            }
            Entry::Occupied(mut v) => {
                v.get_mut().merge(&query_profile);
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use databend_common_base::runtime::profile::ProfileLabel;

    use super::*;

    fn plan_profile(id: u32, parent_id: Option<u32>, statistic: usize) -> PlanProfile {
        let mut statistics = std::array::from_fn(|_| 0);
        statistics[0] = statistic;

        PlanProfile {
            id: Some(id),
            name: Some(format!("plan-{id}")),
            parent_id,
            title: Arc::new(format!("plan-{id}")),
            labels: Arc::new(Vec::<ProfileLabel>::new()),
            metrics: BTreeMap::new(),
            statistics,
            errors: vec![],
        }
    }

    fn profiles_by_id(profiles: Vec<PlanProfile>) -> HashMap<u32, PlanProfile> {
        profiles
            .into_iter()
            .map(|profile| (profile.id.unwrap(), profile))
            .collect()
    }

    #[test]
    fn query_profiles_reuses_execution_offset_for_repeated_batches() {
        let mut query_profiles = QueryProfiles::default();
        let first_batch =
            profiles_by_id(vec![plan_profile(0, None, 1), plan_profile(1, Some(0), 2)]);
        let second_batch =
            profiles_by_id(vec![plan_profile(0, None, 3), plan_profile(1, Some(0), 4)]);

        query_profiles.add_with_execution("exec-a", &first_batch);
        query_profiles.add_with_execution("exec-a", &second_batch);

        let profiles = profiles_by_id(query_profiles.get());
        assert_eq!(profiles.len(), 2);
        assert_eq!(profiles[&0].statistics[0], 4);
        assert_eq!(profiles[&1].statistics[0], 6);
        assert_eq!(profiles[&1].parent_id, Some(0));

        let raw_profiles = query_profiles.get_with_execution_id("exec-a");
        assert_eq!(raw_profiles.len(), 2);
        assert_eq!(raw_profiles[&0].statistics[0], 4);
        assert_eq!(raw_profiles[&1].statistics[0], 6);
        assert_eq!(raw_profiles[&1].parent_id, Some(0));
    }

    #[test]
    fn query_profiles_remaps_distinct_executions() {
        let mut query_profiles = QueryProfiles::default();
        let first_batch =
            profiles_by_id(vec![plan_profile(0, None, 1), plan_profile(1, Some(0), 2)]);
        let second_batch =
            profiles_by_id(vec![plan_profile(0, None, 3), plan_profile(1, Some(0), 4)]);

        query_profiles.add_with_execution("exec-a", &first_batch);
        query_profiles.add_with_execution("exec-b", &second_batch);

        let profiles = profiles_by_id(query_profiles.get());
        assert_eq!(profiles.len(), 4);
        assert_eq!(profiles[&0].statistics[0], 1);
        assert_eq!(profiles[&1].statistics[0], 2);
        assert_eq!(profiles[&2].statistics[0], 3);
        assert_eq!(profiles[&3].statistics[0], 4);
        assert_eq!(profiles[&1].parent_id, Some(0));
        assert_eq!(profiles[&3].parent_id, Some(2));

        let raw_profiles = query_profiles.get_with_execution_id("exec-b");
        assert_eq!(raw_profiles.len(), 2);
        assert_eq!(raw_profiles[&0].statistics[0], 3);
        assert_eq!(raw_profiles[&1].statistics[0], 4);
        assert_eq!(raw_profiles[&1].parent_id, Some(0));
    }

    #[test]
    fn query_profiles_ignores_empty_execution_batches() {
        let mut query_profiles = QueryProfiles::default();
        let empty_batch = HashMap::new();
        let first_batch = profiles_by_id(vec![plan_profile(0, None, 1)]);
        let second_batch = profiles_by_id(vec![plan_profile(0, None, 2)]);

        query_profiles.add_with_execution("exec-a", &empty_batch);
        query_profiles.add_with_execution("exec-b", &first_batch);
        query_profiles.add_with_execution("exec-a", &second_batch);

        let profiles = profiles_by_id(query_profiles.get());
        assert_eq!(profiles.len(), 2);
        assert_eq!(profiles[&0].statistics[0], 1);
        assert_eq!(profiles[&1].statistics[0], 2);
    }

    #[test]
    fn query_profiles_expands_execution_range_from_later_batch() {
        let mut query_profiles = QueryProfiles::default();
        let first_batch = profiles_by_id(vec![plan_profile(0, None, 1)]);
        let other_execution = profiles_by_id(vec![plan_profile(0, None, 2)]);
        let later_batch = profiles_by_id(vec![plan_profile(2, Some(0), 3)]);

        query_profiles.add_with_execution("exec-a", &first_batch);
        query_profiles.add_with_execution("exec-b", &other_execution);
        query_profiles.add_with_execution("exec-a", &later_batch);

        let raw_profiles = query_profiles.get_with_execution_id("exec-a");
        assert_eq!(raw_profiles.len(), 2);
        assert_eq!(raw_profiles[&0].statistics[0], 1);
        assert_eq!(raw_profiles[&2].statistics[0], 3);
        assert_eq!(raw_profiles[&2].parent_id, Some(0));

        let other_profiles = query_profiles.get_with_execution_id("exec-b");
        assert_eq!(other_profiles.len(), 1);
        assert_eq!(other_profiles[&0].statistics[0], 2);

        let profiles = profiles_by_id(query_profiles.get());
        assert_eq!(profiles.len(), 3);
        assert_eq!(profiles[&1].statistics[0], 2);
        assert_eq!(profiles[&2].statistics[0], 1);
        assert_eq!(profiles[&4].statistics[0], 3);
        assert_eq!(profiles[&4].parent_id, Some(2));
    }

    #[test]
    fn query_profiles_can_overlay_unscoped_profiles_on_one_execution() {
        let mut query_profiles = QueryProfiles::default();
        let cte_producer =
            profiles_by_id(vec![plan_profile(0, None, 1), plan_profile(1, Some(0), 2)]);
        let main_query = profiles_by_id(vec![plan_profile(0, None, 3)]);
        let recursive_steps = profiles_by_id(vec![
            plan_profile(1, Some(0), 4),
            plan_profile(2, Some(1), 5),
        ]);

        query_profiles.add_with_execution("cte-producer", &cte_producer);
        query_profiles.add_with_execution("main-query", &main_query);
        query_profiles.add(&recursive_steps);

        let cte_profiles = query_profiles.get_with_execution_id("cte-producer");
        assert_eq!(cte_profiles.len(), 2);
        assert_eq!(cte_profiles[&0].statistics[0], 1);
        assert_eq!(cte_profiles[&1].statistics[0], 2);

        let main_profiles = query_profiles.get_with_execution_id("main-query");
        assert_eq!(main_profiles.len(), 1);
        assert_eq!(main_profiles[&0].statistics[0], 3);

        let main_profiles = query_profiles.get_for_execution("main-query");
        assert_eq!(main_profiles.len(), 3);
        assert_eq!(main_profiles[&0].statistics[0], 3);
        assert_eq!(main_profiles[&1].statistics[0], 4);
        assert_eq!(main_profiles[&2].statistics[0], 5);
    }

    #[test]
    fn query_profiles_keeps_multiple_zero_based_executions() {
        let mut query_profiles = QueryProfiles::default();
        let cte_a = profiles_by_id(vec![plan_profile(0, None, 1)]);
        let cte_b = profiles_by_id(vec![plan_profile(0, None, 2)]);
        let main_query = profiles_by_id(vec![plan_profile(0, None, 3)]);

        query_profiles.add_with_execution("cte-a", &cte_a);
        query_profiles.add_with_execution("cte-b", &cte_b);
        query_profiles.add_with_execution("main-query", &main_query);

        let profiles = profiles_by_id(query_profiles.get());
        assert_eq!(profiles.len(), 3);
        assert_eq!(profiles[&0].statistics[0], 1);
        assert_eq!(profiles[&1].statistics[0], 2);
        assert_eq!(profiles[&2].statistics[0], 3);
    }
}
