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

use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileLabel;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_base::runtime::runtime_tracker::ThreadTracker;
use databend_common_base::runtime::runtime_tracker::TrackingGuard;
use databend_common_base::runtime::ScopedRegistry;
use databend_common_base::runtime::TrackingPayload;
use databend_query::spillers::record_read_profile_with_flag;
use databend_query::spillers::record_write_profile_with_flag;

fn install_profile() -> (Arc<Profile>, TrackingGuard) {
    let profile = Arc::new(Profile::create(
        1,
        "test".to_string(),
        None,
        None,
        None,
        Arc::new("title".to_string()),
        Arc::new(vec![ProfileLabel::create("k".to_string(), vec!["v".to_string()])]),
        Some(Arc::new(ScopedRegistry::create(None))),
    ));

    let mut payload = ThreadTracker::new_tracking_payload();
    payload.profile = Some(profile.clone());
    payload.metrics = Some(Arc::new(ScopedRegistry::create(None)));
    let guard = ThreadTracker::tracking(payload);
    (profile, guard)
}

#[test]
fn test_local_spill_profiles() {
    let (profile, _guard) = install_profile();

    let start = Instant::now();
    record_write_profile_with_flag(true, &start, 10);
    record_read_profile_with_flag(true, &start, 5);

    assert_eq!(
        profile.load_profile(ProfileStatisticsName::LocalSpillWriteCount),
        1
    );
    assert_eq!(
        profile.load_profile(ProfileStatisticsName::LocalSpillWriteBytes),
        10
    );
    assert!(
        profile.load_profile(ProfileStatisticsName::LocalSpillWriteTime) > 0,
        "write time should be recorded"
    );

    assert_eq!(
        profile.load_profile(ProfileStatisticsName::LocalSpillReadCount),
        1
    );
    assert_eq!(
        profile.load_profile(ProfileStatisticsName::LocalSpillReadBytes),
        5
    );
    assert!(
        profile.load_profile(ProfileStatisticsName::LocalSpillReadTime) > 0,
        "read time should be recorded"
    );
}

#[test]
fn test_remote_spill_profiles() {
    let profile = install_profile();

    let start = Instant::now();
    record_write_profile_with_flag(false, &start, 7);
    record_read_profile_with_flag(false, &start, 3);

    assert_eq!(
        profile.load_profile(ProfileStatisticsName::RemoteSpillWriteCount),
        1
    );
    assert_eq!(
        profile.load_profile(ProfileStatisticsName::RemoteSpillWriteBytes),
        7
    );
    assert!(
        profile.load_profile(ProfileStatisticsName::RemoteSpillWriteTime) > 0,
        "write time should be recorded"
    );

    assert_eq!(
        profile.load_profile(ProfileStatisticsName::RemoteSpillReadCount),
        1
    );
    assert_eq!(
        profile.load_profile(ProfileStatisticsName::RemoteSpillReadBytes),
        3
    );
    assert!(
        profile.load_profile(ProfileStatisticsName::RemoteSpillReadTime) > 0,
        "read time should be recorded"
    );
}
