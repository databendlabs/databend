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

//! Spill profile related tests: verify spill read/write metrics wiring to Profile.

use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_base::runtime::ThreadTracker;
use databend_query::spillers::record_read_profile_with_flag;
use databend_query::spillers::record_write_profile_with_flag;

fn create_test_profile() -> Arc<Profile> {
    Arc::new(Profile::create(
        0,
        "test_spill_profile".to_string(),
        None,
        None,
        None,
        Arc::new(String::new()),
        Arc::new(vec![]),
        None,
    ))
}

#[test]
fn test_spill_profile_write_local_and_remote() {
    ThreadTracker::init();

    // Local spill metrics
    let mut payload_local = ThreadTracker::new_tracking_payload();
    let local_profile = create_test_profile();
    payload_local.profile = Some(local_profile.clone());

    {
        let _guard = ThreadTracker::tracking(payload_local);
        let start = Instant::now();
        record_write_profile_with_flag(true, &start, 128);
    }

    assert_eq!(
        local_profile.load_profile(ProfileStatisticsName::LocalSpillWriteCount),
        1,
    );
    assert_eq!(
        local_profile.load_profile(ProfileStatisticsName::LocalSpillWriteBytes),
        128,
    );

    // Remote spill metrics
    let mut payload_remote = ThreadTracker::new_tracking_payload();
    let remote_profile = create_test_profile();
    payload_remote.profile = Some(remote_profile.clone());

    {
        let _guard = ThreadTracker::tracking(payload_remote);
        let start = Instant::now();
        record_write_profile_with_flag(false, &start, 256);
    }

    assert_eq!(
        remote_profile.load_profile(ProfileStatisticsName::RemoteSpillWriteCount),
        1,
    );
    assert_eq!(
        remote_profile.load_profile(ProfileStatisticsName::RemoteSpillWriteBytes),
        256,
    );
}

#[test]
fn test_spill_profile_read_local_and_remote() {
    ThreadTracker::init();

    // Local spill metrics
    let mut payload_local = ThreadTracker::new_tracking_payload();
    let local_profile = create_test_profile();
    payload_local.profile = Some(local_profile.clone());

    {
        let _guard = ThreadTracker::tracking(payload_local);
        let start = Instant::now();
        record_read_profile_with_flag(true, &start, 64);
    }

    assert_eq!(
        local_profile.load_profile(ProfileStatisticsName::LocalSpillReadCount),
        1,
    );
    assert_eq!(
        local_profile.load_profile(ProfileStatisticsName::LocalSpillReadBytes),
        64,
    );

    // Remote spill metrics
    let mut payload_remote = ThreadTracker::new_tracking_payload();
    let remote_profile = create_test_profile();
    payload_remote.profile = Some(remote_profile.clone());

    {
        let _guard = ThreadTracker::tracking(payload_remote);
        let start = Instant::now();
        record_read_profile_with_flag(false, &start, 512);
    }

    assert_eq!(
        remote_profile.load_profile(ProfileStatisticsName::RemoteSpillReadCount),
        1,
    );
    assert_eq!(
        remote_profile.load_profile(ProfileStatisticsName::RemoteSpillReadBytes),
        512,
    );
}
