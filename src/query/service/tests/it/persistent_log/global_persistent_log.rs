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

use databend_common_exception::Result;
use databend_query::persistent_log::GlobalPersistentLog;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
pub async fn test_global_persistent_log_acquire_lock() -> Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.log.persistentlog.on = true;
    let _guard = TestFixture::setup_with_config(&config).await?;
    let res = GlobalPersistentLog::instance().try_acquire().await?;
    assert!(res, "should acquire lock");

    let res = GlobalPersistentLog::instance().try_acquire().await?;
    assert!(!res, "should not acquire lock before expire");
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    let res = GlobalPersistentLog::instance().try_acquire().await?;
    assert!(res, "should acquire lock after expire");

    Ok(())
}
