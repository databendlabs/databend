// Copyright 2021 Datafuse Labs.
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

use common_base::tokio;
use common_mem_allocator::malloc_size;
use databend_query::configs::Config;
use databend_query::sessions::Session;
use databend_query::sessions::SessionManager;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_session_mem_usage() {
    let conf = Config::load_from_args();

    let session_manager = SessionManager::from_conf(conf.clone()).await.unwrap();

    let session = Session::try_create(
        conf.clone(),
        String::from("test-001"),
        String::from("test-type"),
        session_manager,
    )
    .unwrap();

    let session_size = malloc_size(&session);

    assert!(session_size > 3000);
    assert_eq!(session_size, session.get_memory_usage());
}
