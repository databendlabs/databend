// Copyright 2023 Datafuse Labs.
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

use common_base::base::GlobalInstance;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_exception::Result;
use opendal::raw::HttpClient;

/// GlobalHttpClient is the global shared http client for storage.
pub struct GlobalHttpClient;

impl GlobalHttpClient {
    pub async fn init() -> Result<()> {
        // Make sure http client is initiated inside global runtime.
        GlobalIORuntime::instance()
            .spawn(async {
                let client = HttpClient::new()?;
                GlobalInstance::set(client);

                Ok::<_, common_exception::ErrorCode>(())
            })
            .await
            .expect("http client must build with success")?;

        Ok(())
    }

    pub fn instance() -> HttpClient {
        GlobalInstance::get()
    }
}
