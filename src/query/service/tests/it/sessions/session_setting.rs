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

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_query::sessions::SessionType;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_session_setting() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let session = fixture.new_session_with_type(SessionType::Dummy).await?;

    // Settings.
    {
        let settings = session.get_settings();
        settings.set_setting("max_threads".to_string(), "3".to_string())?;
        let actual = settings.get_max_threads()?;
        let expect = 3;
        assert_eq!(actual, expect);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_session_setting_override() -> Result<()> {
    // Setup.
    let config = ConfigBuilder::create()
        .max_storage_io_requests(1000)
        .parquet_fast_read_bytes(1000000)
        .build();
    let fixture = TestFixture::setup_with_config(&config).await?;
    let session = fixture.new_session_with_type(SessionType::Dummy).await?;

    // Settings.
    {
        let settings = session.get_settings();
        let overrided = settings.get_parquet_fast_read_bytes()?;
        let expect = 1000000;
        assert_eq!(overrided, expect);
        let overrided = settings.get_max_storage_io_requests()?;
        let expect = 1000;
        assert_eq!(overrided, expect);
        settings.set_setting("max_storage_io_requests".to_string(), "300".to_string())?;
        let actual = settings.get_max_storage_io_requests()?;
        let expect = 300;
        assert_eq!(actual, expect);
    }

    Ok(())
}
