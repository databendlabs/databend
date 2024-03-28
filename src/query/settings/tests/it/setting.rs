// Copyright 2022 Datafuse Labs.
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

use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::Settings;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_set_settings() {
    let settings = Settings::create(Tenant::new_literal("test"));
    // Number range.
    {
        settings.set_max_threads(2).unwrap();

        let result = settings.set_max_threads(1025);
        let expect = "WrongValueForVariable. Code: 2803, Text = max_threads: Value 1025 is not within the range [1, 1024].";
        assert_eq!(expect, format!("{}", result.unwrap_err()));
    }

    // Number range.
    {
        {
            // Ok with float.
            settings
                .set_setting("max_memory_usage".to_string(), "1610612736.0".to_string())
                .unwrap();

            // Range than u64.
            let result = settings.set_setting(
                "max_memory_usage".to_string(),
                "161061273600000000000000000000000000000000000000000000000".to_string(),
            );
            let expect = "WrongValueForVariable. Code: 2803, Text = 161061273600000000000000000000000000000000000000000000000 is not a valid integer value.";
            assert_eq!(expect, format!("{}", result.unwrap_err()));

            // Range with neg.
            let result = settings.set_setting("max_memory_usage".to_string(), "-1".to_string());
            let expect =
                "WrongValueForVariable. Code: 2803, Text = -1 is not a valid integer value.";
            assert_eq!(expect, format!("{}", result.unwrap_err()));
        }

        {
            // Ok
            settings
                .set_setting("enable_table_lock".to_string(), "1".to_string())
                .unwrap();
            // Ok
            settings
                .set_setting("enable_table_lock".to_string(), "0".to_string())
                .unwrap();

            // Ok with float.
            settings
                .set_setting("enable_table_lock".to_string(), "1.0".to_string())
                .unwrap();

            // Error
            let result = settings.set_setting("enable_table_lock".to_string(), "3".to_string());
            let expect =
                "WrongValueForVariable. Code: 2803, Text = Value 3 is not within the range [0, 1].";
            assert_eq!(expect, format!("{}", result.unwrap_err()));

            // Error
            let result = settings.set_setting("enable_table_lock".to_string(), "xx".to_string());
            let expect =
                "WrongValueForVariable. Code: 2803, Text = xx is not a valid integer value.";
            assert_eq!(expect, format!("{}", result.unwrap_err()));
        }
    }

    // String out of range.
    {
        // Ok
        settings
            .set_setting("query_flight_compression".to_string(), "LZ4".to_string())
            .unwrap();

        // Ok
        settings
            .set_setting("query_flight_compression".to_string(), "lz4".to_string())
            .unwrap();

        // Error
        let result = settings.set_setting("query_flight_compression".to_string(), "xx".to_string());
        let expect = "WrongValueForVariable. Code: 2803, Text = Value xx is not within the allowed values [\"None\", \"LZ4\", \"ZSTD\"].";
        assert_eq!(expect, format!("{}", result.unwrap_err()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_set_global_settings() {
    let settings = Settings::create(Tenant::new_literal("test"));
    let result = settings
        .set_global_setting(
            "query_flight_compression_notfound".to_string(),
            "xx".to_string(),
        )
        .await;
    let expect = "UnknownVariable. Code: 2801, Text = Unknown variable: \"query_flight_compression_notfound\".";
    assert_eq!(expect, format!("{}", result.unwrap_err()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_set_data_retention_time_in_days() {
    // Init.
    let thread_name = std::thread::current().name().unwrap().to_string();
    databend_common_base::base::GlobalInstance::init_testing(&thread_name);

    // Init with default.
    {
        GlobalConfig::init(&InnerConfig::default()).unwrap();
    }

    let settings = Settings::create(Tenant::new_literal("test"));

    // Default.
    {
        let actual = settings.get_data_retention_time_in_days().unwrap();
        assert_eq!(actual, 1);
    }

    // Ok, 90.
    {
        settings
            .set_setting("data_retention_time_in_days".to_string(), "90".to_string())
            .unwrap();

        let actual = settings.get_data_retention_time_in_days().unwrap();
        assert_eq!(actual, 90);
    }

    // Ok, 0.
    {
        settings
            .set_setting("data_retention_time_in_days".to_string(), "0".to_string())
            .unwrap();

        let actual = settings.get_data_retention_time_in_days().unwrap();
        assert_eq!(actual, 0);
    }

    // Out of range.
    {
        let result =
            settings.set_setting("data_retention_time_in_days".to_string(), "91".to_string());
        let expect =
            "WrongValueForVariable. Code: 2803, Text = Value 91 is not within the range [0, 90].";

        assert_eq!(expect, format!("{}", result.unwrap_err()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_set_data_retention_time_in_days_from_config() {
    // Init.
    let thread_name = std::thread::current().name().unwrap().to_string();
    databend_common_base::base::GlobalInstance::init_testing(&thread_name);

    // Change default value.
    {
        let mut conf = InnerConfig::default();
        conf.query.data_retention_time_in_days_max = 33;
        GlobalConfig::init(&conf).unwrap();
    }

    let settings = Settings::create(Tenant::new_literal("test"));

    // Ok, 0.
    {
        settings
            .set_setting("data_retention_time_in_days".to_string(), "33".to_string())
            .unwrap();

        let actual = settings.get_data_retention_time_in_days().unwrap();
        assert_eq!(actual, 33);
    }

    // Out of range.
    {
        let result =
            settings.set_setting("data_retention_time_in_days".to_string(), "34".to_string());
        let expect =
            "WrongValueForVariable. Code: 2803, Text = Value 34 is not within the range [0, 33].";

        assert_eq!(expect, format!("{}", result.unwrap_err()));
    }
}
