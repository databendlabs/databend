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

use common_settings::Settings;

#[test]
fn test_set_settings() {
    let settings = Settings::create("test".to_string());
    // Ok.
    {
        settings.set_max_threads(2).unwrap();
    }

    // Number out of range.
    {
        let result = settings.set_max_threads(1025);
        let expect = "BadArguments. Code: 1006, Text = max_threads: Value 1025 is not within the range [1, 1024].";
        assert_eq!(expect, format!("{}", result.unwrap_err()));
    }

    // String out of range.
    {
        // Ok
        settings
            .set_setting("query_flight_compression".to_string(), "LZ4".to_string())
            .unwrap();

        // Error
        let result = settings.set_setting("query_flight_compression".to_string(), "xx".to_string());
        let expect = "BadArguments. Code: 1006, Text = Value xx is not within the allowed values [\"None\", \"LZ4\", \"ZSTD\"].";
        assert_eq!(expect, format!("{}", result.unwrap_err()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_set_global_settings() {
    let settings = Settings::create("test".to_string());
    let result = settings
        .set_global_setting(
            "query_flight_compression_notfound".to_string(),
            "xx".to_string(),
        )
        .await;
    let expect = "UnknownVariable. Code: 2801, Text = Unknown variable: \"query_flight_compression_notfound\".";
    assert_eq!(expect, format!("{}", result.unwrap_err()));
}
