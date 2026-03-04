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

use std::ffi::OsString;
use std::io::Write;

use clap::Parser;
use databend_common_config::Config;
use databend_common_config::InnerConfig;
use pretty_assertions::assert_eq;

/// It's required to make sure setting's default value is the same with clap.
#[test]
fn test_config_default() {
    let setting_default = InnerConfig::default();
    let config_default: InnerConfig = Config::parse_from(Vec::<OsString>::new())
        .try_into()
        .expect("parse from args must succeed");

    assert_eq!(
        setting_default, config_default,
        "default setting is different from default config, please check again"
    )
}

#[test]
fn test_load_config() {
    // Create a comprehensive test configuration with multiple sections and data types
    let conf = r#"
# Query configuration section
[query]
max_active_sessions = 256
shutdown_wait_timeout_ms = 5000
flight_record_quota_size = 1048576
unknown_query_field = "should be ignored"

# Log configuration section
[log]
file.level = "INFO"
file.dir = "/tmp/databend/logs"
unknown_log_field = 123

# Storage configuration section
[storage]
type = "fs"

[storage.fs]
data_path = "/tmp/databend/data"

# Meta configuration section
[meta]
endpoint = "localhost:9191"
username = "databend"
password = "databend123"
"#;

    let mut temp_file = tempfile::NamedTempFile::new().unwrap();
    temp_file.write_all(conf.as_bytes()).unwrap();
    let temp_path = temp_file.path().to_str().unwrap().to_string();

    // Save the original environment variable (if it exists)
    let original_env = std::env::var("CONFIG_FILE").ok();

    // Set the environment variable to our test config file
    std::env::set_var("CONFIG_FILE", &temp_path);

    // Use the original load function (without a config_file parameter)
    let config = Config::load(false).unwrap();

    // Restore the original environment variable
    match original_env {
        Some(val) => std::env::set_var("CONFIG_FILE", val),
        None => std::env::remove_var("CONFIG_FILE"),
    }

    // Test query configuration values
	assert_eq!(
		config.query.common.max_active_sessions, 256,
		"max_active_sessions should be 256"
	);
	assert_eq!(
		config.query.common.shutdown_wait_timeout_ms, 5000,
		"shutdown_wait_timeout_ms should be 5000"
	);
	assert_eq!(
		config.query.common.flight_record_quota_size, 1048576,
		"flight_record_quota_size should be 1048576"
	);

    // Test log configuration values
    assert_eq!(
        config.log.file.level, "INFO",
        "log.file.level should be INFO"
    );
    assert_eq!(
        config.log.file.dir,
        "/tmp/databend/logs",
        "log.file.dir should be /tmp/databend/logs"
    );

    // Test storage configuration values
    assert_eq!(
        config.storage.fs.data_path, "/tmp/databend/data",
        "storage.fs.data_path should be /tmp/databend/data"
    );

    // Test meta configuration values
    assert_eq!(
        config.meta.endpoint, "localhost:9191",
        "meta.endpoint should be localhost:9191"
    );
    assert_eq!(
        config.meta.username, "databend",
        "meta.username should be databend"
    );
    assert_eq!(
        config.meta.password, "databend123",
        "meta.password should be databend123"
    );
}
