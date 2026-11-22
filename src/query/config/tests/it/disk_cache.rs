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

use std::io::Write;

use clap::Parser;
use databend_common_config::Config;

#[derive(Parser)]
struct Args {
    #[clap(flatten)]
    config: Config,
}

fn parse(args: &[&str]) -> Config {
    Args::try_parse_from(args).unwrap().config
}

/// `cache.disk.max_bytes` is unset unless configured, which sizes the disk cache from its file
/// system. A value from the file stays unless env or args set one, 0 included: the platform
/// turns the cache off with `CACHE_DISK_MAX_BYTES=0` whatever the file says.
#[test]
fn test_disk_max_bytes_is_unset_unless_configured() {
    let max_bytes = |config: &Config| config.cache.disk_cache_config.max_bytes;
    assert_eq!(max_bytes(&parse(&["databend-query"])), None);
    let flag = parse(&["databend-query", "--cache-disk-max-bytes", "0"]);
    assert_eq!(max_bytes(&flag), Some(0));

    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(b"[cache.disk]\nmax_bytes = 21474836480\n")
        .unwrap();
    let path = file.path().to_str().unwrap().to_string();
    let load = |env: Option<&str>| {
        temp_env::with_var("CACHE_DISK_MAX_BYTES", env, || {
            Config::load_with_config_file(&path).unwrap()
        })
    };
    assert_eq!(max_bytes(&load(None)), Some(21474836480));
    assert_eq!(max_bytes(&load(Some("0"))), Some(0));
    assert_eq!(max_bytes(&load(Some("512"))), Some(512));
    // Args leaving it unset keep the file's value.
    let merged = parse(&["databend-query"]).merge(&path).unwrap();
    assert_eq!(max_bytes(&merged), Some(21474836480));

    let unset = temp_env::with_var_unset("CACHE_DISK_MAX_BYTES", || {
        Config::load_with_config_file("").unwrap()
    });
    assert_eq!(max_bytes(&unset), None);
}
