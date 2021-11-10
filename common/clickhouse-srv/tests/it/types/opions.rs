// Copyright 2020 Datafuse Labs.
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

use std::str::FromStr;
use std::time::Duration;

use common_clickhouse_srv::types::*;
use url::Url;

#[test]
fn test_parse_hosts() {
    let source = "host2:9000,host3:9000";
    let expected = vec![
        Url::from_str("tcp://host2:9000").unwrap(),
        Url::from_str("tcp://host3:9000").unwrap(),
    ];
    let actual = parse_hosts(source).unwrap();
    assert_eq!(actual, expected)
}

#[test]
fn test_parse_default() {
    let url = "tcp://host1";
    let options = from_url(url).unwrap();
    assert_eq!(options.database, "default");
    assert_eq!(options.username, "default");
    assert_eq!(options.password, "");
}

#[test]
#[cfg(feature = "tls")]
fn test_parse_secure_options() {
    let url = "tcp://username:password@host1:9001/database?ping_timeout=42ms&keepalive=99s&compression=lz4&connection_timeout=10s&secure=true&skip_verify=true";
    assert_eq!(
        Options {
            username: "username".into(),
            password: "password".into(),
            addr: Url::parse("tcp://username:password@host1:9001").unwrap(),
            database: "database".into(),
            keepalive: Some(Duration::from_secs(99)),
            ping_timeout: Duration::from_millis(42),
            connection_timeout: Duration::from_secs(10),
            compression: true,
            secure: true,
            skip_verify: true,
            ..Options::default()
        },
        from_url(url).unwrap(),
    );
}

#[test]
fn test_parse_options() {
    let url = "tcp://username:password@host1:9001/database?ping_timeout=42ms&keepalive=99s&compression=lz4&connection_timeout=10s";
    assert_eq!(
        Options {
            username: "username".into(),
            password: "password".into(),
            addr: Url::parse("tcp://username:password@host1:9001").unwrap(),
            database: "database".into(),
            keepalive: Some(Duration::from_secs(99)),
            ping_timeout: Duration::from_millis(42),
            connection_timeout: Duration::from_secs(10),
            compression: true,
            ..Options::default()
        },
        from_url(url).unwrap(),
    );
}

#[test]
#[should_panic]
fn test_parse_invalid_url() {
    let url = "ʘ_ʘ";
    from_url(url).unwrap();
}

#[test]
#[should_panic]
fn test_parse_with_unknown_url() {
    let url = "tcp://localhost:9000/foo?bar=baz";
    from_url(url).unwrap();
}

#[test]
#[should_panic]
fn test_parse_with_multi_databases() {
    let url = "tcp://localhost:9000/foo/bar";
    from_url(url).unwrap();
}

#[test]
fn test_parse_duration() {
    assert_eq!(parse_duration("3s").unwrap(), Duration::from_secs(3));
    assert_eq!(parse_duration("123ms").unwrap(), Duration::from_millis(123));

    parse_duration("ms").unwrap_err();
    parse_duration("1ss").unwrap_err();
}

#[test]
fn test_parse_opt_duration() {
    assert_eq!(
        parse_opt_duration("3s").unwrap(),
        Some(Duration::from_secs(3))
    );
    assert_eq!(parse_opt_duration("none").unwrap(), None::<Duration>);
}

#[test]
fn test_parse_compression() {
    assert!(!parse_compression("none").unwrap());
    assert!(parse_compression("lz4").unwrap());
    parse_compression("?").unwrap_err();
}
