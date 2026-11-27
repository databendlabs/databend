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

use clap::Parser;
use databend_common_config::Config;
use databend_common_config::MetaServiceCache;

#[derive(Parser)]
struct Args {
    #[clap(flatten)]
    config: Config,
}

/// The flag takes the snake_case names of `MetaServiceCache`, separated by commas, and rejects
/// any other name.
#[test]
fn test_meta_service_caches_from_flag() {
    let args = [
        "databend-query",
        "--cache-meta-service-caches",
        "user,cluster_node",
    ];
    let cache = Args::try_parse_from(args).unwrap().config.cache;
    assert_eq!(cache.meta_service_caches, vec![
        MetaServiceCache::User,
        MetaServiceCache::ClusterNode
    ]);
    assert!(cache.is_meta_service_cache_enabled(MetaServiceCache::ClusterNode));
    assert!(!cache.is_meta_service_cache_enabled(MetaServiceCache::Setting));

    let unknown = ["databend-query", "--cache-meta-service-caches", "users"];
    assert!(Args::try_parse_from(unknown).is_err());
}

#[test]
fn test_meta_service_cache_defaults() {
    let expected = [MetaServiceCache::ClusterNode, MetaServiceCache::Setting];
    let cli = Args::try_parse_from(["databend-query"]).unwrap().config;
    let file: Config = toml::from_str("[cache]").unwrap();
    for config in [Config::default(), cli, file] {
        assert_eq!(config.cache.meta_service_caches, expected);
        assert!(
            !config
                .cache
                .is_meta_service_cache_enabled(MetaServiceCache::Ownership)
        );
    }
}

/// A config file lists the names in a TOML array, and fails to load with any other name.
#[test]
fn test_meta_service_caches_from_file() {
    let config: Config = toml::from_str("[cache]\nmeta_service_caches = [\"setting\"]\n").unwrap();
    assert_eq!(config.cache.meta_service_caches, vec![
        MetaServiceCache::Setting
    ]);

    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), "[cache]\nmeta_service_caches = []\n").unwrap();
    let config = Args::try_parse_from(["databend-query"]).unwrap().config;
    let config = config.merge(file.path().to_str().unwrap()).unwrap();
    assert!(config.cache.meta_service_caches.is_empty());

    let unknown = toml::from_str::<Config>("[cache]\nmeta_service_caches = [\"users\"]\n");
    let err = unknown.unwrap_err().to_string();
    assert!(err.contains("unknown variant `users`"), "{err}");
}

/// The environment variable lists the names separated by commas.
#[test]
fn test_meta_service_caches_from_env() {
    let config = temp_env::with_var("CACHE_META_SERVICE_CACHES", Some("user, setting"), || {
        Config::load_with_config_file("")
    })
    .unwrap();
    assert_eq!(config.cache.meta_service_caches, vec![
        MetaServiceCache::User,
        MetaServiceCache::Setting
    ]);
}

/// `ownership` in the list enables the ownership cache, and so does the deprecated
/// `meta_service_ownership_cache = true` alone.
#[test]
fn test_meta_service_caches_ownership() {
    let listed: Config =
        toml::from_str("[cache]\nmeta_service_caches = [\"ownership\"]\n").unwrap();
    let cache = listed.cache;
    assert!(cache.is_meta_service_cache_enabled(MetaServiceCache::Ownership));

    let deprecated: Config =
        toml::from_str("[cache]\nmeta_service_ownership_cache = true\n").unwrap();
    let cache = deprecated.cache;
    assert_eq!(
        cache.meta_service_caches,
        Config::default().cache.meta_service_caches
    );
    assert!(cache.is_meta_service_cache_enabled(MetaServiceCache::Ownership));

    let cache = Args::try_parse_from(["databend-query"])
        .unwrap()
        .config
        .cache;
    assert!(!cache.is_meta_service_cache_enabled(MetaServiceCache::Ownership));
}

/// Only the deprecated `meta_service_ownership_cache = true` gets a startup warning: `ownership`
/// in the list enables the same cache without one.
#[test]
fn test_deprecated_ownership_cache_warning() {
    let deprecated: Config =
        toml::from_str("[cache]\nmeta_service_ownership_cache = true\n").unwrap();
    assert!(
        deprecated
            .cache
            .deprecated_ownership_cache_warning()
            .is_some()
    );

    let listed: Config =
        toml::from_str("[cache]\nmeta_service_caches = [\"ownership\"]\n").unwrap();
    assert_eq!(listed.cache.deprecated_ownership_cache_warning(), None);

    let cache = Args::try_parse_from(["databend-query"])
        .unwrap()
        .config
        .cache;
    assert_eq!(cache.deprecated_ownership_cache_warning(), None);
}
