// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use lazy_static::lazy_static;
use structopt::StructOpt;
use structopt_toml::StructOptToml;

lazy_static! {
    pub static ref FUSE_COMMIT_VERSION: String = {
        let build_semver = option_env!("VERGEN_BUILD_SEMVER");
        let git_sha = option_env!("VERGEN_GIT_SHA_SHORT");
        let rustc_semver = option_env!("VERGEN_RUSTC_SEMVER");
        let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP");

        let ver = match (build_semver, git_sha, rustc_semver, timestamp) {
            #[cfg(not(feature = "simd"))]
            (Some(v1), Some(v2), Some(v3), Some(v4)) => format!("{}-{}({}-{})", v1, v2, v3, v4),
            #[cfg(feature = "simd")]
            (Some(v1), Some(v2), Some(v3), Some(v4)) => {
                format!("{}-{}-simd({}-{})", v1, v2, v3, v4)
            }
            _ => String::new(),
        };
        ver
    };
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
pub struct Config {
    #[structopt(long, env = "FUSE_STORE_LOG_LEVEL", default_value = "INFO")]
    pub log_level: String,

    #[structopt(long, env = "FUSE_STORE_LOG_DIR", default_value = "./_logs")]
    pub log_dir: String,

    #[structopt(
        long,
        env = "FUSE_STORE_METRIC_API_ADDRESS",
        default_value = "127.0.0.1:7171"
    )]
    pub metric_api_address: String,

    #[structopt(long, env = "HTTP_API_ADDRESS", default_value = "127.0.0.1:8181")]
    pub http_api_address: String,

    #[structopt(long, env = "TLS_SERVER_CERT", default_value = "")]
    pub tls_server_cert: String,

    #[structopt(long, env = "TLS_SERVER_KEY", default_value = "")]
    pub tls_server_key: String,

    #[structopt(
        long,
        env = "FUSE_STORE_FLIGHT_API_ADDRESS",
        default_value = "127.0.0.1:9191"
    )]
    pub flight_api_address: String,

    #[structopt(
        long,
        env = "FUSE_STORE_META_API_HOST",
        default_value = "127.0.0.1",
        help = "The listening host for metadata communication"
    )]
    pub meta_api_host: String,

    #[structopt(
        long,
        env = "FUSE_STORE_META_API_PORT",
        default_value = "9291",
        help = "The listening port for metadata communication"
    )]
    pub meta_api_port: u32,

    #[structopt(
        long,
        env = "FUSE_STORE_META_DIR",
        default_value = "./_meta",
        help = "The dir to store persisted meta state, including raft logs, state machine etc."
    )]
    pub meta_dir: String,

    #[structopt(
        long,
        env = "FUSE_STORE_META_NO_SYNC",
        help = concat!("Whether to fsync meta to disk for every meta write(raft log, state machine etc).",
                      " No-sync brings risks of data loss during a crash.",
                      " You should only use this in a testing environment, unless YOU KNOW WHAT YOU ARE DOING."
        ),
    )]
    pub meta_no_sync: bool,

    // raft config
    #[structopt(
        long,
        env = "FUSE_STORE_SNAPSHOT_LOGS_SINCE_LAST",
        default_value = "1024",
        help = "The number of logs since the last snapshot to trigger next snapshot."
    )]
    pub snapshot_logs_since_last: u64,

    #[structopt(
        long,
        env = "FUSE_STORE_HEARTBEAT_INTERVAL",
        default_value = "500",
        help = concat!("The interval in milli seconds at which a leader send heartbeat message to followers.",
                      " Different value of this setting on leader and followers may cause unexpected behavior.")
    )]
    pub heartbeat_interval: u64,

    #[structopt(
        long,
        env = "FUSE_STORE_BOOT",
        help = "Whether to boot up a new cluster. If already booted, it is ignored"
    )]
    pub boot: bool,
}

impl Config {
    /// StructOptToml provides a default Default impl that loads config from cli args,
    /// which conflicts with unit test if case-filter arguments passed, e.g.:
    /// `cargo test my_unit_test_fn`
    ///
    /// Thus we need another method to generate an empty default instance.
    pub fn empty() -> Self {
        <Self as StructOpt>::from_iter(&Vec::<&'static str>::new())
    }

    pub fn meta_api_addr(&self) -> String {
        format!("{}:{}", self.meta_api_host, self.meta_api_port)
    }

    /// Returns true to fsync after a write operation to meta.
    pub fn meta_sync(&self) -> bool {
        !self.meta_no_sync
    }
}
