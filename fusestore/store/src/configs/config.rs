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
            (Some(v1), Some(v2), Some(v3), Some(v4)) => format!("{}-{}({}-{})", v1, v2, v3, v4),
            _ => String::new(),
        };
        ver
    };
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
pub struct Config {
    #[structopt(long, env = "FUSE_STORE_LOG_LEVEL", default_value = "INFO")]
    pub log_level: String,

    #[structopt(
        long,
        env = "FUSE_STORE_METRIC_API_ADDRESS",
        default_value = "127.0.0.1:7171"
    )]
    pub metric_api_address: String,

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
        env = "FUSE_STORE_BOOT",
        help = "Whether to boot up a new cluster. If already booted, it is ignored"
    )]
    pub boot: bool,
}
