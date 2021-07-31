// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use structopt::StructOpt;

#[derive(Clone, Debug, PartialEq, StructOpt)]
pub struct Config {
    #[structopt(long, env = "NAMESPACE", default_value = "test")]
    pub namespace: String,

    #[structopt(long, env = "DATAFUSE_DIR", default_value = ".datafuse")]
    pub datafuse_dir: String,

    #[structopt(
        long,
        env = "DOWNLOAD_URL",
        default_value = "https://github.com/datafuselabs/datafuse/releases/download"
    )]
    pub download_url: String,

    #[structopt(
        long,
        env = "TAG_URL",
        default_value = "https://api.github.com/repos/datafuselabs/datafuse/tags"
    )]
    pub tag_url: String,
}

impl Config {
    pub fn create() -> Self {
        let home_dir = dirs::home_dir().unwrap();
        let datafuse_dir = home_dir.join(".datafuse");

        let mut conf = Config::from_args();
        conf.datafuse_dir = format!("{}/{}", datafuse_dir.to_str().unwrap(), conf.namespace);
        conf
    }
}
