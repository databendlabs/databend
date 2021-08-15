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

use structopt::StructOpt;

#[derive(Clone, Debug, PartialEq, StructOpt, Default)]
pub struct Config {
    #[structopt(long, env = "GROUP", default_value = "test")]
    pub group: String,

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
        let conf = Config::from_args();
        Self::build(conf)
    }

    pub fn default() -> Self {
        let conf: Config = Default::default();
        Self::build(conf)
    }

    fn build(mut conf: Config) -> Self {
        let home_dir = dirs::home_dir().unwrap();
        let datafuse_dir = home_dir.join(".datafuse");
        conf.datafuse_dir = format!("{}/{}", datafuse_dir.to_str().unwrap(), conf.group);
        conf
    }
}
