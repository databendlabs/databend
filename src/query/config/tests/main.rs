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

use clap::Parser;
use common_config::Config;
use common_config::OuterConfig;
use pretty_assertions::assert_eq;

/// It's required to make sure config's default value is the same with clap.
#[test]
fn test_config_default() {
    let type_default = Config::default();
    let args_default: Config = OuterConfig::parse_from(Vec::<OsString>::new())
        .try_into()
        .expect("parse from args must succeed");

    assert_eq!(
        type_default, args_default,
        "inner config's default value is different from args, please check again"
    )
}
