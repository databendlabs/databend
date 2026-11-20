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
use databend_common_config::InnerConfig;

#[derive(Parser)]
struct Args {
    #[clap(flatten)]
    config: Config,
}

#[test]
fn python_external_access_requires_operator_opt_in() {
    assert!(
        !InnerConfig::default()
            .query
            .common
            .enable_python_udf_external_access
    );
    for (arguments, enabled) in [
        (vec!["databend-query"], false),
        (
            vec!["databend-query", "--enable-python-udf-external-access"],
            true,
        ),
        (vec!["databend-query", "--enable-udf-server"], false),
    ] {
        let config: InnerConfig = Args::try_parse_from(arguments)
            .unwrap()
            .config
            .try_into()
            .unwrap();
        assert_eq!(
            config.query.common.enable_python_udf_external_access,
            enabled
        );
    }
    let config: Config =
        toml::from_str("[query]\nenable_python_udf_external_access = true\n").unwrap();
    let config: InnerConfig = config.try_into().unwrap();
    assert!(config.query.common.enable_python_udf_external_access);
}
