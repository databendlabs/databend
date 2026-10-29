// Copyright 2026 Datafuse Labs.
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
fn test_python_udf_network_policy_defaults_to_denied() {
    assert!(!InnerConfig::default().query.common.python_udf_allow_network);
    let parsed: InnerConfig = Args::parse_from(["databend-query"])
        .config
        .try_into()
        .unwrap();
    assert!(!parsed.query.common.python_udf_allow_network);
}

#[test]
fn test_python_udf_network_policy_operator_configuration() {
    let parsed: InnerConfig =
        Args::parse_from(["databend-query", "--python-udf-allow-network", "true"])
            .config
            .try_into()
            .unwrap();
    assert!(parsed.query.common.python_udf_allow_network);
    for enabled in [false, true] {
        let config: Config =
            toml::from_str(&format!("[query]\npython_udf_allow_network = {enabled}\n")).unwrap();
        let config: InnerConfig = config.try_into().unwrap();
        assert_eq!(config.query.common.python_udf_allow_network, enabled);
    }
}
