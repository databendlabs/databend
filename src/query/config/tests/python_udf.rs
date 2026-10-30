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

use clap::CommandFactory;
use clap::Parser;
use databend_common_config::Config;

#[derive(Parser)]
struct Args {
    #[clap(flatten)]
    config: Config,
}

#[test]
fn test_python_udf_has_no_unrestricted_network_configuration() {
    Args::try_parse_from(["databend-query"]).expect("default configuration is valid");
    assert!(
        Args::command()
            .get_arguments()
            .all(|arg| arg.get_long() != Some("python-udf-allow-network"))
    );
    for value in ["false", "true"] {
        let error = Args::try_parse_from(["databend-query", "--python-udf-allow-network", value])
            .err()
            .expect("unrestricted network CLI option must not exist");
        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
    }
}
