// Copyright 2022 Datafuse Labs.
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

use common_exception::exception::Result;
use common_managementv1_proto::example::ExampleV2;

#[test]
fn test_example_version_compatible() -> Result<()> {
    let runner = |old_version_value: String| -> String {
        // 1. Deserialize old_version_value to the latest version struct.
        let latest_version = ExampleV2::try_from(old_version_value.into_bytes()).unwrap();

        // 2. Serialize the latest version struct to string.
        let vec = Vec::try_from(latest_version).unwrap();
        let latest_version_value = String::from_utf8_lossy(&vec).to_string();
        latest_version_value
    };

    let path = "tests/it/testdata/example";
    crate::helper::version_check(path, runner)
}
