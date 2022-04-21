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

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use common_exception::exception::Result;
use common_managementv1_proto::CreateDatabaseV1;

#[test]
fn test_database_create_version_compatible() -> Result<()> {
    let runner = |_old_version_value: String| -> String {
        let created_on = "2022-04-22T02:17:58.360932910Z";
        let created_utc: DateTime<Utc> = created_on.parse().unwrap();

        let latest_version = CreateDatabaseV1 {
            tenant: "v1".to_string(),
            database_name: "v1_db".to_string(),
            engine_name: "v1_engine".to_string(),
            engine_options: Default::default(),
            comment: "v1_comment".to_string(),
            options: Default::default(),
            created_on: created_utc,
        };

        // Serialize the latest version struct to string.
        let vec = Vec::try_from(latest_version).unwrap();
        let latest_version_value = String::from_utf8_lossy(&vec).to_string();
        latest_version_value
    };

    let path = "tests/it/testdata/database_create";
    crate::helper::version_check(path, runner)
}
