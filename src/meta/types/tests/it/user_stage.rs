// Copyright 2021 Datafuse Labs.
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
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;

#[test]
fn test_user_stage() -> Result<()> {
    {
        let internal = UserStageInfo {
            stage_name: "test_stage".to_string(),
            stage_type: StageType::Internal,
            ..Default::default()
        };
        assert_eq!(internal.get_prefix(), "/stage/test_stage/".to_string());
    }
    {
        let external = UserStageInfo {
            stage_name: "test_stage".to_string(),
            stage_type: StageType::External,
            ..Default::default()
        };
        assert_eq!(external.get_prefix(), "/".to_string());
    }

    Ok(())
}
