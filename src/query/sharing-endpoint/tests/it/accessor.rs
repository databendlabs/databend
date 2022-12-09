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

use common_base::base::tokio;
use common_exception::Result;
use sharing_endpoint::accessor::truncate_root;

#[tokio::test]
async fn test_truncate_root() -> Result<()> {
    let t = truncate_root("tenant1/".to_string(), "tenant1/db1/tb1/file1".to_string());
    assert_eq!(t, "file1".to_string());
    Ok(())
}
