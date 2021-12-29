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
use common_meta_types::UserPrivilegeSet;
use common_meta_types::UserPrivilegeType;

#[test]
fn test_user_privilege() -> Result<()> {
    let mut privileges = UserPrivilegeSet::empty();
    let r = privileges.has_privilege(UserPrivilegeType::Insert);
    assert!(!r);

    privileges.set_privilege(UserPrivilegeType::Insert);
    let r = privileges.has_privilege(UserPrivilegeType::Insert);
    assert!(r);

    privileges.set_all_privileges();
    let r = privileges.has_privilege(UserPrivilegeType::Create);
    assert!(r);

    Ok(())
}
