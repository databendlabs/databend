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

use common_base::base::*;
use common_exception::Result;

#[test]
fn test_progress() -> Result<()> {
    let original_key = "databend/test_user123!!";
    let new_key = escape_for_key(original_key);
    assert_eq!(Ok("databend%2ftest_user123%21%21".to_string()), new_key);
    assert_eq!(
        Ok(original_key.to_string()),
        unescape_for_key(new_key.unwrap().as_str())
    );
    Ok(())
}

#[test]
fn mask_string_test() {
    assert_eq!(mask_string("", 10), "".to_string());
    assert_eq!(mask_string("string", 0), "******".to_string());
    assert_eq!(mask_string("string", 1), "******g".to_string());
    assert_eq!(mask_string("string", 2), "******ng".to_string());
    assert_eq!(mask_string("string", 3), "******ing".to_string());
    assert_eq!(mask_string("string", 20), "string".to_string());
}
