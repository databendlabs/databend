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

use databend_common_meta_app::principal::check_record_delimiter;

#[test]
fn test_check_record_delimiter() {
    assert!(check_record_delimiter("|").is_ok());
    assert!(check_record_delimiter("\r\n").is_ok());
    assert!(check_record_delimiter("").is_err());
    assert!(check_record_delimiter("foo").is_err());
    assert!(check_record_delimiter("|\r").is_err());
}
