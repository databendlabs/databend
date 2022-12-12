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

use common_formats::check_record_delimiter;

/// This test code is written by OpenAI's GPT-3.
#[test]
fn test_check_record_delimiter() {
    let mut option = "".to_string();
    assert!(check_record_delimiter(&mut option).is_ok());
    assert_eq!(option, "\n");

    let mut option = "|".to_string();
    assert!(check_record_delimiter(&mut option).is_ok());
    assert_eq!(option, "|");

    let mut option = "\r\n".to_string();
    assert!(check_record_delimiter(&mut option).is_ok());
    assert_eq!(option, "\r\n");

    let mut option = "foo".to_string();
    assert!(check_record_delimiter(&mut option).is_err());

    let mut option = "|\r".to_string();
    assert!(check_record_delimiter(&mut option).is_err());
}
