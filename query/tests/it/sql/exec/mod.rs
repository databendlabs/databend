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

#[test]
pub fn test_format_field_name() {
    use databend_query::sql::exec::decode_field_name;
    use databend_query::sql::exec::format_field_name;
    let display_name = "column_name123名字".to_string();
    let index = 12321;
    let field_name = format_field_name(display_name.as_str(), index);
    let (decoded_name, decoded_index) = decode_field_name(field_name.as_str()).unwrap();
    assert!(decoded_name == display_name && decoded_index == index);
}
