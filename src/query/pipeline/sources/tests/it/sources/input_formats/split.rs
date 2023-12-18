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

use databend_common_pipeline_sources::input_formats::split_by_size;

#[test]
fn test_split_by_size() {
    assert_eq!(split_by_size(10, 3), vec![(0, 3), (3, 3), (6, 3), (9, 1)]);
    assert_eq!(split_by_size(9, 3), vec![(0, 3), (3, 3), (6, 3)]);
    assert_eq!(split_by_size(8, 3), vec![(0, 3), (3, 3), (6, 2)]);
}
