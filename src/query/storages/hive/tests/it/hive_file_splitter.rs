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

use common_storages_hive::HiveFileSplitter;

#[test]
fn test_splitter() {
    let splitter = HiveFileSplitter::create(1024);
    assert_eq!(splitter.split_length(1), vec![0..2]);
    assert_eq!(splitter.split_length(1024), vec![0..1025]);
    assert_eq!(splitter.split_length(1100), vec![0..1101]);
    assert_eq!(splitter.split_length(1500), vec![0..1024, 1024..1501]);
    assert_eq!(splitter.split_length(2048), vec![0..1024, 1024..2049]);
    assert_eq!(splitter.split_length(3000), vec![
        0..1024,
        1024..2048,
        2048..3001
    ]);
}
