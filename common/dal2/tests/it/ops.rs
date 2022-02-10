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

use common_dal2::ops::HeaderRange;

#[test]
fn test_header_range() {
    let h = HeaderRange::new(None, Some(1024));
    assert_eq!(h.to_string(), "bytes=0-1023");

    let h = HeaderRange::new(Some(1024), None);
    assert_eq!(h.to_string(), "bytes=1024-");

    let h = HeaderRange::new(Some(1024), Some(1024));
    assert_eq!(h.to_string(), "bytes=1024-2047");
}
