// Copyright 2021 Datafuse Labs
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

use databend_common_column::binview::BinaryViewColumnBuilder;
use databend_common_column::binview::BinaryViewColumnGeneric;
use databend_common_column::binview::Utf8ViewColumn;

#[test]
fn extend_from_iter() {
    let mut b = BinaryViewColumnBuilder::<str>::new();
    b.extend_trusted_len_values(vec!["a", "b"].into_iter());

    let a = b.clone();
    b.extend_trusted_len_values(a.iter());

    let b: Utf8ViewColumn = b.into();
    let c: Utf8ViewColumn =
        BinaryViewColumnBuilder::<str>::from_iter(vec!["a", "b", "a", "b"]).into();

    assert_eq!(b, c)
}
