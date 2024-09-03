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

use databend_common_exception::Result;
use databend_common_expression::hilbert_decompress;
use databend_common_expression::hilbert_decompress_state_list;
use databend_common_expression::hilbert_index;
use databend_common_expression::FixedLengthEncoding;

#[test]
fn test_hilbert() -> Result<()> {
    let width = 8;
    let point = [1i64, -2, 3, 4, 6]
        .iter()
        .map(|v| v.encode().to_vec())
        .collect::<Vec<_>>();
    let vec_of_slices: Vec<&[u8]> = point
        .iter() // Iterate over references to each [u8; 4]
        .map(|array| &array[..]) // Convert &[u8; 4] to &[u8]
        .collect();
    let key = hilbert_index(&vec_of_slices, width);

    let state_list = hilbert_decompress_state_list(point.len());
    let res = hilbert_decompress(&key, width, &state_list);
    assert_eq!(point, res);

    Ok(())
}
