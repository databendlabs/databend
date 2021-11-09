// Copyright 2020 Datafuse Labs.
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

use common_clickhouse_srv::types::Block;

#[test]
fn test_complex_iter() {
    let lo = Block::new().column("?", vec![1_u32, 2]);
    let hi = Block::new().column("?", vec![3_u32, 4, 5]);

    let block = Block::concat(&[lo, hi]);

    let columns = block.columns()[0].iter::<u32>().unwrap();
    let actual: Vec<_> = columns.collect();
    assert_eq!(actual, vec![&1_u32, &2, &3, &4, &5]);

    let null_u32 = Block::new().column("?", vec![Some(3_u32), Some(4), None]);
    let columns = null_u32.columns()[0].iter::<Option<u32>>().unwrap();
    let actual: Vec<_> = columns.collect();
    assert_eq!(actual, vec![Some(&3_u32), Some(&4), None]);
}
