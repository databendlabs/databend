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

use common_base::base::get_memory_usage;

// This test is ignored because when running unit tests in parallel, other tests also allocates memory in global allocator
#[test]
#[ignore]
fn test_global_tracker() -> anyhow::Result<()> {
    static KB: usize = 1024;
    static MB: usize = KB * KB;

    let sz = 5 * MB;

    let m0 = get_memory_usage();
    {
        let _mem = Vec::<u8>::with_capacity(sz);
        let m1 = get_memory_usage();

        let diff = (m1 - m0) as usize;

        assert!(diff < sz + 10 * KB);
        assert!(sz - 10 * KB < diff);
    }

    let m2 = get_memory_usage();

    let diff = m2 - m0;

    assert!(diff < 10 * KB as i64);
    assert!((-10 * KB as i64) < diff);

    Ok(())
}
