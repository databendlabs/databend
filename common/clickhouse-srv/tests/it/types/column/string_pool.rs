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

use std::io::Write;

use common_clickhouse_srv::types::StringPool;

#[test]
fn test_allocate() {
    let mut pool = StringPool::with_capacity(10);
    for i in 1..1000 {
        let buffer = pool.allocate(i);
        assert_eq!(buffer.len(), i);
        assert_eq!(buffer[0], 0);
        buffer[0] = 1
    }
}

#[test]
fn test_get() {
    let mut pool = StringPool::with_capacity(10);

    for i in 0..1000 {
        let s = format!("text-{}", i);
        let mut buffer = pool.allocate(s.len());

        assert_eq!(buffer.len(), s.len());
        buffer.write_all(s.as_bytes()).unwrap();
    }

    for i in 0..1000 {
        let s = String::from_utf8(Vec::from(pool.get(i))).unwrap();
        assert_eq!(s, format!("text-{}", i));
    }
}
