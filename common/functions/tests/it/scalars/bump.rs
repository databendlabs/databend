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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bumpalo::Bump;
use common_exception::Result;

struct State {
    idx: i32,
    data1: Arc<HashMap<Vec<u8>, String>>,
    data2: HashSet<u8>,
    data3: Option<Vec<u8>>,
}

#[test]
fn test_memory_leak() -> Result<()> {
    {
        let arena = Bump::new();
        let val = arena.alloc(State {
            idx: 0,
            data1: Arc::new(HashMap::new()),
            data2: HashSet::new(),
            data3: None,
        });

        let n = 102400000;
        let mut map = HashMap::new();
        map.insert(vec![0; n], "123".to_string());
        let set = HashSet::from_iter(vec![0u8; n].into_iter());

        val.data1 = Arc::new(map);
        val.data2 = set;
        val.data3 = Some(vec![0; n]);
        // unsafe { std::ptr::drop_in_place(val) };
    }

    println!("{:?}", "EXIT");
    std::thread::sleep(Duration::from_secs(100));
    Ok(())
}
