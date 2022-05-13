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

#[test]
fn test_mutex() {
    use std::sync::Arc;
    use std::thread;

    use common_base::infallible::Mutex;
    let a = 7u8;
    let mutex = Arc::new(Mutex::new(a));
    let mutex2 = mutex.clone();
    let mutex3 = mutex.clone();

    let thread1 = thread::spawn(move || {
        let mut b = mutex2.lock();
        *b = 8;
    });
    let thread2 = thread::spawn(move || {
        let mut b = mutex3.lock();
        *b = 9;
    });

    let _ = thread1.join();
    let _ = thread2.join();

    let _locked = mutex.lock();
}
