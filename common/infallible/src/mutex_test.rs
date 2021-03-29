// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_mutex() {
    use std::sync::Arc;
    use std::thread;

    use crate::Mutex;
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
