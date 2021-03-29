// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_rwlock() {
    use std::sync::Arc;
    use std::thread;

    use crate::RwLock;

    let a = 7u8;
    let rwlock = Arc::new(RwLock::new(a));
    let rwlock2 = rwlock.clone();
    let rwlock3 = rwlock.clone();

    let thread1 = thread::spawn(move || {
        let mut b = rwlock2.write();
        *b = 8;
    });
    let thread2 = thread::spawn(move || {
        let mut b = rwlock3.write();
        *b = 9;
    });

    let _ = thread1.join();
    let _ = thread2.join();

    let _read = rwlock.read();
}
