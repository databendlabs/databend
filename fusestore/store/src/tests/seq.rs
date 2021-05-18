// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

static GLOBAL_SEQ: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub struct Seq(usize);

impl Default for Seq {
    fn default() -> Self {
        Seq(GLOBAL_SEQ.fetch_add(1, Ordering::SeqCst))
    }
}

impl Deref for Seq {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
