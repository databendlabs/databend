// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub struct ExitGuard<F: Fn()> {
    function: F,
}

impl<F: Fn()> ExitGuard<F> {
    pub fn create(f: F) -> ExitGuard<F> {
        ExitGuard { function: f }
    }
}

impl<F: Fn()> Drop for ExitGuard<F> {
    fn drop(&mut self) {
        (self.function)();
    }
}
