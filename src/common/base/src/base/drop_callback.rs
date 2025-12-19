// Copyright 2021 Datafuse Labs
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

use std::fmt;

/// Call a callback when dropped.
pub struct DropCallback {
    callback: Option<Box<dyn FnOnce() + Send + 'static>>,
}

impl fmt::Debug for DropCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DropCallback")
    }
}

impl DropCallback {
    pub fn new(callback: impl FnOnce() + Send + 'static) -> Self {
        DropCallback {
            callback: Some(Box::new(callback)),
        }
    }
}

impl Drop for DropCallback {
    fn drop(&mut self) {
        if let Some(callback) = self.callback.take() {
            callback();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn test_drop_callback() {
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = called.clone();
        {
            let _drop_callback = DropCallback::new(move || {
                called_clone.store(true, Ordering::SeqCst);
            });
            assert!(!called.load(Ordering::SeqCst));
        }
        assert!(called.load(Ordering::SeqCst));
    }
}
