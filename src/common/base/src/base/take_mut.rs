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

use std::panic;

use databend_common_exception::Result;

use crate::runtime::catch_unwind;

/// copy from https://docs.rs/take_mut/0.2.2/take_mut/fn.take.html with some modifications.
/// if a panic occurs, the entire process will be aborted, as there's no valid `T` to put back into the `&mut T`.
pub fn take_mut<T, F>(mut_ref: &mut T, closure: F) -> Result<()>
where F: FnOnce(T) -> Result<T> {
    use std::ptr;

    unsafe {
        let old_t = ptr::read(mut_ref);
        let closure_result = catch_unwind(panic::AssertUnwindSafe(|| closure(old_t)));

        match closure_result {
            Ok(Ok(new_t)) => {
                ptr::write(mut_ref, new_t);
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => ::std::process::abort(),
        }
    }
}
