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

use common_exception::ErrorCode;
use common_exception::Result;

pub fn catch_unwind<F: FnOnce() -> R, R>(f: F) -> Result<R> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(res) => Ok(res),
        Err(cause) => match cause.downcast_ref::<&'static str>() {
            None => match cause.downcast_ref::<String>() {
                None => Err(ErrorCode::PanicError("Sorry, unknown panic message")),
                Some(message) => Err(ErrorCode::PanicError(message.to_string())),
            },
            Some(message) => Err(ErrorCode::PanicError(message.to_string())),
        },
    }
}
