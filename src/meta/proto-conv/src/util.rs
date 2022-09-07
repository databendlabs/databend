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

use crate::Incompatible;

pub const VER: u64 = 5;
pub const MIN_COMPATIBLE_VER: u64 = 1;

pub fn check_ver(msg_ver: u64, msg_min_compatible: u64) -> Result<(), Incompatible> {
    if VER < msg_min_compatible {
        return Err(Incompatible {
            reason: format!(
                "executable ver={} is smaller than the message min compatible ver: {}",
                VER, msg_min_compatible
            ),
        });
    }
    if msg_ver < MIN_COMPATIBLE_VER {
        return Err(Incompatible {
            reason: format!(
                "message ver={} is smaller than executable min compatible ver: {}",
                msg_ver, MIN_COMPATIBLE_VER
            ),
        });
    }
    Ok(())
}

pub fn missing(reason: impl ToString) -> impl FnOnce() -> Incompatible {
    let s = reason.to_string();
    move || Incompatible { reason: s }
}
