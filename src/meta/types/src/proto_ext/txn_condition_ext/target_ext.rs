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

use pb::txn_condition::Target;

use crate::protobuf as pb;

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Target::Value(_) => {
                write!(f, "value(...)",)
            }
            Target::Seq(seq) => {
                write!(f, "seq({})", seq)
            }
            Target::KeysWithPrefix(n) => {
                write!(f, "keys_with_prefix({})", n)
            }
        }
    }
}
